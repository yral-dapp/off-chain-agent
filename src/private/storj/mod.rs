mod admin;
mod nsfw;
mod posts;

use std::{
    env,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use admin::AdminCanisters;
use anyhow::Context;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use ic_agent::Agent;
use nsfw::IsNsfw;
use redis::{aio::MultiplexedConnection, AsyncCommands, JsonAsyncCommands};
use serde_json::json;

const WORK_QUEUE: &str = "work_queue";
const MAYBE_QUEUE: &str = "maybe_nsfw_queue";

async fn get_item_count_in_staging(con: &mut MultiplexedConnection) -> Result<usize> {
    let c = con.llen(WORK_QUEUE).await?;

    Ok(c)
}

async fn get_redis_client() -> Result<redis::Client> {
    let url = env::var("STORJ_REDIS_URL").context("Couldn't load storj redis url")?;
    let client = redis::Client::open(url).context("Couldn't open redis client")?;
    Ok(client)
}

pub async fn fetch(agent: Agent) -> Result<serde_json::Value> {
    let admin = AdminCanisters::new(agent);
    let redis_client = get_redis_client().await?;
    let mut redis_connection = redis_client
        .get_multiplexed_async_connection()
        .await
        .context("Couldn't open redis connection")?;
    let count = get_item_count_in_staging(&mut redis_connection).await?;

    log::info!("Starting out with {count} items in the d1 staging db");

    let low_pass = "2025-03-26T12:25:05Z".parse::<DateTime<Utc>>();
    let low_pass = match low_pass {
        Ok(l) => l,
        Err(err) => {
            anyhow::bail!("Couldn't parse the low pass timestamp: {err}");
        }
    };

    let item_stream = posts::load_items(Arc::new(admin), low_pass, redis_connection.clone())
        .await
        .context("failed to start item stream");

    let item_stream = match item_stream {
        Ok(i) => i,
        Err(err) => {
            anyhow::bail!("Couldn't start streaming {err}");
        }
    };

    let added = AtomicU64::new(0);
    let skipped = AtomicU64::new(0);
    let maybe_nsfw = AtomicU64::new(0);

    // the operation is io bound, so this number can be optimized to saturate
    // the network of the machine running the worker
    const CONCURRENCY_FACTOR: usize = 500;
    let res = item_stream
        .try_for_each_concurrent(CONCURRENCY_FACTOR, |item| {
            let added = &added;
            let skipped = &skipped;
            let maybe_nsfw = &maybe_nsfw;
            let mut redis_connection = redis_connection.clone();
            async move {
                let vid = item.video_id.as_str();
                let has_key: bool = redis_connection.exists(&vid).await?;
                let queue = if item.is_nsfw == IsNsfw::Maybe {
                    MAYBE_QUEUE
                } else {
                    WORK_QUEUE
                };
                if !has_key {
                    let _: () = redis_connection
                        .json_set(vid, "$", &item)
                        .await
                        .context("Couldn't record video related details")?;

                    let _: () = redis_connection
                        .rpush(queue, &vid)
                        .await
                        .context("Couldn't push item to work_queue")?;

                    if item.is_nsfw == IsNsfw::Maybe {
                        maybe_nsfw.fetch_add(1, Ordering::Relaxed);
                    } else {
                        added.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    skipped.fetch_add(1, Ordering::Relaxed);
                }
                anyhow::Ok(())
            }
        })
        .await
        .context("One of the task returned error");

    if let Err(err) = res {
        anyhow::bail!("failed to load items: {err:?}");
    }

    Ok(json!({
        "added": added.load(Ordering::SeqCst),
        "skipped": skipped.load(Ordering::SeqCst),
        "maybe_nsfw": maybe_nsfw.load(Ordering::SeqCst),
        "total": {
            "before": count,
            "after": get_item_count_in_staging(&mut redis_connection).await?
        }
    }))
}
