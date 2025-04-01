use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use candid::Principal;
use chrono::{DateTime, Utc};
use futures::{future, stream, StreamExt, TryStreamExt};
use redis::{aio::MultiplexedConnection, JsonAsyncCommands};
use serde::{Deserialize, Serialize};
use yral_canisters_client::individual_user_template::{
    GetPostsOfUserProfileError, IndividualUserTemplate, PostDetailsForFrontend,
};

use super::{
    admin::AdminCanisters,
    nsfw::{IsNsfw, NsfwResolver},
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Item {
    pub(crate) video_id: String,
    pub(crate) publisher_user_id: String,
    pub(crate) post_id: u64,
    pub(crate) canister_id: Principal,
    pub(crate) timestamp: String,
    pub(crate) is_nsfw: IsNsfw, // TODO: extra metadata
}

#[derive(Debug, Serialize, Deserialize)]
struct PostDetails {
    post_id: u64,
    video_id: String,
    timestamp: String,
    publisher_user_id: String,
}

impl std::convert::From<PostDetailsForFrontend> for PostDetails {
    fn from(value: PostDetailsForFrontend) -> Self {
        Self {
            post_id: value.id,
            video_id: value.video_uid,
            timestamp: nanos_to_rfc3339(
                value.created_at.secs_since_epoch as i64,
                value.created_at.nanos_since_epoch,
            ),
            publisher_user_id: value.created_by_user_principal_id.to_text(),
        }
    }
}

/// loads all posts for the given user and buffers into a vec before returning
async fn load_all_posts(
    user: &IndividualUserTemplate<'_>,
    low_pass: DateTime<Utc>,
    mut con: MultiplexedConnection,
) -> anyhow::Result<Vec<PostDetails>> {
    let maybe_res: Option<String> = con
        .json_get(user.0.to_text(), "$")
        .await
        .expect("at least redis to work");
    if let Some(res) = maybe_res {
        log::info!("cache hit: {res}");
        return Ok(serde_json::from_str(&res)
            .expect("json to be valid because we are the one who set it in the first place"));
    }
    let res = load_all_posts_inner(user, low_pass).await;
    if let Ok(res) = &res {
        let _: () = con
            .json_set(user.0.to_text(), "$", res)
            .await
            .inspect_err(|err| log::error!("redis failed when caching: {err:?}"))
            .expect("at redis to work");
    }

    res
}

async fn load_all_posts_inner(
    user: &IndividualUserTemplate<'_>,
    low_pass: DateTime<Utc>,
) -> anyhow::Result<Vec<PostDetails>> {
    const LIMIT: usize = 100;
    let mut posts = Vec::new();

    for page in (0..).step_by(LIMIT) {
        let post_res = user
            .get_posts_of_this_user_profile_with_pagination_cursor(page, LIMIT as u64)
            .await
            .context("Couldn't get post")?;

        use yral_canisters_client::individual_user_template::Result13;
        let post = match post_res {
            Result13::Ok(posts) => posts,
            Result13::Err(GetPostsOfUserProfileError::ReachedEndOfItemsList) => break,
            Result13::Err(err) => anyhow::bail!("{err:?}"),
        };

        posts.extend(post.into_iter())
    }

    posts.retain(|post| {
        let created_at = DateTime::from_timestamp_nanos(post.created_at.nanos_since_epoch as i64);
        log::info!("{}", created_at.to_rfc3339());

        // MUST BE NON-INCLUSIVE
        created_at < low_pass
    });

    Ok(posts.into_iter().map(|post| post.into()).collect())
}

fn nanos_to_rfc3339(secs: i64, subsec_nanos: u32) -> String {
    let time = DateTime::from_timestamp(secs, subsec_nanos).unwrap();

    time.to_rfc3339()
}

pub(crate) async fn load_items<'a>(
    admin: Arc<AdminCanisters>,
    low_pass: DateTime<Utc>,
    redis_connection: MultiplexedConnection,
) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<Item>>> {
    let subs = admin
        .platform_orchestrator()
        .await
        .get_all_subnet_orchestrators()
        .await
        .context("Couldn't fetch the subnet orchestrator")?;

    let admin_for_index = admin.clone();
    let admin_for_individual_user = admin.clone();
    let con_for_ind_user = redis_connection.clone();
    let items = stream::iter(subs)
        .then(move |sub| {
            let admin = admin_for_index.clone();
            async move {
                admin
                    .user_index_with(sub)
                    .await
                    .get_user_canister_list()
                    .await
                    .inspect(|users| log::info!("found {} users", users.len()))
                    .inspect_err(|err| log::info!("{err:?}"))
            }
        })
        .and_then(|list| future::ok(stream::iter(list).map(anyhow::Ok)))
        .try_flatten_unordered(None)
        .and_then(move |user_canister| {
            let admin = admin_for_individual_user.clone();
            let redis_con = con_for_ind_user.clone();
            async move {
                let index = admin.individual_user_for(user_canister).await;
                load_all_posts(&index, low_pass, redis_con)
                    .await
                    .inspect(|posts| {
                        log::info!("found {} posts for {}", posts.len(), user_canister)
                    })
                    .inspect_err(|err| log::error!("load_all_posts({user_canister}): {err:?}"))
                    .map(|item| (user_canister, item))
            }
        })
        .and_then(|(canister, list)| async move {
            let ids: Vec<_> = list.iter().map(|post| post.video_id.clone()).collect();

            let is_nsfw_search: BTreeMap<String, IsNsfw> =
                NsfwResolver::is_nsfw(&ids).await?.into_iter().collect();

            let res: Vec<_> = list
                .into_iter()
                .map(|post| {
                    (
                        canister,
                        *is_nsfw_search
                            .get(&post.video_id)
                            .expect("NsfwResolver to always return nsfw status"),
                        post,
                    )
                })
                .collect();

            Ok(res)
        })
        .and_then(|list| future::ok(stream::iter(list).map(anyhow::Ok)))
        .try_flatten_unordered(None)
        .map(|post| {
            post.map(|(canister, is_nsfw, post)| Item {
                timestamp: post.timestamp,
                video_id: post.video_id,
                publisher_user_id: post.publisher_user_id,
                post_id: post.post_id,
                canister_id: canister,
                is_nsfw,
            })
        });

    Ok(items)
}
