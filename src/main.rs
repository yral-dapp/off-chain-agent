use axum::http::StatusCode;
use axum::{response::Html, routing::get, Router};
use axum_auth::AuthBearer;
use candid::encode_args;
use futures::prelude::*;
use ic_agent::{export::Principal, Agent};
use indicatif::{ProgressBar, ProgressStyle};
use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::env;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    // build our application with a route
    let app = Router::new()
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler));

    // run it
    // let addr: SocketAddrV6 = "[::]:8080".parse().unwrap();
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 8080));

    println!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn hello_work_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
    if token
        != env::var("CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY")
            .expect("$CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY is not set")
    {
        return Html("Unauthorized");
    }

    Html("Hello, World!")
}

async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}

async fn backup_job_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
    if token
        != env::var("CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY")
            .expect("$CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY is not set")
    {
        return Html("Unauthorized");
    }

    tokio::spawn(async {
        let pk = env::var("RECLAIM_CANISTER_PEM").expect("$RECLAIM_CANISTER_PEM is not set");

        let identity = match ic_agent::identity::Secp256k1Identity::from_pem(
            stringreader::StringReader::new(pk.as_str()),
        ) {
            Ok(identity) => identity,
            Err(err) => {
                println!("Unable to create identity, error: {:?}", err);
                return Html("Unable to create identity");
            }
        };

        let agent = match Agent::builder()
            .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app") // https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/
            .with_identity(identity)
            .build()
        {
            Ok(agent) => agent,
            Err(err) => {
                println!("Unable to create agent, error: {:?}", err);
                return Html("Unable to create agent");
            }
        };
        // ‼️‼️comment below line in mainnet‼️‼️
        // agent.fetch_root_key().await.unwrap();

        // Platform Orchestrator canister id
        let pf_o10r_canister_id = Principal::from_text("74zq4-iqaaa-aaaam-ab53a-cai").unwrap();

        // Get the list of subnet orchestrator canister ids

        let response = match agent
            .query(&pf_o10r_canister_id, "get_all_subnet_orchestrators")
            .with_arg(encode_args(()).unwrap())
            .call()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                println!("Unable to call the method, error: {:?}", err);
                return Html("Unable to call the method");
            }
        };

        let subnet_o10r_ids = match candid::decode_one(&response) {
            Ok(result) => {
                let result: Vec<Principal> = result;
                result
            }
            Err(err) => {
                println!("Unable to decode the response, error: {:?}", err);
                return Html("Unable to decode the response");
            }
        };
        println!(
            "subnet_o10r_ids {:?}",
            subnet_o10r_ids
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
        );

        let mut canister_ids_list = vec![];

        // Iterate over the subnet orchestrator canister ids
        for subnet_o10r_canister_id in subnet_o10r_ids {
            // Get individual canister list

            let response = match agent
                .query(&subnet_o10r_canister_id, "get_user_canister_list")
                .with_arg(encode_args(()).unwrap())
                .call()
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    println!("Unable to call the method, error: {:?}", err);
                    return Html("Unable to call the method");
                }
            };

            let canister_ids = match candid::decode_one(&response) {
                Ok(result) => {
                    let result: Vec<Principal> = result;
                    result
                }
                Err(err) => {
                    println!("Unable to decode the response, error: {:?}", err);
                    return Html("Unable to decode the response");
                }
            };
            println!(
                "subnet_o10r_canister_id {} canister_ids_len {:?}",
                subnet_o10r_canister_id,
                canister_ids.len()
            );

            println!("canister_ids_len {:?}", canister_ids.len());
            // Iterate over the canister ids and download the snapshot
            for canister_id in canister_ids {
                canister_ids_list.push(canister_id);

                if canister_ids_list.len() == 1500 {
                    break;
                }
            }

            if canister_ids_list.len() == 1500 {
                break;
            }
        }

        // Debug point
        // let raw_list = vec![
        //     "hf6cx-xyaaa-aaaao-abgha-cai",
        //     "gp2ny-lqaaa-aaaao-axkeq-cai",
        //     "uojvx-dyaaa-aaaao-azeca-cai",
        //     "dbihv-jiaaa-aaaao-aj26a-cai",
        //     "d2end-ryaaa-aaaao-asura-cai",
        // ];
        // canister_ids_list = raw_list
        //     .iter()
        //     .map(|x| Principal::from_text(x).unwrap())
        //     .collect();

        const PARALLEL_REQUESTS: usize = 100;

        let pb = ProgressBar::new(canister_ids_list.len() as u64);
        pb.set_style(ProgressStyle::with_template("({pos}/{len}, ETA {eta})").unwrap());

        let futures = canister_ids_list.iter().map(|canister_id| async {
            let agent_c = agent.clone();
            let canister_id_c = *canister_id;
            download_snapshot(&agent_c, &canister_id_c).await
        });

        let stream = futures::stream::iter(futures)
            .boxed()
            .buffer_unordered(PARALLEL_REQUESTS);

        let pb_stream = pb.wrap_stream(stream);

        let results = pb_stream.collect::<Vec<Option<String>>>().await;

        // find the failed canister ids
        let failed_canister_ids = results
            .iter()
            .filter(|x| x.is_some())
            .map(|x| x.as_ref().unwrap().to_string())
            .collect::<Vec<String>>();

        pb.finish();

        println!(
            "final success {:?}/{:?} in {:?}",
            canister_ids_list.len() - failed_canister_ids.len(),
            canister_ids_list.len(),
            pb.elapsed()
        );

        println!("failed_canister_ids -  {:?}", failed_canister_ids);

        Html("Ok")
    });
    Html("Ok")
}

async fn download_snapshot(agent: &Agent, canister_id: &Principal) -> Option<String> {
    // Save snapshot

    let response = match agent
        .update(canister_id, "save_snapshot_json")
        .with_arg(encode_args(()).unwrap())
        .call_and_wait()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            println!(
                "Unable to call the method save_snapshot_json, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return Some(canister_id.to_string());
        }
    };

    let snapshot_size = match candid::decode_one(&response) {
        Ok(result) => {
            let result: u32 = result;
            // println!("len {:?}", result);
            result
        }
        Err(err) => {
            println!(
                "Unable to decode the response save_snapshot_json, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return Some(canister_id.to_string());
        }
    };

    // Download snapshot

    let mut snapshot_bytes = vec![];
    let chunk_size = 1000 * 1000;
    let num_iters = (snapshot_size as f32 / chunk_size as f32).ceil() as u32;

    for i in 0..num_iters {
        let start = i * chunk_size;
        let mut end = (i + 1) * chunk_size;
        if end > snapshot_size {
            end = snapshot_size;
        }

        let response = match agent
            .query(canister_id, "download_snapshot")
            .with_arg(encode_args((start as u64, (end - start) as u64)).unwrap())
            .call()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                println!(
                    "Unable to call the method download_snapshot, error: {:?}, canister_id {:?}",
                    err,
                    canister_id.to_string()
                );
                return Some(canister_id.to_string());
            }
        };

        let snapshot_chunk = match candid::decode_one(&response) {
            Ok(result) => {
                let result: Vec<u8> = result;
                // println!("{:?}", result.len());
                result
            }
            Err(err) => {
                println!(
                    "Unable to decode the response download_snapshot, error: {:?}, canister_id {:?}",
                    err,
                    canister_id.to_string()
                );
                return Some(canister_id.to_string());
            }
        };

        snapshot_bytes.extend(snapshot_chunk);
    }

    // let json_str = String::from_utf8_lossy(&snapshot_bytes).to_string();
    // println!("{:?}", json_str);

    // Delete the local snapshot

    let response = match agent
        .update(canister_id, "clear_snapshot")
        .with_arg(encode_args(()).unwrap())
        .call_and_wait()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            println!(
                "Unable to call the method clear_snapshot, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return Some(canister_id.to_string());
        }
    };

    match candid::decode_one(&response) {
        Ok(result) => result,
        Err(err) => {
            println!(
                "Unable to decode the response clear_snapshot, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return Some(canister_id.to_string());
        }
    };

    let keys = Credentials::new(
        Some(
            env::var("CF_R2_ACCESS_KEY_TEMP")
                .expect("CF_R2_ACCESS_KEY_TEMP is not set")
                .as_str(),
        ),
        Some(
            env::var("CF_R2_SECRET_ACCESS_KEY_TEMP")
                .expect("CF_R2_SECRET_ACCESS_KEY_TEMP is not set")
                .as_str(),
        ),
        None,
        None,
        None,
    )
    .expect("Unable to create credentials");

    let bucket = match Bucket::new(
        "canister-entire-contents-json",
        Region::R2 {
            account_id: env::var("HOTORNOT_CF_ACCOUNT_ID")
                .expect("HOTORNOT_CF_ACCOUNT_ID is not set"),
        },
        keys,
    ) {
        Ok(bucket) => bucket.with_path_style(),
        Err(err) => {
            println!(
                "Unable to create bucket, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return Some(canister_id.to_string());
        }
    };

    let key = canister_id.to_string();

    let response_data = match bucket
        .put_object(key.clone(), snapshot_bytes.as_slice())
        .await
    {
        Ok(response_data) => response_data,
        Err(err) => {
            println!(
                "Unable to put object, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return Some(canister_id.to_string());
        }
    };
    if response_data.status_code() != 200 {
        println!(
            "Unable to put object, error: {:?}, canister_id {:?}",
            response_data,
            canister_id.to_string()
        );
        Some(canister_id.to_string())
    } else {
        None
    }

    // println!("🟢 success for canister {}", canister_id.to_string());

    // Retreive snapshot

    // let response_data = match bucket.get_object(key).await {
    //     Ok(response_data) => response_data,
    //     Err(err) => {
    //         println!("Unable to get object, error: {:?}", err);
    //         return;
    //     }
    // };

    // let body = response_data.bytes();
    // let body = String::from_utf8_lossy(&body).to_string();
    // println!("{:?}", body);

    // print response data headers
    // println!("headers {:?}", response_data.headers());
}
