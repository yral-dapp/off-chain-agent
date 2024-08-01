use std::sync::Arc;

use warehouse_events::warehouse_events_server::WarehouseEvents;

use crate::events::warehouse_events::{Empty, WarehouseEvent};
use crate::AppState;

pub mod warehouse_events {
    tonic::include_proto!("warehouse_events");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("warehouse_events_descriptor");
}

pub mod event;

pub struct WarehouseEventsService {
    pub shared_state: Arc<AppState>,
}

#[tonic::async_trait]
impl WarehouseEvents for WarehouseEventsService {
    async fn send_event(
        &self,
        request: tonic::Request<WarehouseEvent>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let shared_state = self.shared_state.clone();

        let request = request.into_inner();
        let event = event::Event::new(request);

        event.stream_to_bigquery();

        event.upload_to_gcs();

        event.update_watch_history(&shared_state.clone());

        event.update_success_history(&shared_state.clone());

        Ok(tonic::Response::new(Empty {}))
    }
}

// #[derive(Debug, Serialize, Deserialize)]
// struct CFStreamResult {
//     result: Vec<CFStream>,
// }

// #[derive(Debug, Serialize, Deserialize, Clone)]
// struct CFStream {
//     uid: String,
//     created: String,
// }

// pub async fn test_cloudflare(
//     Query(params): Query<HashMap<String, String>>,
// ) -> Result<(), AppError> {
//     // Get Request to https://api.cloudflare.com/client/v4/accounts/{account_id}/stream
//     // Query param start 2021-05-03T00:00:00Z
//     let startdate = params.get("startdate").unwrap().clone();
//     let thresh = params.get("thresh").unwrap().parse::<usize>().unwrap();

//     let url = format!(
//         "https://api.cloudflare.com/client/v4/accounts/{}/stream",
//         CLOUDFLARE_ACCOUNT_ID
//     );
//     let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

//     let client = reqwest::Client::new();
//     let mut num_vids = 0;
//     let mut start_time = startdate;
//     let mut cnt = 0;
//     let mut hashset: HashSet<String> = HashSet::new();

//     loop {
//         let response = client
//             .get(&url)
//             .bearer_auth(&bearer_token)
//             .query(&[("asc", "true"), ("start", &start_time)])
//             .send()
//             .await?;
//         // log::info!("Response: {:?}", response);
//         if response.status() != 200 {
//             log::error!(
//                 "Failed to get response from Cloudflare: {:?}",
//                 response.text().await?
//             );
//             return Err(anyhow::anyhow!("Failed to get response from Cloudflare").into());
//         }

//         let body = response.text().await?;
//         let result: CFStreamResult = serde_json::from_str(&body)?;
//         let mut result_vec = result.result.clone();

//         // add uids to hashset
//         for r in &result_vec {
//             hashset.insert(r.uid.clone());

//             if hashset.len() >= thresh {
//                 break;
//             }
//         }

//         if cnt > 0 {
//             result_vec.remove(0);
//         }

//         num_vids += result_vec.len();
//         if result_vec.len() == 0 {
//             break;
//         }
//         let last = &result.result[result.result.len() - 1];
//         start_time = last.created.clone();
//         cnt += 1;

//         if cnt > 10000 {
//             log::info!("Breaking after 10000 iterations");
//             break;
//         }

//         if hashset.len() >= thresh {
//             // hashset retain only 100 elements
//             log::error!("Last: {:?}", last);
//             break;
//         }
//     }

//     log::info!("Total number of videos: {}", num_vids);
//     log::info!("Total number of videos in hashset: {}", hashset.len());
//     // log::info!("Hashset: {:?}", hashset);

//     // call upload_gcs
//     tokio::spawn(async move {
//         const PARALLEL_REQUESTS: usize = 50;
//         let futures = hashset
//             .iter()
//             .map(|uid| upload_gcs(&uid))
//             .collect::<Vec<_>>();

//         let stream = futures::stream::iter(futures)
//             .boxed()
//             .buffer_unordered(PARALLEL_REQUESTS);
//         let results = stream.collect::<Vec<Result<(), anyhow::Error>>>().await;

//         for r in results {
//             match r {
//                 Ok(_) => continue,
//                 Err(e) => log::error!("Failed to upload to GCS: {:?}", e),
//             }
//         }
//     });

//     Ok(())
// }

// pub async fn test_cloudflare_v2(
//     Query(params): Query<HashMap<String, String>>,
// ) -> Result<(), AppError> {
//     // Get Request to https://api.cloudflare.com/client/v4/accounts/{account_id}/stream
//     // Query param start 2021-05-03T00:00:00Z
//     let startdate = params.get("startdate").unwrap().clone();
//     let thresh = params.get("thresh").unwrap().parse::<usize>().unwrap();

//     let url = format!(
//         "https://api.cloudflare.com/client/v4/accounts/{}/stream",
//         CLOUDFLARE_ACCOUNT_ID
//     );
//     let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

//     let client = reqwest::Client::new();
//     let mut num_vids = 0;
//     let mut start_time = startdate;
//     let mut cnt = 0;
//     let mut hashset: HashSet<String> = HashSet::new();

//     loop {
//         let response = client
//             .get(&url)
//             .bearer_auth(&bearer_token)
//             .query(&[("asc", "true"), ("start", &start_time)])
//             .send()
//             .await?;
//         // log::info!("Response: {:?}", response);
//         if response.status() != 200 {
//             log::error!(
//                 "Failed to get response from Cloudflare: {:?}",
//                 response.text().await?
//             );
//             return Err(anyhow::anyhow!("Failed to get response from Cloudflare").into());
//         }

//         let body = response.text().await?;
//         let result: CFStreamResult = serde_json::from_str(&body)?;
//         let mut result_vec = result.result.clone();

//         // add uids to hashset
//         for r in &result_vec {
//             hashset.insert(r.uid.clone());

//             if hashset.len() >= thresh {
//                 log::error!("Last above: {:?}", r);
//                 break;
//             }
//         }

//         if cnt > 0 {
//             result_vec.remove(0);
//         }

//         num_vids += result_vec.len();
//         if result_vec.len() == 0 {
//             break;
//         }
//         let last = &result.result[result.result.len() - 1];
//         start_time = last.created.clone();
//         cnt += 1;

//         if cnt > 10000 {
//             log::info!("Breaking after 10000 iterations");
//             break;
//         }

//         if hashset.len() >= thresh {
//             // hashset retain only 100 elements
//             log::error!("Last: {:?}", last);
//             break;
//         }
//     }

//     log::info!("Total number of videos: {}", num_vids);
//     log::info!("Total number of videos in hashset: {}", hashset.len());
//     // log::info!("Hashset: {:?}", hashset);

//     Ok(())
// }

// pub async fn get_cf_info(Query(params): Query<HashMap<String, String>>) -> Result<(), AppError> {
//     let uid = params.get("uid").unwrap().clone();
//     let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

//     // CALL GET https://api.cloudflare.com/client/v4/accounts/{account_id}/stream/{identifier}
//     let url = format!(
//         "https://api.cloudflare.com/client/v4/accounts/{}/stream/{}",
//         CLOUDFLARE_ACCOUNT_ID, uid
//     );

//     let client = reqwest::Client::new();
//     let response = client.get(&url).bearer_auth(&bearer_token).send().await?;

//     if response.status() != 200 {
//         log::error!(
//             "Failed to get response from Cloudflare: {:?}",
//             response.text().await?
//         );
//         return Err(anyhow::anyhow!("Failed to get response from Cloudflare").into());
//     }

//     let body = response.text().await?;
//     log::info!("Response: {:?}", body);

//     Ok(())
// }

// pub async fn test_gcs(Query(params): Query<HashMap<String, String>>) -> Result<(), AppError> {
//     // Call GET https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{uid}/downloads/default.mp4 and download the video content

//     let uid = params.get("uid").unwrap().clone();

//     tokio::spawn(async move {
//         let res = upload_gcs(&uid).await;
//         log::info!("Upload GCS Response: {:?}", res);
//     });

//     Ok(())
// }
