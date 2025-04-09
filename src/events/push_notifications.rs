use std::str::FromStr;

use crate::app_state::AppState;
use anyhow::Result;
use candid::Principal;
use serde_json::Value;

struct Notification {
    title: String,
    body: String,
    image: String,
    // badge: String,
    // data: String,
}

async fn notify_principal(
    target_principal: &str,
    notif: Notification,
    app_state: &AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let url = "https://fcm.googleapis.com/v1/projects/hot-or-not-feed-intelligence/messages:send";
    let token = app_state
        .get_access_token(&["https://www.googleapis.com/auth/firebase.messaging"])
        .await;

    let metadata_client = &app_state.yral_metadata_client;
    let user_principal = Principal::from_str(target_principal).unwrap();
    let user_metadata = metadata_client
        .get_user_metadata(user_principal)
        .await?
        .ok_or("metadata for principal not found")?;
    let notification_key = user_metadata
        .notification_key
        .ok_or("notification key not found")?;
    let data = format!(
        r#"{{
            "message": {{
                "token": "{}",
                "notification": {{
                    "title": "{}",
                    "body": "{}"
                }}
            }}
        }}"#,
        notification_key.key, notif.title, notif.body
    );

    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await;

    if response.is_ok() && response.as_ref().unwrap().status().is_success() {
        log::info!("Notification sent successfully");
    } else {
        log::error!("Error sending notification: {:?}", response);
        return Err(anyhow::anyhow!("Error sending notification").into());
    }

    Ok(())
}

pub async fn dispatch_notif(
    event_type: &str,
    params: Value,
    app_state: &AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    match event_type {
        // LikeVideo
        "like_video" => {
            let target_principal = params["publisher_user_id"].as_str().unwrap();
            let like_count = params["like_count"].as_u64().unwrap();
            let liker_name = params["display_name"].as_str().unwrap_or("A YRAL user");
            let notif = Notification {
                title: "New Like".to_string(),
                body: format!("{}{} liked your video", liker_name, {
                    if like_count > 1 {
                        format!(" and {} others", like_count - 1)
                    } else {
                        "".to_string()
                    }
                }),
                image: "https://imagedelivery.net/abXI9nS4DYYtyR1yFFtziA/gob.42/public".to_string(),
            };
            notify_principal(target_principal, notif, app_state).await?;
        }
        // ShareVideo
        "share_video" => {
            let target_principal = params["publisher_user_id"].as_str().unwrap();
            // let target_principal = "qd72i-rom2e-dycfz-dlylp-rfux5-5k56f-h4u3a-yz4xl-lcvkk-hatrh-zae";
            let sharer_name = params["display_name"].as_str().unwrap_or("A YRAL user");
            let notif = Notification {
                title: "New Share".to_string(),
                body: format!("{} shared your video", sharer_name),
                image: "https://imagedelivery.net/abXI9nS4DYYtyR1yFFtziA/gob.42/public".to_string(),
            };
            notify_principal(target_principal, notif, app_state).await?;
        }
        // VideoWatched
        "video_viewed" => {
            let target_principal = params["publisher_user_id"].as_str().unwrap();
            let viewer_name = params["display_name"].as_str().unwrap_or("A YRAL user");
            let view_count = params["view_count"].as_u64().unwrap();
            let notif = Notification {
                title: "New View".to_string(),
                body: format!("{}{} viewed your video", viewer_name, {
                    if view_count > 1 {
                        format!(" and {} others", view_count - 1)
                    } else {
                        "".to_string()
                    }
                }),
                image: "https://imagedelivery.net/abXI9nS4DYYtyR1yFFtziA/gob.42/public".to_string(),
            };
            notify_principal(target_principal, notif, app_state).await?;
        }
        // VideoUploadUnsuccessful
        "video_upload_unsuccessful" => {
            let target_principal = params["user_id"].as_str().unwrap();
            let notif = Notification {
                title: "Upload Failed".to_string(),
                body: "Your video upload was unsuccessful".to_string(),
                image: "https://imagedelivery.net/abXI9nS4DYYtyR1yFFtziA/gob.42/public".to_string(),
            };
            notify_principal(target_principal, notif, app_state).await?;
        }
        // VideoUploadSuccessful
        "video_upload_successful" => {
            let target_principal = params["user_id"].as_str().unwrap();
            let notif = Notification {
                title: "Upload Successful".to_string(),
                body: "Your video upload was successful".to_string(),
                image: "https://imagedelivery.net/abXI9nS4DYYtyR1yFFtziA/gob.42/public".to_string(),
            };
            notify_principal(target_principal, notif, app_state).await?;
        }
        _ => {}
    }
    Ok(())
}
