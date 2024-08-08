use crate::events::event::get_access_token;
use serde_json::Value;
use anyhow::{Context, Result};

struct Notification {
    title: String,
    body: String,
    image: String,
    // badge: String,
    // data: String,
}

async fn notify_principal(target_principal: &str, notif: Notification) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let url = "https://fcm.googleapis.com/v1/projects/hot-or-not-feed-intelligence/messages:send";
    let token = get_access_token(&["https://www.googleapis.com/auth/firebase.messaging"]).await;

    let data = format!(
        r#"{{
            "message": {{
                "topic": "{}",
                "notification": {{
                    "title": "{}",
                    "body": "{}"
                }}
            }}
        }}"#,
        target_principal, notif.title, notif.body
    );

    let response = client.post(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await;

    if response.is_err() {
        log::error!("Error sending notification: {:?}", response);
        return Err(anyhow::anyhow!("Error sending notification").into());
    } else {
        log::info!("Notification sent successfully");
    }
    Ok(())
}

pub async fn subscribe_device_to_topic(target_principal: &str, topic: &str) -> Result<()> {
    println!("Subscribing\n{}\nto topic\n{}", target_principal, topic);
    let client = reqwest::Client::new();
    let url = format!("https://iid.googleapis.com/iid/v1/{}/rel/topics/{}", target_principal, topic);
    let token = get_access_token(&["https://www.googleapis.com/auth/firebase.messaging"]).await;

    let response = client.post(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("access_token_auth", "true")
        .send()
        .await;

    if response.is_err() {
        log::error!("Error subscribing to topic: {:?}", response);
        return Err(anyhow::anyhow!("Error subscribing to topic").into());
    } else {
        log::info!("Subscribed to topic successfully");
    }
    Ok(())
}

pub async fn dispatch_notif(event_type: &str, params: Value) -> Result<(), Box<dyn std::error::Error>> {
    match event_type {
        // LikeVideo
        "like_video" => {
            // let target_principal = params["publisher_user_id"].as_str().unwrap();
            let target_principal = "qd72i-rom2e-dycfz-dlylp-rfux5-5k56f-h4u3a-yz4xl-lcvkk-hatrh-zae";
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
            notify_principal(target_principal, notif).await?;
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
            notify_principal(target_principal, notif).await?;
        }
        // VideoWatched
        "video_viewed" => {
            let target_principal = params["publisher_user_id"].as_str().unwrap();
            // let target_principal = "qd72i-rom2e-dycfz-dlylp-rfux5-5k56f-h4u3a-yz4xl-lcvkk-hatrh-zae";
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
            notify_principal(target_principal, notif).await?;
        }
        // VideoUploadUnsuccessful
        "video_upload_unsuccessful" => {
            let target_principal = params["user_id"].as_str().unwrap();
            // let target_principal = "qd72i-rom2e-dycfz-dlylp-rfux5-5k56f-h4u3a-yz4xl-lcvkk-hatrh-zae";
            let notif = Notification {
                title: "Upload Failed".to_string(),
                body: "Your video upload was unsuccessful".to_string(),
                image: "https://imagedelivery.net/abXI9nS4DYYtyR1yFFtziA/gob.42/public".to_string(),
            };
            notify_principal(target_principal, notif).await?;
        }
        // VideoUploadSuccessful
        "video_upload_successful" => {
            let target_principal = params["user_id"].as_str().unwrap();
            // let target_principal = "qd72i-rom2e-dycfz-dlylp-rfux5-5k56f-h4u3a-yz4xl-lcvkk-hatrh-zae";
            let notif = Notification {
                title: "Upload Successful".to_string(),
                body: "Your video upload was successful".to_string(),
                image: "https://imagedelivery.net/abXI9nS4DYYtyR1yFFtziA/gob.42/public".to_string(),
            };
            notify_principal(target_principal, notif).await?;
        }
        _ => {
            log::error!("Event not found");
        }
    }
    Ok(())
}