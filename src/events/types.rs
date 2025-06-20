use candid::Principal;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use yral_metrics::metrics::{
    like_video::LikeVideo, sealed_metric::SealedMetric,
    video_duration_watched::VideoDurationWatched, video_watched::VideoWatched,
};

#[derive(Serialize, Clone, Debug, ToSchema)]
#[serde(tag = "event")]
pub enum AnalyticsEvent {
    VideoWatched(VideoWatched),
    VideoDurationWatched(VideoDurationWatched),
    LikeVideo(LikeVideo),
}

// open issues for tagged and untagged enums - https://github.com/serde-rs/json/issues/1046 and https://github.com/serde-rs/json/issues/1108
impl<'de> Deserialize<'de> for AnalyticsEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // First deserialize to a generic Value to handle arbitrary_precision issues
        let value = Value::deserialize(deserializer)?;

        // Then try to deserialize from the Value to our enum
        match value.get("event").and_then(|v| v.as_str()) {
            Some("VideoWatched") => {
                let video_watched: VideoWatched =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::VideoWatched(video_watched))
            }
            Some("VideoDurationWatched") => {
                let video_duration_watched: VideoDurationWatched =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::VideoDurationWatched(video_duration_watched))
            }
            Some("LikeVideo") => {
                let like_video: LikeVideo =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::LikeVideo(like_video))
            }
            Some(event_type) => Err(serde::de::Error::custom(format!(
                "Unknown event type: {}",
                event_type
            ))),
            None => Err(serde::de::Error::custom("Missing 'event' field")),
        }
    }
}

macro_rules! delegate_metric_method {
    ($self:ident, $method:ident) => {
        match $self {
            AnalyticsEvent::VideoWatched(event) => event.$method(),
            AnalyticsEvent::VideoDurationWatched(event) => event.$method(),
            AnalyticsEvent::LikeVideo(event) => event.$method(),
        }
    };
    // Overload for methods that need serde_json::to_value
    ($self:ident, $method:ident, to_value) => {
        match $self {
            AnalyticsEvent::VideoWatched(event) => serde_json::to_value(event).unwrap(),
            AnalyticsEvent::VideoDurationWatched(event) => serde_json::to_value(event).unwrap(),
            AnalyticsEvent::LikeVideo(event) => serde_json::to_value(event).unwrap(),
        }
    };
}

impl SealedMetric for AnalyticsEvent {
    fn tag(&self) -> String {
        delegate_metric_method!(self, tag)
    }

    fn user_id(&self) -> Option<String> {
        delegate_metric_method!(self, user_id)
    }

    fn user_canister(&self) -> Option<Principal> {
        delegate_metric_method!(self, user_canister)
    }
}

impl AnalyticsEvent {
    pub fn params(&self) -> Value {
        // Use the overloaded macro variant for to_value
        delegate_metric_method!(self, params, to_value)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VideoDurationWatchedParams {
    pub percentage_watched: f64,
    #[serde(default)]
    pub nsfw_probability: f64,
    pub canister_id: Principal,
    pub publisher_canister_id: Principal,
    pub post_id: u64,
    pub video_id: String,
}
