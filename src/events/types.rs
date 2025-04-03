use candid::Principal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use yral_metrics::metrics::{
    like_video::LikeVideo, sealed_metric::SealedMetric,
    video_duration_watched::VideoDurationWatched, video_watched::VideoWatched,
};

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(tag = "event")]
pub enum AnalyticsEvent {
    VideoWatched(VideoWatched),
    VideoDurationWatched(VideoDurationWatched),
    LikeVideo(LikeVideo),
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
