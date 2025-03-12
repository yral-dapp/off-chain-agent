use std::time::UNIX_EPOCH;
use yral_canisters_client::individual_user_template::SystemTime;

pub fn system_time_to_custom(time: std::time::SystemTime) -> SystemTime {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    SystemTime {
        nanos_since_epoch: duration.subsec_nanos(),
        secs_since_epoch: duration.as_secs(),
    }
}
