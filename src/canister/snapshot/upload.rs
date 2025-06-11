use std::process::Stdio;

use candid::Principal;
use chrono::{Duration, Utc};
use tokio::{io::AsyncWriteExt, process::Command};

use crate::consts::{CANISTER_BACKUPS_BUCKET, STORJ_BACKUP_CANISTER_ACCESS_GRANT};

pub async fn upload_snapshot_to_storj_v2(
    canister_id: Principal,
    object_id: String,
    snapshot_bytes: Vec<u8>,
) -> Result<(), anyhow::Error> {
    let access_grant = &STORJ_BACKUP_CANISTER_ACCESS_GRANT.to_string();
    let bucket_name = CANISTER_BACKUPS_BUCKET;
    let dest = format!("sj://{bucket_name}/{canister_id}/{object_id}");

    let mut child = Command::new("uplink")
        .args([
            "cp",
            "--interactive=false",
            "--analytics=false",
            "--progress=false",
            "--access",
            access_grant,
            "-",
            dest.as_str(), // from stdin to dest
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()?;

    let mut pipe = child.stdin.take().expect("Stdin pipe to be opened for us");

    pipe.write_all(&snapshot_bytes).await?;

    let ninety_days_ago = Utc::now() - Duration::days(90);
    let date_str_ninety_days_ago = ninety_days_ago.format("%Y-%m-%d").to_string();

    let to_delete_dest = format!("sj://{bucket_name}/{canister_id}/{date_str_ninety_days_ago}");

    let mut child = Command::new("uplink")
        .args(["rm", "--access", access_grant, to_delete_dest.as_str()])
        .spawn()?;

    child.wait().await?;

    Ok(())
}
