use hmac::{Hmac, Mac} ;
use sha2::Sha256;
use hex;
use std::str::FromStr;

#[derive(Debug)]
pub struct WebhookSignature {
    pub timestamp: u64,
    pub signature: String,
}

// Webhook-Signature: time=1230811200,sig1=60493ec9388b44585a29543bcf0de62e377d4da393246a8b1c901d0e3e672404
impl FromStr for WebhookSignature {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return Err("Invalid signature format".to_string());
        }

        let timestamp_part: Vec<&str> = parts[0].split('=').collect();
        if timestamp_part.len() != 2 {
            return Err("Invalid timestamp format".to_string());
        }

        let signature_part: Vec<&str> = parts[1].split('=').collect();
        if signature_part.len() != 2 {
            return Err("Invalid signature format".to_string());
        }

        let timestamp = timestamp_part[1].parse::<u64>().map_err(|_| "Invalid timestamp".to_string())?;
        let signature = signature_part[1].to_string();

        Ok(WebhookSignature { timestamp, signature })
    }
}


pub fn verify_signature(secret: &str, webhook_signature: &WebhookSignature, body: &str) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{}.{}", webhook_signature.timestamp, body).as_bytes());

    let expected_signature = hex::encode(mac.finalize().into_bytes());

    // Use constant-time comparison function if available to compare signatures
    // for security reasons.
    expected_signature == webhook_signature.signature
}
