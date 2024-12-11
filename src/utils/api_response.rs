use std::error::Error;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    success: bool,
    error: Option<String>,
    data: Option<T>,
}

impl<T> From<Result<T, Box<dyn Error + Send + Sync>>> for ApiResponse<T>
where
    T: Sized,
{
    fn from(value: Result<T, Box<dyn Error + Send + Sync>>) -> Self {
        match value {
            Ok(res) => ApiResponse {
                success: true,
                error: None,
                data: Some(res),
            },
            Err(e) => ApiResponse {
                success: false,
                error: Some(e.to_string()),
                data: None,
            },
        }
    }
}
