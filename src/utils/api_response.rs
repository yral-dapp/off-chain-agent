use std::error::Error;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub error: Option<String>,
    pub data: Option<T>,
}

impl<T> From<Result<T, Box<dyn Error>>> for ApiResponse<T>
where
    T: Sized,
{
    fn from(value: Result<T, Box<dyn Error>>) -> Self {
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
