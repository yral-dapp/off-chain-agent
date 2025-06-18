pub mod delete_user;

use std::sync::Arc;

use axum::routing::delete;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::app_state::AppState;

pub fn user_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(delete_user::handle_delete_user))
        .with_state(state)
}
