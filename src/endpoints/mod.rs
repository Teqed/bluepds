use axum::Router;

use crate::AppState;

mod server;

pub fn routes() -> Router<AppState> {
    Router::new().merge(server::routes())
}
