use atrium_api::com::atproto::sync::subscribe_repos;
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use constcat::concat;

use crate::{firehose::FirehoseProducer, AppState};

async fn subscribe_repos(
    ws: WebSocketUpgrade,
    State(fh): State<FirehoseProducer>,
) -> impl IntoResponse {
    ws.on_upgrade(|ws| async move {
        fh.connect(ws).await;
    })
}

pub fn routes() -> axum::Router<AppState> {
    Router::new().route(concat!("/", subscribe_repos::NSID), get(subscribe_repos))
}
