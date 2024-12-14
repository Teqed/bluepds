use std::{net::SocketAddr, str::FromStr};

use axum::{extract::FromRef, response::IntoResponse, routing::get, Router};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use anyhow::Context;

async fn index() -> impl IntoResponse {
    "hello"
}

#[derive(Clone, FromRef)]
struct AppState {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let app = Router::new()
        .route("/", get(index))
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {});

    // Required endpoints:
    // U /xrpc/_health (undocumented, but impl by reference PDS)
    //
    // U /xrpc/com.atproto.identity.resolveHandle
    // U /xrpc/com.atproto.identity.updateHandle
    //
    // U /xrpc/com.atproto.server.describeServer
    // U /xrpc/com.atproto.server.createSession
    // U /xrpc/com.atproto.server.getSession
    //
    // A /xrpc/com.atproto.repo.applyWrites
    // A /xrpc/com.atproto.repo.createRecord
    // A /xrpc/com.atproto.repo.putRecord
    // A /xrpc/com.atproto.repo.deleteRecord
    // U /xrpc/com.atproto.repo.describeRepo
    // U /xrpc/com.atproto.repo.getRecord
    // U /xrpc/com.atproto.repo.listRecords
    // A /xrpc/com.atproto.repo.uploadBlob
    //
    // U /xrpc/com.atproto.sync.getBlob
    // U /xrpc/com.atproto.sync.getBlocks
    // U /xrpc/com.atproto.sync.getLatestCommit
    // U /xrpc/com.atproto.sync.getRecord
    // U /xrpc/com.atproto.sync.getRepoStatus
    // U /xrpc/com.atproto.sync.getRepo
    // U /xrpc/com.atproto.sync.listBlobs
    // U /xrpc/com.atproto.sync.listRepos
    // U /xrpc/com.atproto.sync.subscribeRepos

    let addr = SocketAddr::from_str("0.0.0.0:8000").unwrap();
    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind address")?;

    tracing::info!("listening on {addr}");
    tracing::info!("connect to: http://127.0.0.1:{}", addr.port());

    axum::serve(listener, app.into_make_service())
        .await
        .context("failed to serve app")
}
