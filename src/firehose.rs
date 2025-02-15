use std::collections::VecDeque;

use atrium_api::com::atproto::sync;
use axum::extract::ws::WebSocket;
use tracing::info;

enum FirehoseMessage {
    Broadcast(sync::subscribe_repos::Message),
    Connect(axum::extract::ws::WebSocket),
}

pub struct Commit {}

#[derive(Clone, Debug)]
pub struct FirehoseProducer {
    tx: tokio::sync::mpsc::Sender<FirehoseMessage>,
}

impl FirehoseProducer {
    /// Broadcast an `#account` event.
    pub async fn account(&self, account: impl Into<sync::subscribe_repos::Account>) {
        let _ = self
            .tx
            .send(FirehoseMessage::Broadcast(
                sync::subscribe_repos::Message::Account(Box::new(account.into())),
            ))
            .await;
    }

    /// Broadcast an `#identity` event.
    pub async fn identity(&self, identity: impl Into<sync::subscribe_repos::Identity>) {
        let _ = self
            .tx
            .send(FirehoseMessage::Broadcast(
                sync::subscribe_repos::Message::Identity(Box::new(identity.into())),
            ))
            .await;
    }

    /// Broadcast a `#commit` event.
    pub async fn commit(&self, commit: impl Into<sync::subscribe_repos::Commit>) {
        let _ = self
            .tx
            .send(FirehoseMessage::Broadcast(
                sync::subscribe_repos::Message::Commit(Box::new(commit.into())),
            ))
            .await;
    }

    pub async fn connect(&self, ws: WebSocket) {
        let _ = self.tx.send(FirehoseMessage::Connect(ws)).await;
    }
}

pub async fn spawn() -> (tokio::task::JoinHandle<()>, FirehoseProducer) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    let handle = tokio::spawn(async move {
        let mut clients = vec![];
        let mut history = VecDeque::with_capacity(1000);

        while let Some(msg) = rx.recv().await {
            match msg {
                FirehoseMessage::Broadcast(msg) => {
                    let enc = serde_ipld_dagcbor::to_vec(&msg).unwrap().into_boxed_slice();
                    history.push_back(enc.clone());

                    info!(
                        "Broadcasting message {} ({} bytes)",
                        match msg {
                            sync::subscribe_repos::Message::Account(_) => "#account",
                            sync::subscribe_repos::Message::Commit(_) => "#commit",
                            sync::subscribe_repos::Message::Handle(_) => "#handle",
                            sync::subscribe_repos::Message::Identity(_) => "#identity",
                            sync::subscribe_repos::Message::Info(_) => "#info",
                            sync::subscribe_repos::Message::Migrate(_) => "#migrate",
                            sync::subscribe_repos::Message::Tombstone(_) => "#tombstone",
                        },
                        enc.len()
                    );
                }
                FirehoseMessage::Connect(ws) => {
                    clients.push(ws);
                }
            }
        }
    });

    (handle, FirehoseProducer { tx })
}
