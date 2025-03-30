//! The firehose module.
use std::{collections::VecDeque, time::Duration};

use anyhow::{Result, bail};
use atrium_api::{
    com::atproto::sync::{self},
    types::string::{Datetime, Did},
};
use atrium_repo::Cid;
use axum::extract::ws::{Message, WebSocket};
use metrics::{counter, gauge};
use rand::Rng as _;
use serde::{Serialize, ser::SerializeMap as _};
use tracing::{debug, error, info, warn};

use crate::{
    Client,
    config::AppConfig,
    metrics::{FIREHOSE_HISTORY, FIREHOSE_LISTENERS, FIREHOSE_MESSAGES, FIREHOSE_SEQUENCE},
};

enum FirehoseMessage {
    Broadcast(sync::subscribe_repos::Message),
    Connect(Box<(WebSocket, Option<i64>)>),
}

enum FrameHeader {
    Message(String),
    Error,
}

impl Serialize for FrameHeader {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        match self {
            Self::Message(s) => {
                map.serialize_key("op")?;
                map.serialize_value(&1)?;
                map.serialize_key("t")?;
                map.serialize_value(s.as_str())?;
            }
            Self::Error => {
                map.serialize_key("op")?;
                map.serialize_value(&-1)?;
            }
        }

        map.end()
    }
}

/// A repository operation.
pub(crate) enum RepoOp {
    /// Create a new record.
    Create {
        /// The CID of the record.
        cid: Cid,
        /// The path of the record.
        path: String,
    },
    /// Update an existing record.
    Update {
        /// The CID of the record.
        cid: Cid,
        /// The path of the record.
        path: String,
        /// The previous CID of the record.
        prev: Cid,
    },
    /// Delete an existing record.
    Delete {
        /// The path of the record.
        path: String,
        /// The previous CID of the record.
        prev: Cid,
    },
}

impl From<RepoOp> for sync::subscribe_repos::RepoOp {
    fn from(val: RepoOp) -> Self {
        let (action, cid, path) = match val {
            RepoOp::Create { cid, path } => ("create", Some(cid), path),
            RepoOp::Update {
                cid,
                path,
                prev: _prev,
            } => ("update", Some(cid), path),
            RepoOp::Delete { path, prev: _prev } => ("delete", None, path),
        };

        sync::subscribe_repos::RepoOpData {
            action: action.to_owned(),
            cid: cid.map(atrium_api::types::CidLink),
            path,
        }
        .into()
    }
}

/// A commit to the repository.
pub(crate) struct Commit {
    /// The car file containing the commit blocks.
    pub car: Vec<u8>,
    /// The operations performed in this commit.
    pub ops: Vec<RepoOp>,
    /// The CID of the commit.
    pub cid: Cid,
    /// The revision of the commit.
    pub rev: String,
    /// The DID of the repository changed.
    pub did: Did,
    /// Blobs that were created in this commit.
    pub blobs: Vec<Cid>,
}

impl From<Commit> for sync::subscribe_repos::Commit {
    fn from(val: Commit) -> Self {
        sync::subscribe_repos::CommitData {
            blobs: val
                .blobs
                .into_iter()
                .map(atrium_api::types::CidLink)
                .collect::<Vec<_>>(),
            blocks: val.car,
            commit: atrium_api::types::CidLink(val.cid),
            ops: val.ops.into_iter().map(Into::into).collect::<Vec<_>>(),
            prev: None,
            rebase: false,
            repo: val.did,
            rev: val.rev,
            seq: 0,
            since: None,
            time: Datetime::now(),
            too_big: false,
        }
        .into()
    }
}

/// A firehose producer. This is used to transmit messages to the firehose for broadcast.
#[derive(Clone, Debug)]
pub(crate) struct FirehoseProducer {
    tx: tokio::sync::mpsc::Sender<FirehoseMessage>,
}

impl FirehoseProducer {
    /// Broadcast an `#account` event.
    pub(crate) async fn account(&self, account: impl Into<sync::subscribe_repos::Account>) {
        drop(
            self.tx
                .send(FirehoseMessage::Broadcast(
                    sync::subscribe_repos::Message::Account(Box::new(account.into())),
                ))
                .await,
        );
    }

    /// Broadcast an `#identity` event.
    pub(crate) async fn identity(&self, identity: impl Into<sync::subscribe_repos::Identity>) {
        drop(
            self.tx
                .send(FirehoseMessage::Broadcast(
                    sync::subscribe_repos::Message::Identity(Box::new(identity.into())),
                ))
                .await,
        );
    }

    /// Broadcast a `#commit` event.
    pub(crate) async fn commit(&self, commit: impl Into<sync::subscribe_repos::Commit>) {
        drop(
            self.tx
                .send(FirehoseMessage::Broadcast(
                    sync::subscribe_repos::Message::Commit(Box::new(commit.into())),
                ))
                .await,
        );
    }

    /// Handle client connection.
    pub(crate) async fn client_connection(&self, ws: WebSocket, cursor: Option<i64>) {
        drop(
            self.tx
                .send(FirehoseMessage::Connect(Box::new((ws, cursor))))
                .await,
        );
    }
}

/// Serialize a message.
async fn serialize_message(
    seq: u64,
    mut msg: sync::subscribe_repos::Message,
) -> (&'static str, Vec<u8>) {
    let mut dummy_seq = 0_i64;
    let (ty, nseq) = match &mut msg {
        sync::subscribe_repos::Message::Account(m) => ("#account", &mut m.seq),
        sync::subscribe_repos::Message::Commit(m) => ("#commit", &mut m.seq),
        sync::subscribe_repos::Message::Handle(m) => ("#handle", &mut m.seq),
        sync::subscribe_repos::Message::Identity(m) => ("#identity", &mut m.seq),
        sync::subscribe_repos::Message::Info(_m) => ("#info", &mut dummy_seq),
        sync::subscribe_repos::Message::Migrate(m) => ("#migrate", &mut m.seq),
        sync::subscribe_repos::Message::Tombstone(m) => ("#tombstone", &mut m.seq),
    };

    // Set the sequence number.
    *nseq = seq as i64;

    let hdr = FrameHeader::Message(ty.to_owned());

    let mut frame = Vec::new();
    serde_ipld_dagcbor::to_writer(&mut frame, &hdr).expect("should serialize header");
    serde_ipld_dagcbor::to_writer(&mut frame, &msg).expect("should serialize message");

    (ty, frame)
}

/// Broadcast a message out to all clients.
async fn broadcast_message(clients: &mut Vec<WebSocket>, msg: Message) -> Result<()> {
    counter!(FIREHOSE_MESSAGES).increment(1);

    for i in (0..clients.len()).rev() {
        let client = &mut clients[i];
        if let Err(e) = client.send(msg.clone()).await {
            debug!("Firehose client disconnected: {e}");
            drop(clients.remove(i));
        }
    }

    gauge!(FIREHOSE_LISTENERS).set(clients.len() as f64);
    Ok(())
}

/// Handle a new connection from a websocket client created by subscribeRepos.
async fn handle_connect(
    mut ws: WebSocket,
    seq: u64,
    history: &VecDeque<(u64, &str, sync::subscribe_repos::Message)>,
    cursor: Option<i64>,
) -> Result<WebSocket> {
    if let Some(cursor) = cursor {
        let mut frame = Vec::new();
        let cursor = cursor as u64;

        // Cursor specified; attempt to backfill the consumer.
        if cursor > seq {
            let hdr = FrameHeader::Error;
            let msg = sync::subscribe_repos::Error::FutureCursor(Some(format!(
                "cursor {cursor} is greater than the current sequence number {seq}"
            )));

            serde_ipld_dagcbor::to_writer(&mut frame, &hdr).expect("should serialize header");
            serde_ipld_dagcbor::to_writer(&mut frame, &msg).expect("should serialize message");

            // Drop the connection.
            drop(ws.send(Message::binary(frame)).await);
            bail!(
                "connection dropped: cursor {cursor} is greater than the current sequence number {seq}"
            );
        }

        for (seq, ty, msg) in history.iter() {
            if *seq > cursor {
                break;
            }

            let hdr = FrameHeader::Message(ty.to_string());
            serde_ipld_dagcbor::to_writer(&mut frame, &hdr).expect("should serialize header");
            serde_ipld_dagcbor::to_writer(&mut frame, msg).expect("should serialize message");

            if let Err(e) = ws.send(Message::binary(frame.clone())).await {
                debug!("Firehose client disconnected during backfill: {e}");
                break;
            }

            // Clear out the frame to begin a new one.
            frame.clear();
        }
    }

    Ok(ws)
}

/// Reconnect to upstream relays.
pub(crate) async fn reconnect_relays(client: &Client, config: &AppConfig) {
    // Avoid connecting to upstream relays in test mode.
    if config.test {
        return;
    }

    info!("attempting to reconnect to upstream relays");
    for relay in &config.firehose.relays {
        let host = match relay.host_str() {
            Some(host) => host,
            None => {
                warn!("relay {} has no host specified", relay);
                continue;
            }
        };

        let r = client
            .post(format!("https://{host}/xrpc/com.atproto.sync.requestCrawl"))
            .json(&serde_json::json!({
                "hostname": format!("https://{}", config.host_name)
            }))
            .send()
            .await;

        let r = match r {
            Ok(r) => r,
            Err(e) => {
                error!("failed to hit upstream relay {host}: {e}");
                continue;
            }
        };

        let s = r.status();
        if let Err(e) = r.error_for_status_ref() {
            error!("failed to hit upstream relay {host}: {e}");
        }

        let b = r.json::<serde_json::Value>().await;
        if let Ok(b) = b {
            info!("relay {host}: {} {}", s, b);
        } else {
            info!("relay {host}: {}", s);
        }
    }
}

/// The main entrypoint for the firehose.
///
/// This will broadcast all updates in this PDS out to anyone who is listening.
///
/// Reference: https://atproto.com/specs/sync
pub(crate) async fn spawn(
    client: Client,
    config: AppConfig,
) -> (tokio::task::JoinHandle<()>, FirehoseProducer) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    let handle = tokio::spawn(async move {
        let mut clients: Vec<WebSocket> = Vec::new();
        let mut history = VecDeque::with_capacity(1000);
        let mut seq = 1_u64;

        // TODO: We should use `com.atproto.sync.notifyOfUpdate` to reach out to relays
        // that may have disconnected from us due to timeout.

        loop {
            match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
                Ok(msg) => match msg {
                    Some(FirehoseMessage::Broadcast(msg)) => {
                        let (ty, by) = serialize_message(seq, msg.clone()).await;

                        history.push_back((seq, ty, msg));
                        gauge!(FIREHOSE_HISTORY).set(history.len() as f64);

                        info!(
                            "Broadcasting message {} {} to {} clients",
                            seq,
                            ty,
                            clients.len()
                        );

                        counter!(FIREHOSE_SEQUENCE).absolute(seq);
                        seq = seq.wrapping_add(1);

                        drop(broadcast_message(&mut clients, Message::binary(by)).await);
                    }
                    Some(FirehoseMessage::Connect(ws_cursor)) => {
                        let (ws, cursor) = *ws_cursor;
                        match handle_connect(ws, seq, &history, cursor).await {
                            Ok(r) => {
                                gauge!(FIREHOSE_LISTENERS).increment(1);
                                clients.push(r);
                            }
                            Err(e) => {
                                error!("failed to connect new client: {e}");
                            }
                        }
                    }
                    // All producers have been destroyed.
                    None => break,
                },
                Err(_) => {
                    if clients.is_empty() {
                        reconnect_relays(&client, &config).await;
                    }

                    let contents = rand::thread_rng()
                        .sample_iter(rand::distributions::Alphanumeric)
                        .take(15)
                        .map(char::from)
                        .collect::<String>();

                    // Send a websocket ping message.
                    // Reference: https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/Writing_WebSocket_servers#pings_and_pongs_the_heartbeat_of_websockets
                    let message = Message::Ping(axum::body::Bytes::from_owner(contents));
                    drop(broadcast_message(&mut clients, message).await);
                }
            }
        }
    });

    (handle, FirehoseProducer { tx })
}
