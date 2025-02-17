use std::collections::VecDeque;

use atrium_api::{
    com::atproto::sync::{self},
    types::string::{Datetime, Did},
};
use atrium_repo::Cid;
use axum::extract::ws::{Message, WebSocket};
use serde::{ser::SerializeMap, Serialize};
use tracing::info;

enum FirehoseMessage {
    Broadcast(sync::subscribe_repos::Message),
    Connect(axum::extract::ws::WebSocket),
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
            FrameHeader::Message(s) => {
                map.serialize_key("op")?;
                map.serialize_value(&1)?;
                map.serialize_key("t")?;
                map.serialize_value(s.as_str())?;
            }
            FrameHeader::Error => {
                map.serialize_key("op")?;
                map.serialize_value(&-1)?;

                // TODO...
                todo!()
            }
        }

        map.end()
    }
}

pub enum RepoOp {
    Create { cid: Cid, path: String },
    Update { cid: Cid, path: String },
    Delete { path: String },
}

impl Into<sync::subscribe_repos::RepoOp> for RepoOp {
    fn into(self) -> sync::subscribe_repos::RepoOp {
        let (action, cid, path) = match self {
            RepoOp::Create { cid, path } => ("create", Some(cid), path),
            RepoOp::Update { cid, path } => ("update", Some(cid), path),
            RepoOp::Delete { path } => ("delete", None, path),
        };

        sync::subscribe_repos::RepoOpData {
            action: action.to_string(),
            cid: cid.map(atrium_api::types::CidLink),
            path,
        }
        .into()
    }
}

pub struct Commit {
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
}

impl Into<sync::subscribe_repos::Commit> for Commit {
    fn into(self) -> sync::subscribe_repos::Commit {
        sync::subscribe_repos::CommitData {
            blobs: Vec::new(),
            blocks: self.car,
            commit: atrium_api::types::CidLink(self.cid),
            ops: self.ops.into_iter().map(Into::into).collect::<Vec<_>>(),
            prev: None,
            rebase: false,
            repo: self.did,
            rev: self.rev,
            seq: 0,
            since: None,
            time: Datetime::now(),
            too_big: false,
        }
        .into()
    }
}

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

    pub async fn client_connection(&self, ws: WebSocket) {
        let _ = self.tx.send(FirehoseMessage::Connect(ws)).await;
    }
}

pub async fn spawn() -> (tokio::task::JoinHandle<()>, FirehoseProducer) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    let handle = tokio::spawn(async move {
        let mut clients: Vec<WebSocket> = Vec::new();
        let mut history = VecDeque::with_capacity(1000);
        let mut seq = 0i64; // TODO: This should be saved in the database.

        while let Some(msg) = rx.recv().await {
            match msg {
                FirehoseMessage::Broadcast(mut msg) => {
                    let enc = serde_ipld_dagcbor::to_vec(&msg).unwrap().into_boxed_slice();

                    let mut dummy_seq = 0i64;
                    let (ty, nseq) = match &mut msg {
                        sync::subscribe_repos::Message::Account(m) => ("#account", &mut m.seq),
                        sync::subscribe_repos::Message::Commit(m) => ("#commit", &mut m.seq),
                        sync::subscribe_repos::Message::Handle(m) => ("#handle", &mut m.seq),
                        sync::subscribe_repos::Message::Identity(m) => ("#identity", &mut m.seq),
                        sync::subscribe_repos::Message::Info(_m) => ("#info", &mut dummy_seq),
                        sync::subscribe_repos::Message::Migrate(m) => ("#migrate", &mut m.seq),
                        sync::subscribe_repos::Message::Tombstone(m) => ("#tombstone", &mut m.seq),
                    };

                    // Increment the sequence number.
                    *nseq = seq;
                    seq += 1;

                    history.push_back(msg.clone());

                    info!("Broadcasting message {} ({} bytes)", ty, enc.len());

                    let hdr = FrameHeader::Message(ty.to_string());

                    let mut frame = Vec::new();
                    serde_ipld_dagcbor::to_writer(&mut frame, &hdr).unwrap();
                    serde_ipld_dagcbor::to_writer(&mut frame, &msg).unwrap();

                    for client in &mut clients {
                        let _ = client.send(Message::binary(frame.clone())).await;
                    }
                }
                FirehoseMessage::Connect(ws) => {
                    clients.push(ws);
                }
            }
        }
    });

    (handle, FirehoseProducer { tx })
}
