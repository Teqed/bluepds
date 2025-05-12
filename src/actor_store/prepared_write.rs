use rsky_repo::types::{
    CommitAction, PreparedBlobRef, PreparedCreateOrUpdate, PreparedDelete, WriteOpAction,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum PreparedWrite {
    Create(PreparedCreateOrUpdate),
    Update(PreparedCreateOrUpdate),
    Delete(PreparedDelete),
}

impl PreparedWrite {
    pub fn uri(&self) -> &String {
        match self {
            PreparedWrite::Create(w) => &w.uri,
            PreparedWrite::Update(w) => &w.uri,
            PreparedWrite::Delete(w) => &w.uri,
        }
    }

    pub fn cid(&self) -> Option<Cid> {
        match self {
            PreparedWrite::Create(w) => Some(w.cid),
            PreparedWrite::Update(w) => Some(w.cid),
            PreparedWrite::Delete(_) => None,
        }
    }

    pub fn swap_cid(&self) -> &Option<Cid> {
        match self {
            PreparedWrite::Create(w) => &w.swap_cid,
            PreparedWrite::Update(w) => &w.swap_cid,
            PreparedWrite::Delete(w) => &w.swap_cid,
        }
    }

    pub fn action(&self) -> &WriteOpAction {
        match self {
            PreparedWrite::Create(w) => &w.action,
            PreparedWrite::Update(w) => &w.action,
            PreparedWrite::Delete(w) => &w.action,
        }
    }

    /// TEQ: Add blobs() impl
    pub fn blobs(&self) -> Option<&Vec<PreparedBlobRef>> {
        match self {
            PreparedWrite::Create(w) => Some(&w.blobs),
            PreparedWrite::Update(w) => Some(&w.blobs),
            PreparedWrite::Delete(_) => None,
        }
    }
}

impl From<&PreparedWrite> for CommitAction {
    fn from(value: &PreparedWrite) -> Self {
        match value {
            &PreparedWrite::Create(_) => CommitAction::Create,
            &PreparedWrite::Update(_) => CommitAction::Update,
            &PreparedWrite::Delete(_) => CommitAction::Delete,
        }
    }
}
