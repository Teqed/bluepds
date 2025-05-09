//! Database schema definitions for the actor store.

/// Repository root information
#[derive(Debug, Clone)]
pub(crate) struct RepoRoot {
    pub(crate) did: String,
    pub(crate) cid: String,
    pub(crate) rev: String,
    pub(crate) indexed_at: String,
}

pub(crate) const REPO_ROOT_TABLE: &str = "repo_root";

/// Repository block (IPLD block)
#[derive(Debug, Clone)]
pub(crate) struct RepoBlock {
    pub(crate) cid: String,
    pub(crate) repo_rev: String,
    pub(crate) size: i64,
    pub(crate) content: Vec<u8>,
}

pub(crate) const REPO_BLOCK_TABLE: &str = "repo_block";

/// Record information
#[derive(Debug, Clone)]
pub(crate) struct Record {
    pub(crate) uri: String,
    pub(crate) cid: String,
    pub(crate) collection: String,
    pub(crate) rkey: String,
    pub(crate) repo_rev: String,
    pub(crate) indexed_at: String,
    pub(crate) takedown_ref: Option<String>,
}

pub(crate) const RECORD_TABLE: &str = "record";

/// Blob information
#[derive(Debug, Clone)]
pub(crate) struct Blob {
    pub(crate) cid: String,
    pub(crate) mime_type: String,
    pub(crate) size: i64,
    pub(crate) temp_key: Option<String>,
    pub(crate) width: Option<i64>,
    pub(crate) height: Option<i64>,
    pub(crate) created_at: String,
    pub(crate) takedown_ref: Option<String>,
}

pub(crate) const BLOB_TABLE: &str = "blob";

/// Record-blob association
#[derive(Debug, Clone)]
pub(crate) struct RecordBlob {
    pub(crate) blob_cid: String,
    pub(crate) record_uri: String,
}

pub(crate) const RECORD_BLOB_TABLE: &str = "record_blob";

/// Backlink between records
#[derive(Debug, Clone)]
pub(crate) struct Backlink {
    pub(crate) uri: String,
    pub(crate) path: String,
    pub(crate) link_to: String,
}

pub(crate) const BACKLINK_TABLE: &str = "backlink";

/// Account preference
#[derive(Debug, Clone)]
pub(crate) struct AccountPref {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) value_json: String,
}

pub(crate) const ACCOUNT_PREF_TABLE: &str = "account_pref";

pub(crate) type DatabaseSchema = sqlx::Sqlite;
