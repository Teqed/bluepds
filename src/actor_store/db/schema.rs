//! Database schema definitions for the actor store.

/// Repository root information
#[derive(Debug, Clone)]
pub struct RepoRoot {
    pub did: String,
    pub cid: String,
    pub rev: String,
    pub indexed_at: String,
}

pub const REPO_ROOT_TABLE: &str = "repo_root";

/// Repository block (IPLD block)
#[derive(Debug, Clone)]
pub struct RepoBlock {
    pub cid: String,
    pub repo_rev: String,
    pub size: i64,
    pub content: Vec<u8>,
}

pub const REPO_BLOCK_TABLE: &str = "repo_block";

/// Record information
#[derive(Debug, Clone)]
pub struct Record {
    pub uri: String,
    pub cid: String,
    pub collection: String,
    pub rkey: String,
    pub repo_rev: String,
    pub indexed_at: String,
    pub takedown_ref: Option<String>,
}

pub const RECORD_TABLE: &str = "record";

/// Blob information
#[derive(Debug, Clone)]
pub struct Blob {
    pub cid: String,
    pub mime_type: String,
    pub size: i64,
    pub temp_key: Option<String>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub created_at: String,
    pub takedown_ref: Option<String>,
}

pub const BLOB_TABLE: &str = "blob";

/// Record-blob association
#[derive(Debug, Clone)]
pub struct RecordBlob {
    pub blob_cid: String,
    pub record_uri: String,
}

pub const RECORD_BLOB_TABLE: &str = "record_blob";

/// Backlink between records
#[derive(Debug, Clone)]
pub struct Backlink {
    pub uri: String,
    pub path: String,
    pub link_to: String,
}

pub const BACKLINK_TABLE: &str = "backlink";

/// Account preference
#[derive(Debug, Clone)]
pub struct AccountPref {
    pub id: i64,
    pub name: String,
    pub value_json: String,
}

pub const ACCOUNT_PREF_TABLE: &str = "account_pref";

/// Database schema for the reference type system
pub type DatabaseSchema = PartialRepoRoot &
    PartialRepoBlock &
    PartialRecord &
    PartialBlob &
    PartialRecordBlob &
    PartialBacklink &
    PartialAccountPref;

/// Type alias for RepoRoot partial database
pub type PartialRepoRoot = { [REPO_ROOT_TABLE]: RepoRoot };
/// Type alias for RepoBlock partial database
pub type PartialRepoBlock = { [REPO_BLOCK_TABLE]: RepoBlock };
/// Type alias for Record partial database
pub type PartialRecord = { [RECORD_TABLE]: Record };
/// Type alias for Blob partial database
pub type PartialBlob = { [BLOB_TABLE]: Blob };
/// Type alias for RecordBlob partial database
pub type PartialRecordBlob = { [RECORD_BLOB_TABLE]: RecordBlob };
/// Type alias for Backlink partial database
pub type PartialBacklink = { [BACKLINK_TABLE]: Backlink };
/// Type alias for AccountPref partial database
pub type PartialAccountPref = { [ACCOUNT_PREF_TABLE]: AccountPref };

/// Migrator placeholder - implementation in migrations module
pub struct Migrator;
