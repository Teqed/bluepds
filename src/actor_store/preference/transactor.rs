//! Transactor for preference operations.

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;

use super::reader::{AccountPreference, PreferenceReader, pref_in_scope, pref_match_namespace};

/// Transactor for preference operations.
pub struct PreferenceTransactor {
    /// Preference reader.
    pub reader: PreferenceReader,
}
