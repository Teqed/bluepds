//! Reader for preference data in the actor store.

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;

/// Reader for preference data.
pub struct PreferenceReader {
    /// Database connection.
    pub db: SqlitePool,
    /// DID of the repository owner.
    pub did: String,
}

/// User preference with type information.
#[derive(Debug, Clone)]
pub struct AccountPreference {
    /// Type of the preference.
    pub r#type: String,
    /// Preference data as JSON.
    pub value: serde_json::Value,
}

impl PreferenceReader {
    /// Create a new preference reader.
    pub fn new(db: SqlitePool, did: String) -> Self {
        Self { db, did }
    }

    /// Get preferences for a namespace.
    pub async fn get_preferences(
        &self,
        namespace: &str,
        scope: &str,
    ) -> Result<Vec<AccountPreference>> {
        // TODO: Implement preference reader
        // For now, just return an empty list
        Ok(Vec::new())
    }
}

/// Check if a preference matches a namespace.
pub fn pref_match_namespace(namespace: &str, fullname: &str) -> bool {
    fullname == namespace || fullname.starts_with(&format!("{}.", namespace))
}

/// Check if a preference is in scope.
pub fn pref_in_scope(scope: &str, pref_type: &str) -> bool {
    scope == "access" || !FULL_ACCESS_ONLY_PREFS.contains(&pref_type)
}

/// Preferences that require full access.
pub const FULL_ACCESS_ONLY_PREFS: &[&str] = &["app.bsky.actor.defs#personalDetailsPref"];
