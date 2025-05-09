//! Reader for preference data in the actor store.

use anyhow::Result;
use sqlx::SqlitePool;

/// Reader for preference data.
pub(crate) struct PreferenceReader {
    /// Database connection.
    pub db: SqlitePool,
    /// DID of the repository owner.
    pub did: String,
}

/// User preference with type information.
#[derive(Debug, Clone)]
pub(crate) struct AccountPreference {
    /// Type of the preference.
    pub r#type: String,
    /// Preference data as JSON.
    pub value: serde_json::Value,
}

impl PreferenceReader {
    /// Create a new preference reader.
    pub(crate) fn new(db: SqlitePool, did: String) -> Self {
        Self { db, did }
    }

    /// Get preferences for a namespace.
    pub(crate) async fn get_preferences(
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
pub(super) fn pref_match_namespace(namespace: &str, fullname: &str) -> bool {
    fullname == namespace || fullname.starts_with(&format!("{}.", namespace))
}

/// Check if a preference is in scope.
pub(super) fn pref_in_scope(scope: &str, pref_type: &str) -> bool {
    scope == "access" || !FULL_ACCESS_ONLY_PREFS.contains(&pref_type)
}

/// Preferences that require full access.
pub(super) const FULL_ACCESS_ONLY_PREFS: &[&str] = &["app.bsky.actor.defs#personalDetailsPref"];
