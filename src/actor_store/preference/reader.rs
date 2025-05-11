//! Reader for preference data in the actor store.

use anyhow::Result;

use super::super::ActorDb;

/// Reader for preference data.
pub(crate) struct PreferenceReader {
    /// Database connection.
    pub db: ActorDb,
}

/// User preference with type information.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct AccountPreference {
    /// Type of the preference.
    pub r#type: String,
    /// Preference data as JSON.
    pub value: serde_json::Value,
}

impl PreferenceReader {
    /// Create a new preference reader.
    pub(crate) fn new(db: ActorDb) -> Self {
        Self { db }
    }

    /// Get preferences for a namespace.
    pub(crate) async fn get_preferences(
        &self,
        namespace: Option<&str>,
        scope: &str,
    ) -> Result<Vec<AccountPreference>> {
        let prefs_res = sqlx::query!("SELECT * FROM account_pref ORDER BY id")
            .fetch_all(&self.db.db)
            .await?;

        let prefs = prefs_res
            .into_iter()
            .filter(|pref| {
                namespace.map_or(true, |ns| pref_match_namespace(ns, &pref.name))
                    && pref_in_scope(scope, &pref.name)
            })
            .map(|pref| serde_json::from_str(&pref.valueJson).unwrap())
            .collect();

        Ok(prefs)
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
