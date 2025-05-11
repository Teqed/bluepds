// src/actor_store/preference/transactor.rs
use anyhow::{Context as _, Result, bail};

use super::super::ActorDb;

use super::reader::{AccountPreference, PreferenceReader, pref_in_scope, pref_match_namespace};

/// Transactor for preference operations.
pub(crate) struct PreferenceTransactor {
    /// Preference reader.
    pub reader: PreferenceReader,
}

impl PreferenceTransactor {
    /// Create a new preference transactor.
    pub(crate) fn new(db: ActorDb) -> Self {
        Self {
            reader: PreferenceReader::new(db),
        }
    }

    /// Put preferences for a namespace.
    pub(crate) async fn put_preferences(
        &self,
        values: Vec<AccountPreference>,
        namespace: &str,
        scope: &str,
    ) -> Result<()> {
        // Validate all preferences match the namespace
        if !values
            .iter()
            .all(|value| pref_match_namespace(namespace, &value.r#type))
        {
            bail!("Some preferences are not in the {} namespace", namespace);
        }

        // Validate scope permissions
        let not_in_scope = values
            .iter()
            .filter(|val| !pref_in_scope(scope, &val.r#type))
            .collect::<Vec<_>>();

        if !not_in_scope.is_empty() {
            bail!("Do not have authorization to set preferences");
        }

        // Get current preferences
        let mut tx = self
            .reader
            .db
            .pool
            .begin()
            .await
            .context("failed to begin transaction")?;

        // Find all preferences in the namespace
        let namespace_pattern = format!("{}%", namespace);
        let all_prefs = sqlx::query!(
            "SELECT id, name FROM account_pref WHERE name LIKE ? OR name = ?",
            namespace_pattern,
            namespace
        )
        .fetch_all(&mut *tx)
        .await
        .context("failed to fetch preferences")?;

        // Filter to those in scope
        let all_pref_ids_in_namespace = all_prefs
            .iter()
            .filter(|pref| pref_match_namespace(namespace, &pref.name))
            .filter(|pref| pref_in_scope(scope, &pref.name))
            .map(|pref| pref.id)
            .collect::<Vec<i64>>();

        // Delete existing preferences in namespace
        if !all_pref_ids_in_namespace.is_empty() {
            let placeholders = std::iter::repeat("?")
                .take(all_pref_ids_in_namespace.len())
                .collect::<Vec<_>>()
                .join(",");

            let query = format!("DELETE FROM account_pref WHERE id IN ({})", placeholders);

            let mut query_builder = sqlx::query(&query);
            for id in &all_pref_ids_in_namespace {
                query_builder = query_builder.bind(id);
            }

            query_builder
                .execute(&mut *tx)
                .await
                .context("failed to delete preferences")?;
        }

        // Insert new preferences
        if !values.is_empty() {
            for pref in values {
                let value_json = serde_json::to_string(&pref.value)?;
                sqlx::query!(
                    "INSERT INTO account_pref (name, valueJson) VALUES (?, ?)",
                    pref.r#type,
                    value_json
                )
                .execute(&mut *tx)
                .await
                .context("failed to insert preference")?;
            }
        }

        tx.commit().await.context("failed to commit transaction")?;

        Ok(())
    }
}
