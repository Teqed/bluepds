//! Preference handling for actor store.

use anyhow::{Context as _, Result, bail};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::actor_store::db::ActorDb;

/// Constants for preference-related operations
const FULL_ACCESS_ONLY_PREFS: &[&str] = &["app.bsky.actor.defs#personalDetailsPref"];

/// User preference with type information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AccountPreference {
    /// Type of the preference.
    pub r#type: String,
    /// Preference data as JSON.
    pub value: JsonValue,
}

/// Handler for preference operations with both read and write capabilities.
pub(crate) struct PreferenceHandler {
    /// Database connection.
    pub db: ActorDb,
    /// DID of the actor.
    pub did: String,
}

impl PreferenceHandler {
    /// Create a new preference handler.
    pub(crate) fn new(db: ActorDb, did: String) -> Self {
        Self { db, did }
    }

    /// Get preferences for a namespace.
    pub(crate) async fn get_preferences(
        &self,
        namespace: Option<&str>,
        scope: &str,
    ) -> Result<Vec<AccountPreference>> {
        use rsky_pds::schema::pds::account_pref::dsl::*;

        let did = self.did.clone();
        let namespace_clone = namespace.map(|ns| ns.to_string());
        let scope_clone = scope.to_string();

        let prefs_res = self
            .db
            .run(move |conn| {
                let prefs = account_pref
                    .filter(did.eq(&did))
                    .order(id.asc())
                    .load::<rsky_pds::models::AccountPref>(conn)
                    .context("Failed to fetch preferences")?;

                Ok::<Vec<rsky_pds::models::AccountPref>, diesel::result::Error>(prefs)
            })
            .await?;

        // Filter preferences based on namespace and scope
        let filtered_prefs = prefs_res
            .into_iter()
            .filter(|pref| {
                namespace_clone
                    .as_ref()
                    .map_or(true, |ns| pref_match_namespace(ns, &pref.name))
            })
            .filter(|pref| pref_in_scope(scope, &pref.name))
            .map(|pref| -> Result<AccountPreference> {
                let value_json = match pref.value_json {
                    Some(json) => serde_json::from_str(&json)
                        .context(format!("Failed to parse preference JSON for {}", pref.name))?,
                    None => bail!("Preference JSON is null for {}", pref.name),
                };

                Ok(AccountPreference {
                    r#type: pref.name,
                    value: value_json,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(filtered_prefs)
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

        let did = self.did.clone();
        let namespace_str = namespace.to_string();
        let scope_str = scope.to_string();

        // Convert preferences to serialized form
        let serialized_prefs = values
            .into_iter()
            .map(|pref| -> Result<(String, String)> {
                let json = serde_json::to_string(&pref.value)
                    .context("Failed to serialize preference value")?;
                Ok((pref.r#type, json))
            })
            .collect::<Result<Vec<_>>>()?;

        // Execute transaction
        self.db
            .transaction(move |conn| {
                use rsky_pds::schema::pds::account_pref::dsl::*;

                // Find all preferences in the namespace
                let namespace_pattern = format!("{}%", namespace_str);
                let all_prefs = account_pref
                    .filter(did.eq(&did))
                    .filter(name.eq(&namespace_str).or(name.like(&namespace_pattern)))
                    .load::<rsky_pds::models::AccountPref>(conn)
                    .context("Failed to fetch preferences")?;

                // Filter to those in scope
                let all_pref_ids_in_namespace = all_prefs
                    .iter()
                    .filter(|pref| pref_match_namespace(&namespace_str, &pref.name))
                    .filter(|pref| pref_in_scope(&scope_str, &pref.name))
                    .map(|pref| pref.id)
                    .collect::<Vec<i32>>();

                // Delete existing preferences in namespace
                if !all_pref_ids_in_namespace.is_empty() {
                    diesel::delete(account_pref)
                        .filter(id.eq_any(all_pref_ids_in_namespace))
                        .execute(conn)
                        .context("Failed to delete existing preferences")?;
                }

                // Insert new preferences
                if !serialized_prefs.is_empty() {
                    for (pref_type, pref_json) in serialized_prefs {
                        diesel::insert_into(account_pref)
                            .values((
                                did.eq(&did),
                                name.eq(&pref_type),
                                valueJson.eq(Some(&pref_json)),
                            ))
                            .execute(conn)
                            .context("Failed to insert preference")?;
                    }
                }

                Ok(())
            })
            .await
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
