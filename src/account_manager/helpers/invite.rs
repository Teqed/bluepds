//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/account_manager/helpers/invite.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend
use anyhow::{Result, bail};
use diesel::*;
use rsky_lexicon::com::atproto::server::AccountCodes;
use rsky_lexicon::com::atproto::server::{
    InviteCode as LexiconInviteCode, InviteCodeUse as LexiconInviteCodeUse,
};
use rsky_pds::account_manager::DisableInviteCodesOpts;
use rsky_pds::models::models;
use std::collections::BTreeMap;
use std::mem;

pub type CodeUse = LexiconInviteCodeUse;
pub type CodeDetail = LexiconInviteCode;

pub async fn ensure_invite_is_available(
    invite_code: String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::actor::dsl as ActorSchema;
    use rsky_pds::schema::pds::invite_code::dsl as InviteCodeSchema;
    use rsky_pds::schema::pds::invite_code_use::dsl as InviteCodeUseSchema;

    db.get().await?.interact(move |conn| {
        let invite: Option<models::InviteCode> = InviteCodeSchema::invite_code
            .left_join(
                ActorSchema::actor.on(InviteCodeSchema::forAccount
                    .eq(ActorSchema::did)
                    .and(ActorSchema::takedownRef.is_null())),
            )
            .filter(InviteCodeSchema::code.eq(&invite_code))
            .select(models::InviteCode::as_select())
            .first(conn)
            .optional()?;

        if let Some(invite) = invite {
            if invite.disabled > 0 {
                bail!("InvalidInviteCode: Disabled. Provided invite code not available `{invite_code:?}`");
            }

            let uses: i64 = InviteCodeUseSchema::invite_code_use
                .count()
                .filter(InviteCodeUseSchema::code.eq(&invite_code))
                .first(conn)?;

            if invite.available_uses as i64 <= uses {
                bail!("InvalidInviteCode: Not enough uses. Provided invite code not available `{invite_code:?}`");
            }
        } else {
            bail!("InvalidInviteCode: None. Provided invite code not available `{invite_code:?}`");
        }

        Ok(())
    }).await.expect("Failed to check invite code availability")?;

    Ok(())
}

pub async fn record_invite_use(
    did: String,
    invite_code: Option<String>,
    now: String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    if let Some(invite_code) = invite_code {
        use rsky_pds::schema::pds::invite_code_use::dsl as InviteCodeUseSchema;

        _ = db
            .get()
            .await?
            .interact(move |conn| {
                insert_into(InviteCodeUseSchema::invite_code_use)
                    .values((
                        InviteCodeUseSchema::code.eq(invite_code),
                        InviteCodeUseSchema::usedBy.eq(did),
                        InviteCodeUseSchema::usedAt.eq(now),
                    ))
                    .execute(conn)
            })
            .await
            .expect("Failed to record invite code use")?;
    }
    Ok(())
}

pub async fn create_invite_codes(
    to_create: Vec<AccountCodes>,
    use_count: i32,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::invite_code::dsl as InviteCodeSchema;
    let created_at = rsky_common::now();

    _ = db
        .get()
        .await?
        .interact(move |conn| {
            let rows: Vec<models::InviteCode> = to_create
                .into_iter()
                .flat_map(|account| {
                    let for_account = account.account;
                    account
                        .codes
                        .iter()
                        .map(|code| models::InviteCode {
                            code: code.clone(),
                            available_uses: use_count,
                            disabled: 0,
                            for_account: for_account.clone(),
                            created_by: "admin".to_owned(),
                            created_at: created_at.clone(),
                        })
                        .collect::<Vec<models::InviteCode>>()
                })
                .collect();
            insert_into(InviteCodeSchema::invite_code)
                .values(&rows)
                .execute(conn)
        })
        .await
        .expect("Failed to create invite codes")?;
    Ok(())
}

pub async fn create_account_invite_codes(
    for_account: &str,
    codes: Vec<String>,
    expected_total: usize,
    disabled: bool,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Vec<CodeDetail>> {
    use rsky_pds::schema::pds::invite_code::dsl as InviteCodeSchema;

    let for_account = for_account.to_owned();
    let rows = db
        .get()
        .await?
        .interact(move |conn| {
            let now = rsky_common::now();

            let rows: Vec<models::InviteCode> = codes
                .into_iter()
                .map(|code| models::InviteCode {
                    code,
                    available_uses: 1,
                    disabled: if disabled { 1 } else { 0 },
                    for_account: for_account.clone(),
                    created_by: for_account.clone(),
                    created_at: now.clone(),
                })
                .collect();

            _ = insert_into(InviteCodeSchema::invite_code)
                .values(&rows)
                .execute(conn)?;

            let final_routine_invite_codes: Vec<models::InviteCode> = InviteCodeSchema::invite_code
                .filter(InviteCodeSchema::forAccount.eq(for_account))
                .filter(InviteCodeSchema::createdBy.ne("admin")) // don't count admin-gifted codes against the user
                .select(models::InviteCode::as_select())
                .get_results(conn)?;

            if final_routine_invite_codes.len() > expected_total {
                bail!("DuplicateCreate: attempted to create additional codes in another request")
            }

            Ok(rows.into_iter().map(|row| CodeDetail {
                code: row.code,
                available: 1,
                disabled: row.disabled == 1,
                for_account: row.for_account,
                created_by: row.created_by,
                created_at: row.created_at,
                uses: Vec::new(),
            }))
        })
        .await
        .expect("Failed to create account invite codes")?;
    Ok(rows.collect())
}

pub async fn get_account_invite_codes(
    did: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Vec<CodeDetail>> {
    use rsky_pds::schema::pds::invite_code::dsl as InviteCodeSchema;

    let did = did.to_owned();
    let res: Vec<models::InviteCode> = db
        .get()
        .await?
        .interact(move |conn| {
            InviteCodeSchema::invite_code
                .filter(InviteCodeSchema::forAccount.eq(did))
                .select(models::InviteCode::as_select())
                .get_results(conn)
        })
        .await
        .expect("Failed to get account invite codes")?;

    let codes: Vec<String> = res.iter().map(|row| row.code.clone()).collect();
    let mut uses = get_invite_codes_uses_v2(codes, db).await?;
    Ok(res
        .into_iter()
        .map(|row| CodeDetail {
            code: row.code.clone(),
            available: row.available_uses,
            disabled: row.disabled == 1,
            for_account: row.for_account,
            created_by: row.created_by,
            created_at: row.created_at,
            uses: mem::take(uses.get_mut(&row.code).unwrap_or(&mut Vec::new())),
        })
        .collect::<Vec<CodeDetail>>())
}

pub async fn get_invite_codes_uses_v2(
    codes: Vec<String>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<BTreeMap<String, Vec<CodeUse>>> {
    use rsky_pds::schema::pds::invite_code_use::dsl as InviteCodeUseSchema;

    let mut uses: BTreeMap<String, Vec<CodeUse>> = BTreeMap::new();
    if !codes.is_empty() {
        let uses_res: Vec<models::InviteCodeUse> = db
            .get()
            .await?
            .interact(|conn| {
                InviteCodeUseSchema::invite_code_use
                    .filter(InviteCodeUseSchema::code.eq_any(codes))
                    .order_by(InviteCodeUseSchema::usedAt.desc())
                    .select(models::InviteCodeUse::as_select())
                    .get_results(conn)
            })
            .await
            .expect("Failed to get invite code uses")?;
        for invite_code_use in uses_res {
            let models::InviteCodeUse {
                code,
                used_by,
                used_at,
            } = invite_code_use;
            match uses.get_mut(&code) {
                None => {
                    drop(uses.insert(code, vec![CodeUse { used_by, used_at }]));
                }
                Some(matched_uses) => matched_uses.push(CodeUse { used_by, used_at }),
            };
        }
    }
    Ok(uses)
}

pub async fn get_invited_by_for_accounts(
    dids: Vec<String>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<BTreeMap<String, CodeDetail>> {
    if dids.is_empty() {
        return Ok(BTreeMap::new());
    }
    use rsky_pds::schema::pds::invite_code::dsl as InviteCodeSchema;
    use rsky_pds::schema::pds::invite_code_use::dsl as InviteCodeUseSchema;

    let dids = dids.clone();
    let res: Vec<models::InviteCode> = db
        .get()
        .await?
        .interact(|conn| {
            InviteCodeSchema::invite_code
                .filter(
                    InviteCodeSchema::forAccount.eq_any(
                        InviteCodeUseSchema::invite_code_use
                            .filter(InviteCodeUseSchema::usedBy.eq_any(dids))
                            .select(InviteCodeUseSchema::code)
                            .distinct(),
                    ),
                )
                .select(models::InviteCode::as_select())
                .get_results(conn)
        })
        .await
        .expect("Failed to get account invite codes")?;
    let codes: Vec<String> = res.iter().map(|row| row.code.clone()).collect();
    let mut uses = get_invite_codes_uses_v2(codes, db).await?;

    let code_details = res
        .into_iter()
        .map(|row| CodeDetail {
            code: row.code.clone(),
            available: row.available_uses,
            disabled: row.disabled == 1,
            for_account: row.for_account,
            created_by: row.created_by,
            created_at: row.created_at,
            uses: mem::take(uses.get_mut(&row.code).unwrap_or(&mut Vec::new())),
        })
        .collect::<Vec<CodeDetail>>();

    Ok(code_details.iter().fold(
        BTreeMap::new(),
        |mut acc: BTreeMap<String, CodeDetail>, cur| {
            for code_use in &cur.uses {
                drop(acc.insert(code_use.used_by.clone(), cur.clone()));
            }
            acc
        },
    ))
}

pub async fn set_account_invites_disabled(
    did: &str,
    disabled: bool,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::account::dsl as AccountSchema;

    let disabled: i16 = if disabled { 1 } else { 0 };
    let did = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            update(AccountSchema::account)
                .filter(AccountSchema::did.eq(did))
                .set((AccountSchema::invitesDisabled.eq(disabled),))
                .execute(conn)
        })
        .await
        .expect("Failed to set account invites disabled")?;
    Ok(())
}

pub async fn disable_invite_codes(
    opts: DisableInviteCodesOpts,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::invite_code::dsl as InviteCodeSchema;

    let DisableInviteCodesOpts { codes, accounts } = opts;
    if !codes.is_empty() {
        _ = db
            .get()
            .await?
            .interact(move |conn| {
                update(InviteCodeSchema::invite_code)
                    .filter(InviteCodeSchema::code.eq_any(&codes))
                    .set((InviteCodeSchema::disabled.eq(1),))
                    .execute(conn)
            })
            .await
            .expect("Failed to disable invite codes")?;
    }
    if !accounts.is_empty() {
        _ = db
            .get()
            .await?
            .interact(move |conn| {
                update(InviteCodeSchema::invite_code)
                    .filter(InviteCodeSchema::forAccount.eq_any(&accounts))
                    .set((InviteCodeSchema::disabled.eq(1),))
                    .execute(conn)
            })
            .await
            .expect("Failed to disable invite codes")?;
    }
    Ok(())
}
