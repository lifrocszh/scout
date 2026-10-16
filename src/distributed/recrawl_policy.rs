//! Persisted approval boundary for optional Source recrawl adjustments.
//!
//! Import validation and human decisions live here. The coordinator can consume
//! `ScheduleAdjustment` later without giving recommendation data direct access
//! to crawl request limits, robots rules, or the task frontier.

use crate::crawl::Config;
use crate::error::Error;
use crate::storage::sha256_hex;
use chrono::{DateTime, Duration, Utc};
use postgres::{Client, NoTls, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;

mod recommendation;
use recommendation::{
    extraction_contract_sha256, parse_idempotency_basis, parse_recommendation_utc, parse_utc,
    source_scope_sha256, validate_against_policy, validate_recommendation,
};
pub(crate) struct RecrawlPolicyStore {
    client: Client,
}

#[derive(Debug, Clone)]
pub(crate) struct SourcePolicyConfig {
    pub(crate) policy_version: String,
    pub(crate) current_interval_hours: f64,
    pub(crate) minimum_interval_hours: f64,
    pub(crate) maximum_interval_hours: f64,
    pub(crate) max_routine_interval_decrease_fraction: f64,
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScheduleAdjustment {
    pub(crate) recommendation_id: String,
    pub(crate) source_id: String,
    pub(crate) interval_hours: f64,
    pub(crate) scope_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ImportResult {
    pub(crate) recommendation_id: Option<String>,
    pub(crate) validation_status: String,
    pub(crate) outcome: String,
    pub(crate) duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DecisionResult {
    pub(crate) outcome: String,
    pub(crate) applied_policy_version: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Recommendation {
    schema_version: String,
    recommendation_id: String,
    idempotency_key: String,
    adjustment_type: String,
    idempotency_basis: String,
    generation_id: String,
    source_id: String,
    generated_at_utc: String,
    expires_at_utc: String,
    expected_policy_version: String,
    interval_hours: IntervalHours,
    evidence: RecommendationEvidence,
    confidence: String,
    reasons: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntervalHours {
    minimum: f64,
    current: f64,
    maximum: f64,
    proposed: Option<f64>,
    adjustment: f64,
    #[serde(default)]
    adjustment_fraction: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecommendationEvidence {
    status: String,
    #[serde(default)]
    snapshot_refs: Vec<SnapshotReference>,
    #[serde(default)]
    provisional_run_id: Option<String>,
    #[serde(default)]
    provisional_event_ids: Vec<String>,
    scope_sha256: Option<String>,
    extraction_contract_sha256: Option<String>,
    #[serde(default)]
    latest_observed_at_utc: Option<String>,
    #[serde(default)]
    latest_capture_event_at_utc: Option<String>,
    #[serde(default)]
    latest_run_event_at_utc: Option<String>,
    #[serde(default)]
    comparable_page_observations: Option<i64>,
    #[serde(default)]
    changed_page_observations: Option<i64>,
    #[serde(default)]
    change_fraction: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecommendationBasis {
    schema_version: String,
    generation_id: String,
    source_id: String,
    generated_at_utc: String,
    expires_at_utc: String,
    expected_policy_version: String,
    interval_hours: BasisInterval,
    recommendation_settings: RecommendationSettings,
    evidence: BasisEvidence,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BasisInterval {
    minimum: String,
    current: String,
    maximum: String,
    proposed: String,
    adjustment: String,
    adjustment_fraction: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecommendationSettings {
    max_adjustment_fraction: String,
    minimum_completed_snapshots: String,
    minimum_comparable_page_observations: String,
    maximum_evidence_age_days: String,
    maximum_provisional_event_age_seconds: String,
    change_fraction_decrease_threshold: String,
    change_fraction_increase_threshold: String,
    decrease_target_multiplier: String,
    increase_target_multiplier: String,
    high_confidence_minimum_snapshots: String,
    high_confidence_minimum_comparable_page_observations: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BasisSnapshotReference {
    snapshot_id: String,
    source_snapshot_manifest_sha256: String,
    crawl_manifest_sha256: String,
    observed_at_utc: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BasisEvidence {
    status: String,
    snapshot_ids: String,
    snapshot_refs: Vec<BasisSnapshotReference>,
    provisional_run_id: String,
    provisional_event_ids: String,
    scope_sha256: String,
    extraction_contract_sha256: String,
    latest_observed_at_utc: String,
    latest_capture_event_at_utc: String,
    latest_run_event_at_utc: String,
    comparable_page_observations: String,
    changed_page_observations: String,
    change_fraction: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotReference {
    snapshot_id: String,
    source_snapshot_manifest_sha256: String,
    crawl_manifest_sha256: String,
    observed_at_utc: String,
}

#[derive(Debug, Clone)]
struct Validation {
    status: &'static str,
    reasons: Vec<String>,
    generated_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct StoredPolicy {
    scope_sha256: String,
    policy_version: String,
    current_interval_hours: f64,
    minimum_interval_hours: f64,
    maximum_interval_hours: f64,
    max_routine_interval_decrease_fraction: f64,
    enabled: bool,
}

impl RecrawlPolicyStore {
    pub(crate) fn connect(database_url: &str) -> Result<Self, Error> {
        let client = Client::connect(database_url, NoTls).map_err(|error| {
            Error::storage(format!("connect recrawl policy PostgreSQL: {error}"))
        })?;
        Ok(Self { client })
    }

    pub(crate) fn reject_scheduled_run_resume(&mut self, run_id: &str) -> Result<(), Error> {
        let has_scheduled_state: bool = self
            .client
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM scout_scheduled_run_sources WHERE crawl_run_id=$1
                     UNION ALL
                     SELECT 1 FROM scout_source_recrawl_policy WHERE claimed_run_id=$1
                 )",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("check scheduled run ownership: {error}")))?
            .get(0);
        if has_scheduled_state {
            return Err(Error::invalid(
                "scheduled crawl run cannot be resumed in manual mode",
            ));
        }
        Ok(())
    }

    pub(crate) fn completed_source_ids(&mut self, run_id: &str) -> Result<Vec<String>, Error> {
        self.client
            .query(
                "SELECT source_id FROM scout_scheduled_run_sources
                 WHERE crawl_run_id=$1 AND work_completed ORDER BY source_id",
                &[&run_id],
            )
            .map(|rows| rows.into_iter().map(|row| row.get(0)).collect())
            .map_err(|error| Error::storage(format!("read completed scheduled Sources: {error}")))
    }

    /// The global and per-Source switches are both off until an operator enables them.
    pub(crate) fn set_enabled(&mut self, enabled: bool, actor: &str) -> Result<(), Error> {
        validate_actor(actor)?;
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin recrawl switch update: {error}")))?;
        let before: bool = tx
            .query_one(
                "SELECT enabled FROM scout_recrawl_control WHERE singleton=TRUE FOR UPDATE",
                &[],
            )
            .map_err(|error| Error::storage(format!("read recrawl switch: {error}")))?
            .get(0);
        tx.execute(
            "UPDATE scout_recrawl_control SET enabled=$1,updated_by=$2,updated_at=clock_timestamp() WHERE singleton=TRUE",
            &[&enabled, &actor],
        ).map_err(|error| Error::storage(format!("update recrawl policy switch: {error}")))?;
        tx.execute(
            "INSERT INTO scout_recrawl_policy_config_audit(action,actor,reason,enabled_before,enabled_after)
             VALUES ($1,$2,$3,$4,$5)",
            &[&if enabled { "global_enabled" } else { "global_disabled" }, &actor,
              &format!("global recrawl policy switch changed by {actor}"), &before, &enabled],
        ).map_err(|error| Error::storage(format!("audit recrawl switch: {error}")))?;
        tx.commit()
            .map_err(|error| Error::storage(format!("commit recrawl switch update: {error}")))
    }

    /// Persist the exact Source allowlist that the coordinator will seed.
    pub(crate) fn record_scheduled_sources(
        &mut self,
        config: &Config,
        run_id: &str,
        source_ids: &HashSet<String>,
    ) -> Result<(), Error> {
        if source_ids.is_empty() {
            return Err(Error::invalid(
                "scheduled run must select at least one Source",
            ));
        }
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin scheduled Source scope persistence: {error}"))
        })?;
        for source in &config.sources {
            if !source_ids.contains(&source.source_id) {
                continue;
            }
            let scope = source_scope_sha256(source)?;
            tx.execute(
                "INSERT INTO scout_scheduled_run_sources(
                     crawl_run_id,source_id,scope_sha256,interval_hours
                 ) SELECT $1,$2,$3,policy.current_interval_hours
                   FROM scout_source_recrawl_policy AS policy
                   WHERE policy.source_id=$2 AND policy.scope_sha256=$3
                     AND policy.enabled AND policy.claimed_run_id=$1
                 ON CONFLICT (crawl_run_id,source_id) DO NOTHING",
                &[&run_id, &source.source_id, &scope],
            )
            .map_err(|error| Error::storage(format!("persist scheduled Source scope: {error}")))?;
            let persisted = tx
                .query_opt(
                    "SELECT scope_sha256 FROM scout_scheduled_run_sources
                 WHERE crawl_run_id=$1 AND source_id=$2",
                    &[&run_id, &source.source_id],
                )
                .map_err(|error| Error::storage(format!("verify scheduled Source scope: {error}")))?
                .is_some_and(|row| row.get::<_, String>(0) == scope);
            if !persisted {
                return Err(Error::not_ready(format!(
                    "scheduled Source {:?} was not reserved under its approved scope",
                    source.source_id
                )));
            }
        }
        if source_ids
            .iter()
            .any(|id| !config.sources.iter().any(|source| &source.source_id == id))
        {
            return Err(Error::invalid(
                "scheduled Source set contains an ID outside the loaded configuration",
            ));
        }
        tx.commit()
            .map_err(|error| Error::storage(format!("commit scheduled Source scope: {error}")))?;
        Ok(())
    }

    /// Record operator-approved bounds against the Source's actual URL scope.
    pub(crate) fn configure_source(
        &mut self,
        config: &Config,
        source_id: &str,
        policy: &SourcePolicyConfig,
        actor: &str,
        reason: &str,
    ) -> Result<(), Error> {
        validate_actor(actor)?;
        validate_policy_config(policy)?;
        if reason.trim().is_empty() || reason.len() > 4000 {
            return Err(Error::invalid(
                "configuration reason must contain 1 to 4000 characters",
            ));
        }
        let source = config
            .sources
            .iter()
            .find(|source| source.source_id == source_id)
            .ok_or_else(|| {
                Error::invalid(format!(
                    "Source {source_id:?} is not in the loaded crawl config"
                ))
            })?;
        let scope = source_scope_sha256(source)?;
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin Source policy configuration: {error}"))
        })?;
        let previous = tx
            .query_opt(
                "SELECT scope_sha256,policy_version,current_interval_hours::float8,enabled,
                    claimed_run_id,run.status,minimum_interval_hours::float8,
                    maximum_interval_hours::float8,max_routine_interval_decrease_fraction::float8
             FROM scout_source_recrawl_policy AS policy
             LEFT JOIN scout_crawl_runs AS run ON run.crawl_run_id=policy.claimed_run_id
             WHERE source_id=$1 FOR UPDATE OF policy",
                &[&source_id],
            )
            .map_err(|error| Error::storage(format!("read prior Source policy: {error}")))?;
        if previous.as_ref().is_some_and(|row| {
            row.get::<_, Option<String>>(4).is_some()
                && row.get::<_, Option<String>>(5).as_deref() == Some("active")
        }) {
            return Err(Error::not_ready(
                "cannot reconfigure a Source while a scheduled Run is active",
            ));
        }
        let before_scope = previous.as_ref().map(|row| row.get::<_, String>(0));
        let before_version = previous.as_ref().map(|row| row.get::<_, String>(1));
        let before_interval = previous.as_ref().map(|row| row.get::<_, f64>(2));
        let before_enabled = previous.as_ref().map(|row| row.get::<_, bool>(3));
        let before_minimum = previous.as_ref().map(|row| row.get::<_, f64>(6));
        let before_maximum = previous.as_ref().map(|row| row.get::<_, f64>(7));
        let before_decrease_cap = previous.as_ref().map(|row| row.get::<_, f64>(8));
        let policy_terms_changed = before_scope.as_deref().is_some_and(|value| value != scope)
            || before_interval
                .is_some_and(|value| !same_hours(value, policy.current_interval_hours))
            || before_minimum
                .is_some_and(|value| !same_hours(value, policy.minimum_interval_hours))
            || before_maximum
                .is_some_and(|value| !same_hours(value, policy.maximum_interval_hours))
            || before_decrease_cap.is_some_and(|value| {
                !same_six_decimal(value, policy.max_routine_interval_decrease_fraction)
            });
        if before_version.as_deref() == Some(policy.policy_version.as_str()) && policy_terms_changed
        {
            return Err(Error::invalid(
                "policy_version must change when Source scope, interval bounds, or adjustment cap changes",
            ));
        }
        tx.execute(
                "INSERT INTO scout_source_recrawl_policy(
                     source_id, scope_sha256, policy_version, current_interval_hours,
                     minimum_interval_hours, maximum_interval_hours,
                     max_routine_interval_decrease_fraction, enabled, updated_by,
                     next_due_at, claimed_run_id, claimed_at
                 ) VALUES ($1,$2,$3,$4::float8::numeric,$5::float8::numeric,
                           $6::float8::numeric,$7::float8::numeric,$8,$9,
                           clock_timestamp(),NULL,NULL)
                 ON CONFLICT (source_id) DO UPDATE SET
                     scope_sha256=EXCLUDED.scope_sha256,
                     policy_version=EXCLUDED.policy_version,
                     current_interval_hours=EXCLUDED.current_interval_hours,
                     minimum_interval_hours=EXCLUDED.minimum_interval_hours,
                     maximum_interval_hours=EXCLUDED.maximum_interval_hours,
                     max_routine_interval_decrease_fraction=EXCLUDED.max_routine_interval_decrease_fraction,
                     enabled=EXCLUDED.enabled, updated_by=EXCLUDED.updated_by,
                     next_due_at=CASE WHEN scout_source_recrawl_policy.last_scheduled_at IS NULL
                         THEN clock_timestamp()
                         ELSE scout_source_recrawl_policy.last_scheduled_at +
                              (EXCLUDED.current_interval_hours::double precision * interval '1 hour') END,
                     claimed_run_id=NULL, claimed_at=NULL,
                     updated_at=clock_timestamp()",
                &[
                    &source_id,
                    &scope,
                    &policy.policy_version,
                    &policy.current_interval_hours,
                    &policy.minimum_interval_hours,
                    &policy.maximum_interval_hours,
                    &policy.max_routine_interval_decrease_fraction,
                    &policy.enabled,
                    &actor,
                ],
            ).map_err(|error| Error::storage(format!("persist Source recrawl policy: {error}")))?;
        let after_enabled = policy.enabled;
        tx.execute(
            "INSERT INTO scout_recrawl_policy_config_audit(
                 action,source_id,actor,reason,scope_sha256_before,scope_sha256_after,
                 policy_version_before,policy_version_after,interval_hours_before,
                 interval_hours_after,enabled_before,enabled_after,
                 minimum_interval_hours_before,minimum_interval_hours_after,
                 maximum_interval_hours_before,maximum_interval_hours_after,
                 max_routine_interval_decrease_fraction_before,
                 max_routine_interval_decrease_fraction_after
             ) VALUES ('source_configured',$1,$2,$3,$4,$5,$6,$7,
                       $8::float8::numeric,$9::float8::numeric,$10,$11,
                       $12::float8::numeric,$13::float8::numeric,
                       $14::float8::numeric,$15::float8::numeric,
                       $16::float8::numeric,$17::float8::numeric)",
            &[
                &source_id,
                &actor,
                &reason,
                &before_scope,
                &scope,
                &before_version,
                &policy.policy_version,
                &before_interval,
                &policy.current_interval_hours,
                &before_enabled,
                &after_enabled,
                &before_minimum,
                &policy.minimum_interval_hours,
                &before_maximum,
                &policy.maximum_interval_hours,
                &before_decrease_cap,
                &policy.max_routine_interval_decrease_fraction,
            ],
        )
        .map_err(|error| Error::storage(format!("audit Source policy configuration: {error}")))?;
        tx.commit().map_err(|error| {
            Error::storage(format!("commit Source policy configuration: {error}"))
        })?;
        Ok(())
    }

    /// Reserve due Sources for one scheduled coordinator run.
    pub(crate) fn reserve_due_sources(
        &mut self,
        config: &Config,
        run_id: &str,
        config_digest: &str,
        now: DateTime<Utc>,
    ) -> Result<HashSet<String>, Error> {
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin scheduled Source reservation: {error}"))
        })?;
        let globally_enabled: bool = tx
            .query_one(
                "SELECT enabled FROM scout_recrawl_control WHERE singleton=TRUE",
                &[],
            )
            .map_err(|error| Error::storage(format!("read scheduled recrawl switch: {error}")))?
            .get(0);
        if !globally_enabled {
            return Err(Error::not_ready("scheduled recrawl is disabled"));
        }
        let inserted_run =
            tx.execute(
                "INSERT INTO scout_crawl_runs(crawl_run_id,config_digest)
             VALUES ($1,$2) ON CONFLICT (crawl_run_id) DO NOTHING",
                &[&run_id, &config_digest],
            )
            .map_err(|error| {
                Error::storage(format!("reserve scheduled crawl run identity: {error}"))
            })? > 0;
        let run = tx.query_one(
            "SELECT config_digest,status FROM scout_crawl_runs WHERE crawl_run_id=$1 FOR UPDATE",
            &[&run_id],
        ).map_err(|error| Error::storage(format!("verify scheduled crawl run identity: {error}")))?;
        let stored_digest: String = run.get(0);
        let run_status: String = run.get(1);
        if stored_digest != config_digest {
            return Err(Error::invalid(
                "scheduled crawl run ID already belongs to a different config",
            ));
        }
        if run_status == "failed" {
            return Err(Error::invalid(
                "failed scheduled crawl run cannot be resumed",
            ));
        }
        let persisted_sources: HashSet<String> = tx
            .query(
                "SELECT source_id FROM scout_scheduled_run_sources WHERE crawl_run_id=$1",
                &[&run_id],
            )
            .map_err(|error| {
                Error::storage(format!(
                    "read persisted scheduled Source selection: {error}"
                ))
            })?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        let existing_claims: i64 = tx
            .query_one(
                "SELECT count(*) FROM scout_source_recrawl_policy WHERE claimed_run_id=$1",
                &[&run_id],
            )
            .map_err(|error| {
                Error::storage(format!("check scheduled Source reservations: {error}"))
            })?
            .get(0);
        if !inserted_run && run_status != "active" {
            return Err(Error::invalid(
                "completed scheduled crawl run cannot be resumed",
            ));
        }
        if !inserted_run && persisted_sources.is_empty() && existing_claims == 0 {
            return Err(Error::invalid(
                "run ID belongs to a manual or unreserved crawl and cannot be resumed as scheduled",
            ));
        }
        let mut due = HashSet::new();
        for source in &config.sources {
            let expected_scope = source_scope_sha256(source)?;
            let row = tx
                .query_opt(
                    "SELECT policy.scope_sha256, policy.enabled,
                            policy.current_interval_hours::float8,
                            (EXTRACT(EPOCH FROM policy.next_due_at) * 1000)::BIGINT,
                            policy.claimed_run_id,
                            run.status
                     FROM scout_source_recrawl_policy AS policy
                     LEFT JOIN scout_crawl_runs AS run
                       ON run.crawl_run_id=policy.claimed_run_id
                     WHERE policy.source_id=$1 FOR UPDATE OF policy",
                    &[&source.source_id],
                )
                .map_err(|error| Error::storage(format!("read scheduled Source policy: {error}")))?
                .ok_or_else(|| {
                    Error::not_ready(format!(
                        "scheduled Source {:?} has no approved policy",
                        source.source_id
                    ))
                })?;
            let scope_sha256: String = row.get(0);
            let enabled: bool = row.get(1);
            let interval_hours: f64 = row.get(2);
            let mut next_due_at_ms: i64 = row.get(3);
            let claimed_run: Option<String> = row.get(4);
            let claimed_status: Option<String> = row.get(5);
            if scope_sha256 != expected_scope {
                return Err(Error::not_ready(format!(
                    "scheduled Source {:?} scope differs from its approved scope hash",
                    source.source_id
                )));
            }
            if !persisted_sources.is_empty() && !persisted_sources.contains(&source.source_id) {
                continue;
            }
            if !enabled {
                if persisted_sources.contains(&source.source_id) {
                    return Err(Error::not_ready(format!(
                        "reserved Source {:?} was disabled before scheduled run recovery",
                        source.source_id
                    )));
                }
                continue;
            }
            if persisted_sources.contains(&source.source_id) {
                match (claimed_run.as_deref(), claimed_status.as_deref()) {
                    (Some(claimed), Some("active")) if claimed == run_id => {}
                    (Some(claimed), Some("active")) => {
                        return Err(Error::not_ready(format!(
                            "reserved Source {:?} belongs to another active run {claimed:?}",
                            source.source_id
                        )));
                    }
                    _ => {
                        tx.execute(
                            "UPDATE scout_source_recrawl_policy
                             SET claimed_run_id=$2,claimed_at=clock_timestamp()
                             WHERE source_id=$1",
                            &[&source.source_id, &run_id],
                        )
                        .map_err(|error| {
                            Error::storage(format!("restore scheduled Source reservation: {error}"))
                        })?;
                    }
                }
                due.insert(source.source_id.clone());
                continue;
            }
            match (claimed_run.as_deref(), claimed_status.as_deref()) {
                (Some(claimed), Some("active")) if claimed == run_id => {
                    due.insert(source.source_id.clone());
                    continue;
                }
                (Some(_), Some("active")) => continue,
                (Some(claimed), Some("completed")) => {
                    let work_completed: bool = tx
                        .query_opt(
                            "SELECT work_completed FROM scout_scheduled_run_sources
                             WHERE crawl_run_id=$1 AND source_id=$2",
                            &[&claimed, &source.source_id],
                        )
                        .map_err(|error| {
                            Error::storage(format!(
                                "read completed scheduled Source outcome: {error}"
                            ))
                        })?
                        .is_some_and(|row| row.get(0));
                    if work_completed {
                        next_due_at_ms = tx
                            .query_one(
                                "UPDATE scout_source_recrawl_policy
                             SET next_due_at=clock_timestamp() +
                                   (current_interval_hours::double precision * interval '1 hour'),
                                 last_scheduled_run_id=$2, last_scheduled_at=clock_timestamp(),
                                 claimed_run_id=NULL, claimed_at=NULL,
                                 last_schedule_outcome='completed_before_checkpoint'
                             WHERE source_id=$1
                             RETURNING (EXTRACT(EPOCH FROM next_due_at) * 1000)::BIGINT",
                                &[&source.source_id, &claimed],
                            )
                            .map_err(|error| {
                                Error::storage(format!(
                                    "recover completed Source schedule: {error}"
                                ))
                            })?
                            .get(0);
                    } else {
                        tx.execute(
                            "UPDATE scout_source_recrawl_policy
                             SET claimed_run_id=NULL, claimed_at=NULL,
                                 last_schedule_outcome='incomplete_recovered'
                             WHERE source_id=$1",
                            &[&source.source_id],
                        )
                        .map_err(|error| {
                            Error::storage(format!("release incomplete Source schedule: {error}"))
                        })?;
                    }
                }
                (Some(_), _) => {
                    tx.execute(
                        "UPDATE scout_source_recrawl_policy
                         SET claimed_run_id=NULL, claimed_at=NULL,
                             last_schedule_outcome='previous_run_inactive'
                         WHERE source_id=$1",
                        &[&source.source_id],
                    )
                    .map_err(|error| {
                        Error::storage(format!("release inactive Source schedule: {error}"))
                    })?;
                }
                (None, _) => {}
            }
            if next_due_at_ms <= now.timestamp_millis() {
                tx.execute(
                    "UPDATE scout_source_recrawl_policy
                     SET claimed_run_id=$2, claimed_at=clock_timestamp()
                     WHERE source_id=$1",
                    &[&source.source_id, &run_id],
                )
                .map_err(|error| Error::storage(format!("reserve due Source schedule: {error}")))?;
                due.insert(source.source_id.clone());
            }
        }
        if !persisted_sources.is_empty() && due != persisted_sources {
            return Err(Error::not_ready(
                "scheduled Source reservations differ from the persisted run scope",
            ));
        }
        if due.is_empty() && inserted_run {
            tx.execute(
                "DELETE FROM scout_crawl_runs WHERE crawl_run_id=$1 AND status='active'",
                &[&run_id],
            )
            .map_err(|error| {
                Error::storage(format!("discard not-due scheduled crawl run: {error}"))
            })?;
        }
        tx.commit().map_err(|error| {
            Error::storage(format!("commit scheduled Source reservation: {error}"))
        })?;
        Ok(due)
    }

    /// Advance due times only after the associated coordinator run completed.
    pub(crate) fn complete_scheduled_run(
        &mut self,
        run_id: &str,
        stop_reason: &str,
    ) -> Result<(), Error> {
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin scheduled Source completion: {error}"))
        })?;
        let selected_count: i64 = tx
            .query_one(
                "SELECT count(*) FROM scout_scheduled_run_sources WHERE crawl_run_id=$1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("count persisted scheduled Sources: {error}")))?
            .get(0);
        let updated = tx
            .execute(
                "UPDATE scout_source_recrawl_policy AS policy
                 SET next_due_at=CASE WHEN selected.work_completed THEN clock_timestamp() +
                         (policy.current_interval_hours::double precision * interval '1 hour')
                         ELSE policy.next_due_at END,
                     last_scheduled_run_id=CASE WHEN selected.work_completed THEN $1
                         ELSE policy.last_scheduled_run_id END,
                     last_scheduled_at=CASE WHEN selected.work_completed THEN clock_timestamp()
                         ELSE policy.last_scheduled_at END,
                     last_schedule_outcome=CASE WHEN selected.work_completed THEN $2
                         ELSE 'incomplete' END,
                     claimed_run_id=NULL, claimed_at=NULL
                 FROM scout_scheduled_run_sources AS selected
                 WHERE selected.crawl_run_id=$1 AND selected.source_id=policy.source_id
                   AND policy.claimed_run_id=$1",
                &[&run_id, &stop_reason],
            )
            .map_err(|error| {
                Error::storage(format!(
                    "advance completed scheduled Source intervals: {error}"
                ))
            })?;
        if updated as i64 != selected_count {
            return Err(Error::not_ready(
                "scheduled Source completion claims do not match the persisted run scope",
            ));
        }
        tx.commit().map_err(|error| {
            Error::storage(format!("commit scheduled Source completion: {error}"))
        })?;
        Ok(())
    }

    pub(crate) fn pending_reviews(&mut self) -> Result<Vec<Value>, Error> {
        self.client
            .query(
                "SELECT recommendation_id, source_id, expected_policy_version,
                        validation_status, evidence_status, proposed_interval_hours::float8,
                        evidence_scope_sha256, imported_at::text
                 FROM scout_recrawl_recommendation_imports
                 WHERE outcome='pending_review' ORDER BY imported_at, import_id",
                &[],
            )
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        json!({
                            "recommendation_id": row.get::<_, Option<String>>(0),
                            "source_id": row.get::<_, Option<String>>(1),
                            "expected_policy_version": row.get::<_, Option<String>>(2),
                            "validation_status": row.get::<_, String>(3),
                            "evidence_status": row.get::<_, Option<String>>(4),
                            "proposed_interval_hours": row.get::<_, Option<f64>>(5),
                            "scope_sha256": row.get::<_, Option<String>>(6),
                            "imported_at_utc": row.get::<_, String>(7),
                        })
                    })
                    .collect()
            })
            .map_err(|error| Error::storage(format!("list recrawl policy reviews: {error}")))
    }

    /// Validate, audit, and classify one immutable recommendation import.
    pub(crate) fn import_recommendation(
        &mut self,
        payload: &str,
        config: &Config,
        now: DateTime<Utc>,
    ) -> Result<ImportResult, Error> {
        let value: Value = match serde_json::from_str(payload) {
            Ok(value) => value,
            Err(error) => {
                let reason = format!("invalid JSON: {error}");
                return self.store_unparsed(payload, reason);
            }
        };
        let recommendation: Recommendation = match serde_json::from_value(value.clone()) {
            Ok(recommendation) => recommendation,
            Err(error) => {
                let reason = format!("recommendation schema is invalid: {error}");
                return self.store_unparsed(payload, reason);
            }
        };

        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin recrawl recommendation import: {error}"))
        })?;
        if let Some(existing) = find_duplicate(&mut tx, &recommendation, &value)? {
            tx.commit().map_err(|error| {
                Error::storage(format!("finish duplicate recrawl import: {error}"))
            })?;
            return Ok(existing);
        }

        let mut validation = validate_recommendation(&recommendation, now);
        let source = config
            .sources
            .iter()
            .find(|source| source.source_id == recommendation.source_id);
        let source_scope = source.map(source_scope_sha256).transpose()?;
        let extraction_scope = source.map(|_| extraction_contract_sha256(config));
        let global_enabled: bool = tx
            .query_one(
                "SELECT enabled FROM scout_recrawl_control WHERE singleton=TRUE",
                &[],
            )
            .map_err(|error| Error::storage(format!("read recrawl policy switch: {error}")))?
            .get(0);
        let policy = read_source_policy(&mut tx, &recommendation.source_id)?;

        if source.is_none() {
            validation.push(
                "Source is not owned by the loaded crawl configuration".into(),
                "out_of_scope",
            );
        } else if recommendation
            .evidence
            .scope_sha256
            .as_deref()
            .is_some_and(|scope| Some(scope) != source_scope.as_deref())
        {
            validation.push(
                "evidence scope hash does not match the loaded Source allowlist".into(),
                "out_of_scope",
            );
        } else if recommendation
            .evidence
            .extraction_contract_sha256
            .as_deref()
            .is_some_and(|hash| Some(hash) != extraction_scope.as_deref())
        {
            validation.push(
                "evidence extraction contract does not match the loaded crawl policy".into(),
                "out_of_scope",
            );
        }

        if recommendation.evidence.status == "provisional"
            && validation.status != "invalid"
            && validation.status != "unsupported"
        {
            if let Some(basis) = parse_idempotency_basis(&recommendation).ok() {
                validate_provisional_ownership(
                    &mut tx,
                    &recommendation,
                    &basis,
                    now,
                    &mut validation,
                )?;
            }
        }

        match policy.as_ref() {
            None => validation.push(
                "Source has no persisted approved recrawl policy".into(),
                "out_of_scope",
            ),
            Some(policy) => validate_against_policy(&recommendation, policy, &mut validation),
        }

        let policy_version = policy.as_ref().map(|policy| policy.policy_version.as_str());
        let source_policy_enabled = policy.as_ref().is_some_and(|policy| policy.enabled);
        let target_scope = source_scope.as_deref();
        let scope_change = policy
            .as_ref()
            .is_some_and(|policy| target_scope.is_some_and(|scope| policy.scope_sha256 != scope));
        let needs_review = recommendation.evidence.status == "provisional"
            || scope_change
            || policy.as_ref().is_some_and(|policy| {
                recommendation
                    .interval_hours
                    .proposed
                    .is_some_and(|proposed| {
                        proposed
                            < policy.current_interval_hours
                                * (1.0 - policy.max_routine_interval_decrease_fraction)
                    })
            })
            || recommendation.confidence == "low"
            || recommendation.confidence == "provisional";
        if recommendation.evidence.status == "provisional" && validation.status == "valid" {
            validation.status = "provisional_review_required";
        }

        let outcome =
            if validation.status != "valid" && validation.status != "provisional_review_required" {
                validation.status.to_owned()
            } else if !global_enabled || !source_policy_enabled {
                "disabled".to_owned()
            } else if needs_review || validation.status == "provisional_review_required" {
                "pending_review".to_owned()
            } else {
                "scheduled".to_owned()
            };
        insert_import(
            &mut tx,
            Some(&recommendation),
            payload,
            validation.status,
            &validation.reasons,
            &outcome,
            policy_version,
            target_scope,
        )?;
        if outcome == "scheduled" {
            let policy = policy
                .as_ref()
                .expect("valid recommendation has a persisted Source policy");
            let source = source.expect("valid recommendation has a configured Source");
            let scope = source_scope
                .as_deref()
                .expect("configured Source scope hash");
            let proposed = recommendation
                .interval_hours
                .proposed
                .expect("validated proposed interval");
            let next_version = apply_policy_change(
                &mut tx,
                &recommendation.recommendation_id,
                &recommendation.source_id,
                &policy.scope_sha256,
                scope,
                policy,
                proposed,
                None,
            )?;
            let _ = source;
            tx.execute(
                "UPDATE scout_recrawl_recommendation_imports
                 SET applied_policy_version=$2, scheduling_outcome='automatic policy applied',
                     completed_at=clock_timestamp()
                 WHERE recommendation_id=$1",
                &[&recommendation.recommendation_id, &next_version],
            )
            .map_err(|error| {
                Error::storage(format!(
                    "record automatic recrawl policy application: {error}"
                ))
            })?;
            insert_policy_audit(
                &mut tx,
                &recommendation.recommendation_id,
                &recommendation.source_id,
                None,
                "automatic",
                &policy.policy_version,
                &next_version,
                policy.current_interval_hours,
                proposed,
                &policy.scope_sha256,
                scope,
                "scheduled",
                "routine adjustment applied within approved policy limits",
            )?;
        }
        tx.commit().map_err(|error| {
            Error::storage(format!("commit recrawl recommendation import: {error}"))
        })?;
        Ok(ImportResult {
            recommendation_id: Some(recommendation.recommendation_id),
            validation_status: validation.status.into(),
            outcome,
            duplicate: false,
        })
    }

    /// Apply an explicit actor decision to one pending recommendation.
    pub(crate) fn decide(
        &mut self,
        recommendation_id: &str,
        approved: bool,
        actor: &str,
        reason: &str,
        config: &Config,
        now: DateTime<Utc>,
    ) -> Result<DecisionResult, Error> {
        validate_actor(actor)?;
        if reason.trim().is_empty() || reason.len() > 4000 {
            return Err(Error::invalid(
                "decision reason must contain 1 to 4000 characters",
            ));
        }
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin recrawl policy decision: {error}")))?;
        let row = tx
            .query_opt(
                "SELECT source_id, evidence_scope_sha256, expected_policy_version,
                    policy_version_at_import, current_interval_hours::float8,
                    proposed_interval_hours::float8, validation_status, outcome, payload_json
             FROM scout_recrawl_recommendation_imports
             WHERE recommendation_id=$1 FOR UPDATE",
                &[&recommendation_id],
            )
            .map_err(|error| Error::storage(format!("load recrawl review item: {error}")))?
            .ok_or_else(|| Error::invalid("recommendation does not exist"))?;
        let source_id: Option<String> = row.get(0);
        let target_scope: Option<String> = row.get(1);
        let expected_version: Option<String> = row.get(2);
        let version_at_import: Option<String> = row.get(3);
        let interval_before: Option<f64> = row.get(4);
        let interval_after: Option<f64> = row.get(5);
        let validation_status: String = row.get(6);
        let outcome: String = row.get(7);
        let payload: String = row.get(8);
        if outcome != "pending_review" {
            return Err(Error::invalid(
                "recommendation is not awaiting an operator decision",
            ));
        }
        let source_id = source_id.ok_or_else(|| Error::invalid("review item has no Source ID"))?;
        let target_scope =
            target_scope.ok_or_else(|| Error::invalid("review item has no Source scope hash"))?;
        let version_at_import =
            version_at_import.ok_or_else(|| Error::invalid("review item has no policy version"))?;
        let interval_before =
            interval_before.ok_or_else(|| Error::invalid("review item has no current interval"))?;
        let interval_after =
            interval_after.ok_or_else(|| Error::invalid("review item has no proposed interval"))?;
        let source = config
            .sources
            .iter()
            .find(|source| source.source_id == source_id)
            .ok_or_else(|| Error::invalid("review Source is not in the loaded configuration"))?;
        let live_scope = source_scope_sha256(source)?;
        if live_scope != target_scope {
            return Err(Error::invalid("review Source scope changed after import"));
        }
        let recommendation: Recommendation = serde_json::from_str(&payload).map_err(|error| {
            Error::storage(format!("stored recommendation JSON is invalid: {error}"))
        })?;
        let policy = read_source_policy(&mut tx, &source_id)?
            .ok_or_else(|| Error::invalid("Source policy was removed before review"))?;
        let global_enabled: bool = tx
            .query_one(
                "SELECT enabled FROM scout_recrawl_control WHERE singleton=TRUE",
                &[],
            )
            .map_err(|error| Error::storage(format!("read recrawl policy switch: {error}")))?
            .get(0);
        if approved {
            let validation = validate_recommendation(&recommendation, now);
            if !matches!(validation.status, "valid" | "provisional_review_required")
                || !matches!(
                    validation_status.as_str(),
                    "valid" | "provisional_review_required"
                )
            {
                return Err(Error::invalid(
                    "recommendation is no longer valid for approval",
                ));
            }
            if recommendation.source_id != source_id
                || recommendation
                    .evidence
                    .scope_sha256
                    .as_deref()
                    .is_some_and(|scope| scope != live_scope)
                || recommendation
                    .evidence
                    .extraction_contract_sha256
                    .as_deref()
                    .is_some_and(|hash| hash != extraction_contract_sha256(config))
            {
                return Err(Error::invalid(
                    "recommendation evidence no longer matches its configured Source",
                ));
            }
            if recommendation.evidence.status == "provisional" {
                let basis = parse_idempotency_basis(&recommendation).map_err(|message| {
                    Error::invalid(format!("invalid recommendation basis: {message}"))
                })?;
                let mut current_validation = Validation {
                    status: "valid",
                    reasons: Vec::new(),
                    generated_at: None,
                    expires_at: None,
                };
                validate_provisional_ownership(
                    &mut tx,
                    &recommendation,
                    &basis,
                    now,
                    &mut current_validation,
                )?;
                if current_validation.status != "valid" {
                    return Err(Error::invalid(
                        "provisional evidence is no longer fresh and Source-owned",
                    ));
                }
                validate_provisional_review_ready(
                    &mut tx,
                    recommendation
                        .evidence
                        .provisional_run_id
                        .as_deref()
                        .unwrap_or(""),
                )?;
            }
            if !global_enabled || !policy.enabled {
                return Err(Error::not_ready(
                    "recrawl policy is disabled; review cannot apply an adjustment",
                ));
            }
            let mut current_policy_validation = validation.clone();
            validate_against_policy(&recommendation, &policy, &mut current_policy_validation);
            let current_policy_outcome = if current_policy_validation.status == "out_of_scope" {
                Some("out_of_scope")
            } else if current_policy_validation.status == "stale" {
                Some("stale")
            } else {
                None
            };
            if policy.policy_version != version_at_import
                || expected_version.as_deref() != Some(policy.policy_version.as_str())
                || !same_hours(policy.current_interval_hours, interval_before)
                || !same_hours(recommendation.interval_hours.current, interval_before)
                || current_policy_outcome.is_some()
            {
                let outcome = current_policy_outcome.unwrap_or("stale");
                let detail = current_policy_validation.reasons.join("; ");
                tx.execute(
                    "UPDATE scout_recrawl_recommendation_imports
                     SET outcome=$2, scheduling_outcome=$3,
                         completed_at=clock_timestamp()
                     WHERE recommendation_id=$1",
                    &[&recommendation_id, &outcome, &detail],
                )
                .map_err(|error| Error::storage(format!("mark changed recrawl review: {error}")))?;
                record_decision(
                    &mut tx,
                    recommendation_id,
                    actor,
                    "denied",
                    &format!("{reason}; {detail}"),
                    &policy,
                    &target_scope,
                    interval_before,
                    interval_before,
                )?;
                tx.commit().map_err(|error| {
                    Error::storage(format!("commit changed recrawl review: {error}"))
                })?;
                return Ok(DecisionResult {
                    outcome: outcome.into(),
                    applied_policy_version: None,
                });
            }
        }

        let decision = if approved { "approved" } else { "denied" };
        record_decision(
            &mut tx,
            recommendation_id,
            actor,
            decision,
            reason,
            &policy,
            &target_scope,
            interval_before,
            if approved {
                interval_after
            } else {
                interval_before
            },
        )?;
        if approved {
            let next_version = apply_policy_change(
                &mut tx,
                recommendation_id,
                &source_id,
                &policy.scope_sha256,
                &target_scope,
                &policy,
                interval_after,
                Some(actor),
            )?;
            tx.execute(
                "UPDATE scout_recrawl_recommendation_imports
                 SET outcome='scheduled', applied_policy_version=$2,
                     scheduling_outcome='approved by actor', completed_at=clock_timestamp()
                 WHERE recommendation_id=$1",
                &[&recommendation_id, &next_version],
            )
            .map_err(|error| {
                Error::storage(format!("apply approved recrawl adjustment: {error}"))
            })?;
            insert_policy_audit(
                &mut tx,
                recommendation_id,
                &source_id,
                Some(actor),
                "approved",
                &policy.policy_version,
                &next_version,
                interval_before,
                interval_after,
                &policy.scope_sha256,
                &target_scope,
                "scheduled",
                reason,
            )?;
            tx.commit().map_err(|error| {
                Error::storage(format!("commit approved recrawl policy decision: {error}"))
            })?;
            Ok(DecisionResult {
                outcome: "scheduled".into(),
                applied_policy_version: Some(next_version),
            })
        } else {
            tx.execute(
                "UPDATE scout_recrawl_recommendation_imports
                 SET outcome='rejected', scheduling_outcome='denied by actor', completed_at=clock_timestamp()
                 WHERE recommendation_id=$1",
                &[&recommendation_id],
            ).map_err(|error| Error::storage(format!("reject recrawl change: {error}")))?;
            insert_policy_audit(
                &mut tx,
                recommendation_id,
                &source_id,
                Some(actor),
                "denied",
                &policy.policy_version,
                &policy.policy_version,
                interval_before,
                interval_before,
                &policy.scope_sha256,
                &policy.scope_sha256,
                "denied",
                reason,
            )?;
            tx.commit().map_err(|error| {
                Error::storage(format!("commit denied recrawl policy decision: {error}"))
            })?;
            Ok(DecisionResult {
                outcome: "rejected".into(),
                applied_policy_version: None,
            })
        }
    }

    fn store_unparsed(&mut self, payload: &str, reason: String) -> Result<ImportResult, Error> {
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin invalid recrawl import: {error}")))?;
        insert_raw_invalid(&mut tx, payload, &reason)?;
        tx.commit()
            .map_err(|error| Error::storage(format!("commit invalid recrawl import: {error}")))?;
        Ok(ImportResult {
            recommendation_id: None,
            validation_status: "invalid".into(),
            outcome: "invalid".into(),
            duplicate: false,
        })
    }
}

fn find_duplicate(
    tx: &mut Transaction<'_>,
    recommendation: &Recommendation,
    value: &Value,
) -> Result<Option<ImportResult>, Error> {
    if recommendation.recommendation_id != recommendation.idempotency_key
        || !is_sha256_id(&recommendation.idempotency_key)
    {
        return Ok(None);
    }
    let row = tx
        .query_opt(
            "SELECT recommendation_id, payload_json, validation_status, outcome
             FROM scout_recrawl_recommendation_imports
             WHERE idempotency_key=$1 OR recommendation_id=$1 FOR UPDATE",
            &[&recommendation.idempotency_key],
        )
        .map_err(|error| Error::storage(format!("check recrawl idempotency key: {error}")))?;
    let Some(row) = row else { return Ok(None) };
    let saved_json: String = row.get(1);
    let saved: Value = serde_json::from_str(&saved_json).unwrap_or(Value::Null);
    if &saved != value {
        return Err(Error::invalid(
            "recommendation ID was reused for a different payload",
        ));
    }
    Ok(Some(ImportResult {
        recommendation_id: row.get(0),
        validation_status: row.get(2),
        outcome: row.get(3),
        duplicate: true,
    }))
}

fn insert_import(
    tx: &mut Transaction<'_>,
    recommendation: Option<&Recommendation>,
    payload: &str,
    validation_status: &str,
    reasons: &[String],
    outcome: &str,
    policy_version: Option<&str>,
    target_scope: Option<&str>,
) -> Result<(), Error> {
    let recommendation_id = recommendation
        .filter(|recommendation| {
            recommendation.recommendation_id == recommendation.idempotency_key
                && is_sha256_id(&recommendation.recommendation_id)
        })
        .map(|recommendation| recommendation.recommendation_id.as_str());
    let idempotency_key = recommendation
        .filter(|recommendation| {
            recommendation.recommendation_id == recommendation.idempotency_key
                && is_sha256_id(&recommendation.idempotency_key)
        })
        .map(|recommendation| recommendation.idempotency_key.as_str());
    let (source_id, generation_id, evidence_status, scope, expected_version, current, proposed) =
        recommendation.map_or(
            (None, None, None, None, None, None, None),
            |recommendation| {
                (
                    Some(recommendation.source_id.as_str()),
                    Some(recommendation.generation_id.as_str()),
                    Some(recommendation.evidence.status.as_str()),
                    target_scope.or(recommendation.evidence.scope_sha256.as_deref()),
                    Some(recommendation.expected_policy_version.as_str()),
                    Some(recommendation.interval_hours.current),
                    recommendation.interval_hours.proposed,
                )
            },
        );
    let reasons = serde_json::to_string(reasons).map_err(|error| {
        Error::storage(format!("serialize recrawl validation reasons: {error}"))
    })?;
    tx.execute(
        "INSERT INTO scout_recrawl_recommendation_imports(
             recommendation_id,idempotency_key,source_id,generation_id,payload_json,
             evidence_status,evidence_scope_sha256,expected_policy_version,
             policy_version_at_import,current_interval_hours,proposed_interval_hours,
             validation_status,validation_reasons,outcome
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::float8::numeric,$11::float8::numeric,
                   $12,$13::text::jsonb,$14)",
        &[
            &recommendation_id,
            &idempotency_key,
            &source_id,
            &generation_id,
            &payload,
            &evidence_status,
            &scope,
            &expected_version,
            &policy_version,
            &current,
            &proposed,
            &validation_status,
            &reasons,
            &outcome,
        ],
    )
    .map_err(|error| Error::storage(format!("persist recrawl recommendation import: {error}")))?;
    Ok(())
}

fn insert_raw_invalid(tx: &mut Transaction<'_>, payload: &str, reason: &str) -> Result<(), Error> {
    let reasons = serde_json::to_string(&[reason])
        .map_err(|error| Error::storage(format!("serialize invalid recrawl reason: {error}")))?;
    tx.execute(
        "INSERT INTO scout_recrawl_recommendation_imports(
             payload_json,validation_status,validation_reasons,outcome
         ) VALUES ($1,'invalid',$2::text::jsonb,'invalid')",
        &[&payload, &reasons],
    )
    .map_err(|error| Error::storage(format!("persist invalid recrawl import: {error}")))?;
    Ok(())
}

fn read_source_policy(
    tx: &mut Transaction<'_>,
    source_id: &str,
) -> Result<Option<StoredPolicy>, Error> {
    tx.query_opt(
        "SELECT scope_sha256, policy_version, current_interval_hours::float8,
                minimum_interval_hours::float8, maximum_interval_hours::float8,
                max_routine_interval_decrease_fraction::float8, enabled
         FROM scout_source_recrawl_policy WHERE source_id=$1 FOR UPDATE",
        &[&source_id],
    )
    .map(|row| {
        row.map(|row| StoredPolicy {
            scope_sha256: row.get(0),
            policy_version: row.get(1),
            current_interval_hours: row.get(2),
            minimum_interval_hours: row.get(3),
            maximum_interval_hours: row.get(4),
            max_routine_interval_decrease_fraction: row.get(5),
            enabled: row.get(6),
        })
    })
    .map_err(|error| Error::storage(format!("read Source recrawl policy: {error}")))
}

fn record_decision(
    tx: &mut Transaction<'_>,
    recommendation_id: &str,
    actor: &str,
    decision: &str,
    reason: &str,
    policy: &StoredPolicy,
    scope_after: &str,
    interval_before: f64,
    interval_after: f64,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO scout_recrawl_policy_decisions(
             recommendation_id,actor,decision,reason,scope_sha256_before,
             scope_sha256_after,policy_version,interval_hours_before,interval_hours_after
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::float8::numeric,$9::float8::numeric)",
        &[
            &recommendation_id,
            &actor,
            &decision,
            &reason,
            &policy.scope_sha256,
            &scope_after,
            &policy.policy_version,
            &interval_before,
            &interval_after,
        ],
    )
    .map_err(|error| Error::storage(format!("persist recrawl actor decision: {error}")))?;
    Ok(())
}

fn validate_provisional_ownership(
    tx: &mut Transaction<'_>,
    recommendation: &Recommendation,
    basis: &RecommendationBasis,
    now: DateTime<Utc>,
    validation: &mut Validation,
) -> Result<(), Error> {
    let Some(run_id) = recommendation.evidence.provisional_run_id.as_deref() else {
        validation.push("provisional run ID is missing".into(), "unsupported");
        return Ok(());
    };
    let generated = parse_recommendation_utc(&recommendation.generated_at_utc, "generated_at_utc")?;
    let generated_ms = generated.timestamp_millis();
    let max_age_seconds = basis
        .recommendation_settings
        .maximum_provisional_event_age_seconds
        .parse::<i64>()
        .unwrap_or_default();
    if !run_id.strip_prefix("run-").is_some_and(is_hex_sha256) {
        validation.push("provisional run ID is malformed".into(), "invalid");
        return Ok(());
    }
    let run = tx
        .query_opt(
            "SELECT status, (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT
         FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("read provisional crawl run: {error}")))?;
    let Some(run) = run else {
        validation.push(
            "provisional run is not persisted locally".into(),
            "out_of_scope",
        );
        return Ok(());
    };
    let run_created_ms: i64 = run.get(1);
    if run_created_ms > generated.timestamp_millis() {
        validation.push(
            "provisional run started after recommendation generation".into(),
            "out_of_scope",
        );
    }
    let terminal_at_generation: bool = tx
        .query_one(
            "SELECT EXISTS (
             SELECT 1 FROM scout_crawl_events
             WHERE crawl_run_id=$1
               AND event_type IN ('crawl_completed','corpus_snapshot_published')
             AND created_at <= to_timestamp($2::double precision / 1000.0)
         )",
            &[&run_id, &generated_ms],
        )
        .map_err(|error| {
            Error::storage(format!("check provisional run generation state: {error}"))
        })?
        .get(0);
    if terminal_at_generation {
        validation.push(
            "provisional run had completed before recommendation generation".into(),
            "unsupported",
        );
    }

    for event_id in &recommendation.evidence.provisional_event_ids {
        if event_id.is_empty()
            || event_id.len() > 512
            || !event_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        {
            validation.push("provisional event ID is malformed".into(), "invalid");
            continue;
        }
        let event = tx
            .query_opt(
                "SELECT event_type, payload_json, (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT
             FROM scout_crawl_events WHERE event_id=$1 AND crawl_run_id=$2",
                &[&event_id, &run_id],
            )
            .map_err(|error| Error::storage(format!("read provisional capture event: {error}")))?;
        let Some(event) = event else {
            validation.push(
                "provisional event is not persisted under the referenced run".into(),
                "out_of_scope",
            );
            continue;
        };
        let event_type: String = event.get(0);
        let payload: String = event.get(1);
        let created_ms: i64 = event.get(2);
        if event_type != "capture_committed" {
            validation.push(
                "provisional event is not a capture_committed event".into(),
                "unsupported",
            );
            continue;
        }
        let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
        let data = &value["data"];
        if value["event_type"].as_str() != Some("capture_committed")
            || data["event_id"].as_str() != Some(event_id.as_str())
            || data["crawl_run_id"].as_str() != Some(run_id)
            || data["source_id"].as_str() != Some(recommendation.source_id.as_str())
        {
            validation.push(
                "provisional event is not owned by the recommendation Source".into(),
                "out_of_scope",
            );
        }
        if created_ms > generated.timestamp_millis()
            || created_ms > now.timestamp_millis()
            || max_age_seconds <= 0
            || generated.timestamp_millis().saturating_sub(created_ms)
                > max_age_seconds.saturating_mul(1000)
            || now.timestamp_millis().saturating_sub(created_ms)
                > max_age_seconds.saturating_mul(1000)
        {
            validation.push(
                "provisional capture event is future-dated or stale".into(),
                "stale",
            );
        }
    }
    let expected_capture = recommendation
        .evidence
        .latest_capture_event_at_utc
        .as_deref()
        .and_then(|value| parse_utc(value, "latest_capture_event_at_utc").ok())
        .map(|value| value.timestamp_millis());
    let expected_latest = recommendation
        .evidence
        .latest_run_event_at_utc
        .as_deref()
        .and_then(|value| parse_utc(value, "latest_run_event_at_utc").ok())
        .map(|value| value.timestamp_millis());
    let latest: Option<(Option<i64>, Option<i64>)> = tx.query_opt(
        "SELECT
             (EXTRACT(EPOCH FROM MAX(created_at) FILTER (WHERE event_type='capture_committed')) * 1000)::BIGINT,
             (EXTRACT(EPOCH FROM MAX(created_at)) * 1000)::BIGINT
         FROM scout_crawl_events
         WHERE crawl_run_id=$1
           AND created_at <= to_timestamp($2::double precision / 1000.0)",
        &[&run_id, &generated_ms],
    ).map_err(|error| Error::storage(format!("read provisional run event freshness: {error}")))?
        .map(|row| (row.get(0), row.get(1)));
    if let Some((capture_ms, latest_ms)) = latest {
        if expected_capture != capture_ms || expected_latest != latest_ms {
            validation.push(
                "provisional latest-event timestamps do not match persisted run events".into(),
                "out_of_scope",
            );
        }
        if [capture_ms, latest_ms]
            .into_iter()
            .flatten()
            .any(|event_ms| {
                event_ms > now.timestamp_millis()
                    || now.timestamp_millis().saturating_sub(event_ms)
                        > max_age_seconds.saturating_mul(1000)
            })
        {
            validation.push("provisional run activity is stale".into(), "stale");
        }
    }
    Ok(())
}

fn validate_provisional_review_ready(tx: &mut Transaction<'_>, run_id: &str) -> Result<(), Error> {
    let ready: bool = tx
        .query_one(
            "SELECT run.status='completed' AND EXISTS (
             SELECT 1 FROM scout_crawl_events AS event
             WHERE event.crawl_run_id=run.crawl_run_id
               AND event.event_type='corpus_snapshot_published'
         )
         FROM scout_crawl_runs AS run WHERE run.crawl_run_id=$1",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("validate reconciled provisional run: {error}")))?
        .get(0);
    if !ready {
        return Err(Error::not_ready(
            "provisional review requires the Run to be completed and corpus_snapshot_published to be persisted",
        ));
    }
    Ok(())
}

fn apply_policy_change(
    tx: &mut Transaction<'_>,
    recommendation_id: &str,
    source_id: &str,
    scope_before: &str,
    scope_after: &str,
    policy: &StoredPolicy,
    interval_after: f64,
    actor: Option<&str>,
) -> Result<String, Error> {
    let changed =
        scope_before != scope_after || !same_hours(policy.current_interval_hours, interval_after);
    let next_version = if changed {
        scope_policy_version(
            &policy.policy_version,
            scope_after,
            interval_after,
            recommendation_id,
        )
    } else {
        policy.policy_version.clone()
    };
    let updated_by = actor.unwrap_or("scout:auto");
    tx.execute(
        "UPDATE scout_source_recrawl_policy
         SET scope_sha256=$2, policy_version=$3,
             current_interval_hours=$4::float8::numeric,
             next_due_at=CASE WHEN last_scheduled_at IS NULL THEN clock_timestamp()
                 ELSE last_scheduled_at + ($4::float8 * interval '1 hour') END,
             updated_by=$5, updated_at=clock_timestamp()
         WHERE source_id=$1",
        &[
            &source_id,
            &scope_after,
            &next_version,
            &interval_after,
            &updated_by,
        ],
    )
    .map_err(|error| Error::storage(format!("apply Source recrawl policy: {error}")))?;
    Ok(next_version)
}

fn insert_policy_audit(
    tx: &mut Transaction<'_>,
    recommendation_id: &str,
    source_id: &str,
    actor: Option<&str>,
    decision: &str,
    version_before: &str,
    version_after: &str,
    interval_before: f64,
    interval_after: f64,
    scope_before: &str,
    scope_after: &str,
    outcome: &str,
    detail: &str,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO scout_recrawl_policy_audit(
             recommendation_id,source_id,actor,decision,policy_version_before,
             policy_version_after,interval_hours_before,interval_hours_after,
             scope_sha256_before,scope_sha256_after,outcome,outcome_detail
         ) VALUES ($1,$2,$3,$4,$5,$6,$7::float8::numeric,$8::float8::numeric,$9,$10,$11,$12)",
        &[
            &recommendation_id,
            &source_id,
            &actor,
            &decision,
            &version_before,
            &version_after,
            &interval_before,
            &interval_after,
            &scope_before,
            &scope_after,
            &outcome,
            &detail,
        ],
    )
    .map_err(|error| Error::storage(format!("audit Source recrawl policy: {error}")))?;
    Ok(())
}

fn validate_policy_config(policy: &SourcePolicyConfig) -> Result<(), Error> {
    let values = [
        policy.current_interval_hours,
        policy.minimum_interval_hours,
        policy.maximum_interval_hours,
        policy.max_routine_interval_decrease_fraction,
    ];
    if policy.policy_version.trim().is_empty()
        || policy.policy_version.len() > 256
        || values.iter().any(|value| !value.is_finite())
        || policy.minimum_interval_hours <= 0.0
        || policy.minimum_interval_hours > policy.current_interval_hours
        || policy.current_interval_hours > policy.maximum_interval_hours
        || !(0.0..1.0).contains(&policy.max_routine_interval_decrease_fraction)
    {
        return Err(Error::invalid(
            "invalid Source recrawl policy version, interval bounds, or automatic adjustment cap",
        ));
    }
    for value in values[..3].iter() {
        if !same_hours(*value, (*value * 1000.0).round() / 1000.0) {
            return Err(Error::invalid(
                "Source recrawl intervals must have at most three decimal places",
            ));
        }
    }
    Ok(())
}

fn validate_actor(actor: &str) -> Result<(), Error> {
    if actor.trim().is_empty()
        || actor.len() > 256
        || !actor.is_ascii()
        || actor.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid(
            "actor must be a nonempty ASCII audit identity of at most 256 bytes",
        ));
    }
    Ok(())
}

fn is_sha256_id(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(is_hex_sha256)
}

fn is_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn same_hours(left: f64, right: f64) -> bool {
    (left - right).abs() <= 0.0005001
}

fn same_six_decimal(left: f64, right: f64) -> bool {
    (left - right).abs() <= 0.0000011
}

fn format_hours(value: f64) -> String {
    format!("{value:.3}")
}

fn format_six_decimal(value: f64) -> String {
    format!("{value:.6}")
}

fn scope_policy_version(
    previous: &str,
    scope_sha256: &str,
    interval_hours: f64,
    recommendation_id: &str,
) -> String {
    let material = format!("{previous}\n{scope_sha256}\n{interval_hours:.3}\n{recommendation_id}");
    format!("sha256:{}", sha256_hex(material.as_bytes()))
}
