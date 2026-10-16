//! Pure recommendation envelope, basis, freshness, and evidence validation.

use super::*;
use crate::crawl::{EXTRACTION_SCHEMA, Source};
use std::collections::BTreeMap;

const RECOMMENDATION_SCHEMA: &str = "scout.recrawl-recommendation.v1";
const MAX_RECOMMENDATION_AGE_DAYS: i64 = 7;

impl Validation {
    pub(super) fn push(&mut self, reason: String, status: &'static str) {
        self.reasons.push(reason);
        if validation_rank(status) > validation_rank(self.status) {
            self.status = status;
        }
    }
}

fn validation_rank(status: &str) -> u8 {
    match status {
        "valid" => 0,
        "provisional_review_required" => 1,
        "unsupported" => 2,
        "out_of_scope" => 3,
        "stale" => 4,
        _ => 5,
    }
}

pub(super) fn validate_recommendation(
    recommendation: &Recommendation,
    now: DateTime<Utc>,
) -> Validation {
    let mut validation = Validation {
        status: "valid",
        reasons: Vec::new(),
        generated_at: None,
        expires_at: None,
    };
    if recommendation.schema_version != RECOMMENDATION_SCHEMA {
        validation.push(
            "unsupported recommendation schema_version".into(),
            "unsupported",
        );
    }
    if recommendation.recommendation_id != recommendation.idempotency_key
        || !is_sha256_id(&recommendation.recommendation_id)
    {
        validation.push(
            "recommendation_id and idempotency_key must be the same sha256 ID".into(),
            "invalid",
        );
    }
    let basis = parse_idempotency_basis(recommendation).ok();
    if basis.is_none() {
        validation.push(
            "idempotency_basis hash or nested fields do not bind the recommendation".into(),
            "invalid",
        );
    }
    if recommendation.generation_id.trim().is_empty()
        || recommendation.source_id.trim().is_empty()
        || recommendation.expected_policy_version.trim().is_empty()
        || recommendation
            .reasons
            .iter()
            .any(|reason| reason.trim().is_empty())
    {
        validation.push(
            "required identity, policy-version, or reason fields are empty".into(),
            "invalid",
        );
    }
    let generated = parse_recommendation_utc(&recommendation.generated_at_utc, "generated_at_utc");
    let expires = parse_recommendation_utc(&recommendation.expires_at_utc, "expires_at_utc");
    validation.generated_at = generated.as_ref().ok().copied();
    validation.expires_at = expires.as_ref().ok().copied();
    match (generated, expires) {
        (Ok(generated), Ok(expires)) => {
            if generated > now
                || expires <= now
                || expires <= generated
                || now - generated > Duration::days(MAX_RECOMMENDATION_AGE_DAYS)
            {
                validation.push(
                    "recommendation is expired, future-dated, or too old".into(),
                    "stale",
                );
            }
        }
        _ => validation.push(
            "recommendation timestamps must be RFC 3339 UTC strings ending in Z".into(),
            "invalid",
        ),
    }
    validate_interval(recommendation, basis.as_ref(), &mut validation);
    validate_evidence(recommendation, basis.as_ref(), now, &mut validation);
    if !matches!(
        recommendation.confidence.as_str(),
        "high" | "medium" | "low" | "provisional"
    ) {
        validation.push("unsupported confidence value".into(), "unsupported");
    }
    if let Some(basis) = basis.as_ref() {
        let expected_confidence = match recommendation.evidence.status.as_str() {
            "completed_history" => {
                let min_snapshots = basis
                    .recommendation_settings
                    .high_confidence_minimum_snapshots
                    .parse::<usize>()
                    .unwrap_or(usize::MAX);
                let min_observations = basis
                    .recommendation_settings
                    .high_confidence_minimum_comparable_page_observations
                    .parse::<i64>()
                    .unwrap_or(i64::MAX);
                if recommendation.evidence.snapshot_refs.len() >= min_snapshots
                    && recommendation
                        .evidence
                        .comparable_page_observations
                        .unwrap_or_default()
                        >= min_observations
                {
                    "high"
                } else {
                    "medium"
                }
            }
            "provisional" => "provisional",
            "unsupported" => "low",
            _ => "unsupported",
        };
        if recommendation.confidence != expected_confidence {
            validation.push(
                "confidence does not match the documented evidence thresholds".into(),
                "invalid",
            );
        }
    }
    if recommendation.evidence.status == "unsupported" {
        validation.push(
            "recommendation evidence is unsupported".into(),
            "unsupported",
        );
    } else if recommendation.evidence.status == "provisional" {
        if validation.status == "valid" {
            validation.status = "provisional_review_required";
        }
        validation
            .reasons
            .push("provisional evidence requires explicit operator review".into());
    } else if recommendation.evidence.status != "completed_history" {
        validation.push("unsupported evidence status".into(), "unsupported");
    }
    validation
}

fn validate_interval(
    recommendation: &Recommendation,
    basis: Option<&RecommendationBasis>,
    validation: &mut Validation,
) {
    let interval = &recommendation.interval_hours;
    let Some(proposed) = interval.proposed else {
        validation.push("interval_hours.proposed is missing".into(), "unsupported");
        return;
    };
    let values = [
        interval.minimum,
        interval.current,
        interval.maximum,
        proposed,
        interval.adjustment,
    ];
    if values.iter().any(|value| !value.is_finite())
        || interval.minimum <= 0.0
        || interval.minimum > interval.current
        || interval.current > interval.maximum
        || proposed < interval.minimum
        || proposed > interval.maximum
        || !same_hours(proposed - interval.current, interval.adjustment)
    {
        validation.push(
            "proposed interval or its bounds are inconsistent".into(),
            "invalid",
        );
    }
    if let Some(fraction) = interval.adjustment_fraction
        && (!fraction.is_finite()
            || !same_six_decimal(fraction, interval.adjustment / interval.current))
    {
        validation.push(
            "adjustment_fraction does not match proposed/current hours".into(),
            "invalid",
        );
    }
    let actual_type = if interval.adjustment < -0.0005 {
        "decrease"
    } else if interval.adjustment > 0.0005 {
        "increase"
    } else {
        "hold"
    };
    if recommendation.adjustment_type != actual_type {
        validation.push(
            "adjustment_type does not match the signed adjustment".into(),
            "invalid",
        );
    }
    if let Some(basis) = basis {
        let maximum_fraction = basis
            .recommendation_settings
            .max_adjustment_fraction
            .parse::<f64>()
            .unwrap_or(f64::NAN);
        if !maximum_fraction.is_finite()
            || maximum_fraction < 0.0
            || maximum_fraction > 0.5
            || interval
                .adjustment_fraction
                .is_none_or(|fraction| fraction.abs() > maximum_fraction + 0.0000011)
        {
            validation.push(
                "recommendation exceeds the adjustment fraction bound in its basis".into(),
                "out_of_scope",
            );
        }
    }
}

fn validate_evidence(
    recommendation: &Recommendation,
    basis: Option<&RecommendationBasis>,
    now: DateTime<Utc>,
    validation: &mut Validation,
) {
    let evidence = &recommendation.evidence;
    let completed = evidence.status == "completed_history";
    for (label, value) in [
        ("scope", evidence.scope_sha256.as_deref()),
        (
            "extraction contract",
            evidence.extraction_contract_sha256.as_deref(),
        ),
    ] {
        if value.is_some_and(|value| !is_hex_sha256(value)) || (completed && value.is_none()) {
            validation.push(
                format!("{label} evidence hash is missing or malformed"),
                "invalid",
            );
        }
    }
    if evidence.snapshot_refs.iter().any(|reference| {
        reference.snapshot_id.trim().is_empty()
            || !is_hex_sha256(&reference.source_snapshot_manifest_sha256)
            || !is_hex_sha256(&reference.crawl_manifest_sha256)
    }) {
        validation.push(
            "snapshot evidence reference has an invalid ID or checksum".into(),
            "invalid",
        );
    }
    let mut snapshot_ids: Vec<_> = evidence
        .snapshot_refs
        .iter()
        .map(|reference| &reference.snapshot_id)
        .collect();
    snapshot_ids.sort();
    if snapshot_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        validation.push(
            "snapshot evidence references contain duplicate snapshot IDs".into(),
            "invalid",
        );
    }
    if evidence
        .snapshot_refs
        .windows(2)
        .any(|pair| pair[0].snapshot_id > pair[1].snapshot_id)
    {
        validation.push(
            "snapshot evidence references are not sorted by snapshot ID".into(),
            "invalid",
        );
    }
    let settings = basis.map(|basis| &basis.recommendation_settings);
    let max_age_days = settings
        .and_then(|settings| settings.maximum_evidence_age_days.parse::<i64>().ok())
        .unwrap_or(0);
    if completed {
        let Some(max_age_days) = (max_age_days > 0).then_some(max_age_days) else {
            validation.push(
                "maximum evidence age is invalid in the idempotency basis".into(),
                "invalid",
            );
            return;
        };
        for reference in &evidence.snapshot_refs {
            match parse_utc(&reference.observed_at_utc, "snapshot_refs.observed_at_utc") {
                Ok(observed)
                    if observed <= now && now - observed <= Duration::days(max_age_days) => {}
                _ => validation.push(
                    "snapshot evidence is future-dated, stale, or has an invalid UTC timestamp"
                        .into(),
                    "stale",
                ),
            }
        }
        let min_snapshots = settings
            .and_then(|settings| settings.minimum_completed_snapshots.parse::<usize>().ok())
            .unwrap_or(usize::MAX);
        let min_observations = settings
            .and_then(|settings| {
                settings
                    .minimum_comparable_page_observations
                    .parse::<i64>()
                    .ok()
            })
            .unwrap_or(i64::MAX);
        if evidence.snapshot_refs.len() < min_snapshots
            || evidence.comparable_page_observations.unwrap_or_default() < min_observations
        {
            validation.push(
                "completed evidence is below the minimum snapshot or Page-observation threshold"
                    .into(),
                "unsupported",
            );
        }
        if let Some(latest) = evidence.latest_observed_at_utc.as_deref() {
            match parse_utc(latest, "latest_observed_at_utc") {
                Ok(observed)
                    if observed <= now && now - observed <= Duration::days(max_age_days) => {}
                _ => validation.push(
                    "latest evidence observation is future-dated, stale, or malformed".into(),
                    "stale",
                ),
            }
        } else {
            validation.push(
                "completed evidence is missing its latest observation time".into(),
                "unsupported",
            );
        }
    }
    if let (Some(comparable), Some(changed)) = (
        evidence.comparable_page_observations,
        evidence.changed_page_observations,
    ) {
        if comparable < 0 || changed < 0 || changed > comparable {
            validation.push(
                "evidence page-observation counts are inconsistent".into(),
                "invalid",
            );
        }
        if let Some(fraction) = evidence.change_fraction {
            if !fraction.is_finite()
                || !(0.0..=1.0).contains(&fraction)
                || (comparable > 0
                    && !same_six_decimal(fraction, changed as f64 / comparable as f64))
            {
                validation.push(
                    "change_fraction does not match evidence counts".into(),
                    "invalid",
                );
            }
        }
    } else if completed {
        validation.push(
            "completed evidence page-observation counts are missing".into(),
            "unsupported",
        );
    }
    match evidence.status.as_str() {
        "completed_history" if evidence.snapshot_refs.is_empty() => {
            validation.push(
                "completed evidence requires at least one snapshot reference".into(),
                "unsupported",
            );
        }
        "provisional"
            if evidence
                .provisional_run_id
                .as_deref()
                .unwrap_or("")
                .is_empty()
                || evidence.provisional_event_ids.is_empty() =>
        {
            validation.push(
                "provisional evidence requires a run ID and capture event IDs".into(),
                "unsupported",
            );
        }
        "provisional" if !evidence.snapshot_refs.is_empty() => {
            validation.push(
                "provisional evidence cannot include completed snapshots".into(),
                "invalid",
            );
        }
        "completed_history"
            if evidence.provisional_run_id.is_some()
                || !evidence.provisional_event_ids.is_empty() =>
        {
            validation.push(
                "completed evidence cannot include provisional events".into(),
                "invalid",
            );
        }
        _ => {}
    }
    let mut event_ids = evidence.provisional_event_ids.clone();
    event_ids.sort();
    if event_ids.iter().any(|event_id| event_id.trim().is_empty())
        || event_ids.windows(2).any(|pair| pair[0] == pair[1])
    {
        validation.push(
            "provisional event IDs must be unique nonempty values".into(),
            "invalid",
        );
    }
    if event_ids != evidence.provisional_event_ids {
        validation.push("provisional event IDs are not sorted".into(), "invalid");
    }
}

pub(super) fn validate_against_policy(
    recommendation: &Recommendation,
    policy: &StoredPolicy,
    validation: &mut Validation,
) {
    let interval = &recommendation.interval_hours;
    if !policy.enabled {
        validation
            .reasons
            .push("Source recrawl policy is disabled".into());
    }
    if recommendation.expected_policy_version != policy.policy_version
        || !same_hours(interval.current, policy.current_interval_hours)
    {
        validation.push(
            "recommendation policy version or current interval is stale".into(),
            "stale",
        );
    }
    if interval.minimum < policy.minimum_interval_hours
        || interval.maximum > policy.maximum_interval_hours
        || interval.proposed.is_none_or(|proposed| {
            proposed < policy.minimum_interval_hours || proposed > policy.maximum_interval_hours
        })
    {
        validation.push(
            "recommendation exceeds the Source policy interval bounds".into(),
            "out_of_scope",
        );
    }
}

pub(super) fn parse_idempotency_basis(
    recommendation: &Recommendation,
) -> Result<RecommendationBasis, String> {
    let digest = recommendation
        .recommendation_id
        .strip_prefix("sha256:")
        .ok_or("recommendation ID has no sha256 prefix")?;
    if sha256_hex(recommendation.idempotency_basis.as_bytes()) != digest {
        return Err("idempotency_basis digest differs from recommendation ID".into());
    }
    let basis: RecommendationBasis = serde_json::from_str(&recommendation.idempotency_basis)
        .map_err(|error| format!("invalid idempotency_basis JSON: {error}"))?;
    let canonical = serde_json::to_string(&basis)
        .map_err(|error| format!("serialize parsed idempotency_basis: {error}"))?;
    if canonical != recommendation.idempotency_basis {
        return Err("idempotency_basis is not compact JSON in the documented field order".into());
    }
    if basis.schema_version != recommendation.schema_version
        || basis.generation_id != recommendation.generation_id
        || basis.source_id != recommendation.source_id
        || basis.generated_at_utc != recommendation.generated_at_utc
        || basis.expires_at_utc != recommendation.expires_at_utc
        || basis.expected_policy_version != recommendation.expected_policy_version
    {
        return Err("idempotency_basis identity does not match its envelope".into());
    }
    let interval = &recommendation.interval_hours;
    if basis.interval_hours.minimum != format_hours(interval.minimum)
        || basis.interval_hours.current != format_hours(interval.current)
        || basis.interval_hours.maximum != format_hours(interval.maximum)
        || basis.interval_hours.proposed != interval.proposed.map(format_hours).unwrap_or_default()
        || basis.interval_hours.adjustment != format_hours(interval.adjustment)
        || basis.interval_hours.adjustment_fraction
            != interval
                .adjustment_fraction
                .map(format_six_decimal)
                .unwrap_or_default()
    {
        return Err("idempotency_basis interval values differ from its envelope".into());
    }
    validate_recommendation_settings(&basis.recommendation_settings)?;

    let evidence = &recommendation.evidence;
    let mut snapshot_refs = evidence
        .snapshot_refs
        .iter()
        .map(|reference| BasisSnapshotReference {
            snapshot_id: reference.snapshot_id.clone(),
            source_snapshot_manifest_sha256: reference.source_snapshot_manifest_sha256.clone(),
            crawl_manifest_sha256: reference.crawl_manifest_sha256.clone(),
            observed_at_utc: reference.observed_at_utc.clone(),
        })
        .collect::<Vec<_>>();
    snapshot_refs.sort_by(|left, right| left.snapshot_id.cmp(&right.snapshot_id));
    let mut event_ids = evidence.provisional_event_ids.clone();
    event_ids.sort();
    let basis_evidence = &basis.evidence;
    let reference_ids = snapshot_refs
        .iter()
        .map(|reference| reference.snapshot_id.as_str())
        .collect::<Vec<_>>();
    let evidence_snapshot_ids = basis_evidence
        .snapshot_ids
        .split(',')
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();
    if evidence_snapshot_ids.len() > 5
        || evidence_snapshot_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || evidence_snapshot_ids
            .iter()
            .any(|id| !id.strip_prefix("snapshot-").is_some_and(is_hex_sha256))
    {
        return Err("idempotency_basis snapshot IDs are malformed or unsorted".into());
    }
    if basis_evidence.status != evidence.status
        || basis_evidence.snapshot_refs != snapshot_refs
        || (evidence.status == "completed_history" && evidence_snapshot_ids != reference_ids)
        || (evidence.status == "provisional" && !basis_evidence.snapshot_ids.is_empty())
        || basis_evidence.provisional_run_id != evidence.provisional_run_id.as_deref().unwrap_or("")
        || basis_evidence.provisional_event_ids != event_ids.join(",")
        || basis_evidence.scope_sha256 != evidence.scope_sha256.as_deref().unwrap_or("")
        || basis_evidence.extraction_contract_sha256
            != evidence.extraction_contract_sha256.as_deref().unwrap_or("")
        || basis_evidence.latest_observed_at_utc
            != evidence.latest_observed_at_utc.as_deref().unwrap_or("")
        || basis_evidence.latest_capture_event_at_utc
            != evidence
                .latest_capture_event_at_utc
                .as_deref()
                .unwrap_or("")
        || basis_evidence.latest_run_event_at_utc
            != evidence.latest_run_event_at_utc.as_deref().unwrap_or("")
        || basis_evidence.comparable_page_observations
            != evidence
                .comparable_page_observations
                .map(|value| value.to_string())
                .unwrap_or_default()
        || basis_evidence.changed_page_observations
            != evidence
                .changed_page_observations
                .map(|value| value.to_string())
                .unwrap_or_default()
        || basis_evidence.change_fraction
            != evidence
                .change_fraction
                .map(format_six_decimal)
                .unwrap_or_default()
    {
        return Err("idempotency_basis evidence values differ from its envelope".into());
    }
    Ok(basis)
}

fn validate_recommendation_settings(settings: &RecommendationSettings) -> Result<(), String> {
    let adjustment = settings
        .max_adjustment_fraction
        .parse::<f64>()
        .map_err(|_| "invalid max_adjustment_fraction in idempotency_basis")?;
    let minimum_snapshots = parse_canonical_i64(&settings.minimum_completed_snapshots)?;
    let minimum_observations = parse_canonical_i64(&settings.minimum_comparable_page_observations)?;
    let max_evidence_days = parse_canonical_i64(&settings.maximum_evidence_age_days)?;
    let max_provisional_seconds =
        parse_canonical_i64(&settings.maximum_provisional_event_age_seconds)?;
    if !adjustment.is_finite()
        || !(0.0..=0.5).contains(&adjustment)
        || settings.max_adjustment_fraction != format_six_decimal(adjustment)
        || !(3..=5).contains(&minimum_snapshots)
        || minimum_observations <= 0
        || !(1..=365).contains(&max_evidence_days)
        || !(1..=7_776_000).contains(&max_provisional_seconds)
        || settings.change_fraction_decrease_threshold != "0.250000"
        || settings.change_fraction_increase_threshold != "0.020000"
        || settings.decrease_target_multiplier != "0.500000"
        || settings.increase_target_multiplier != "1.500000"
        || settings.high_confidence_minimum_snapshots != "4"
        || settings.high_confidence_minimum_comparable_page_observations != "100"
    {
        return Err("unsupported or unsafe recommendation settings in idempotency_basis".into());
    }
    Ok(())
}

fn parse_canonical_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| "invalid integer in idempotency_basis")?;
    if parsed.to_string() != value {
        return Err("noncanonical integer in idempotency_basis".into());
    }
    Ok(parsed)
}

pub(super) fn parse_utc(value: &str, label: &str) -> Result<DateTime<Utc>, Error> {
    if !value.ends_with('Z') {
        return Err(Error::invalid(format!("{label} must end in Z")));
    }
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| Error::invalid(format!("invalid {label}: {error}")))
}

pub(super) fn parse_recommendation_utc(value: &str, label: &str) -> Result<DateTime<Utc>, Error> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
        || bytes.iter().enumerate().any(|(index, byte)| {
            !matches!(index, 4 | 7 | 10 | 13 | 16 | 19) && !byte.is_ascii_digit()
        })
    {
        return Err(Error::invalid(format!(
            "{label} must use second-precision YYYY-MM-DDTHH:mm:ssZ"
        )));
    }
    parse_utc(value, label)
}

pub(super) fn source_scope_sha256(source: &Source) -> Result<String, Error> {
    let mut scope = BTreeMap::<String, Value>::new();
    scope.insert("source_id".into(), json!(source.source_id));
    scope.insert("seeds".into(), sorted_strings(&source.seeds));
    scope.insert(
        "allowed_urls".into(),
        source.allowed_urls.as_ref().map_or(Value::Null, |urls| {
            sorted_strings(&urls.iter().cloned().collect::<Vec<_>>())
        }),
    );
    scope.insert(
        "allowed_origins".into(),
        sorted_strings(&source.allowed_origins),
    );
    scope.insert(
        "path_prefixes".into(),
        sorted_strings(&source.path_prefixes),
    );
    scope.insert(
        "deny_path_prefixes".into(),
        sorted_strings(&source.deny_path_prefixes),
    );
    scope.insert(
        "sitemap_url_prefixes".into(),
        sorted_strings(&source.sitemap_url_prefixes),
    );
    let bytes = serde_json::to_vec(&scope)
        .map_err(|error| Error::storage(format!("serialize Source scope hash: {error}")))?;
    Ok(sha256_hex(&bytes))
}

fn sorted_strings(values: &[String]) -> Value {
    let mut values = values.to_vec();
    values.sort();
    Value::Array(values.into_iter().map(Value::String).collect())
}

pub(super) fn extraction_contract_sha256(config: &Config) -> String {
    let contract = BTreeMap::from([
        ("min_content_chars", json!(config.min_content_chars)),
        ("max_decoded_body_bytes", json!(config.max_body_bytes)),
        ("max_extraction_bytes", json!(config.max_extraction_bytes)),
        ("max_extraction_millis", json!(config.max_extraction_millis)),
        ("extraction_schema", json!(EXTRACTION_SCHEMA)),
    ]);
    sha256_hex(
        serde_json::to_string(&contract)
            .unwrap_or_default()
            .as_bytes(),
    )
}
