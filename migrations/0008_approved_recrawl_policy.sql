CREATE TABLE scout_recrawl_control (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE scout_recrawl_policy_config_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action TEXT NOT NULL CHECK (action IN ('global_enabled', 'global_disabled', 'source_configured')),
    source_id TEXT,
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 256),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 4000),
    scope_sha256_before TEXT,
    scope_sha256_after TEXT,
    policy_version_before TEXT,
    policy_version_after TEXT,
    interval_hours_before NUMERIC(12,3),
    interval_hours_after NUMERIC(12,3),
    minimum_interval_hours_before NUMERIC(12,3),
    minimum_interval_hours_after NUMERIC(12,3),
    maximum_interval_hours_before NUMERIC(12,3),
    maximum_interval_hours_after NUMERIC(12,3),
    max_routine_interval_decrease_fraction_before NUMERIC(8,6),
    max_routine_interval_decrease_fraction_after NUMERIC(8,6),
    enabled_before BOOLEAN,
    enabled_after BOOLEAN,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (scope_sha256_before IS NULL OR scope_sha256_before ~ '^[a-f0-9]{64}$'),
    CHECK (scope_sha256_after IS NULL OR scope_sha256_after ~ '^[a-f0-9]{64}$')
);

INSERT INTO scout_recrawl_control(singleton, enabled)
VALUES (TRUE, FALSE)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE scout_source_recrawl_policy (
    source_id TEXT PRIMARY KEY,
    scope_sha256 TEXT NOT NULL CHECK (scope_sha256 ~ '^[a-f0-9]{64}$'),
    policy_version TEXT NOT NULL CHECK (length(policy_version) BETWEEN 1 AND 256),
    current_interval_hours NUMERIC(12,3) NOT NULL CHECK (current_interval_hours > 0),
    minimum_interval_hours NUMERIC(12,3) NOT NULL CHECK (minimum_interval_hours > 0),
    maximum_interval_hours NUMERIC(12,3) NOT NULL CHECK (maximum_interval_hours >= minimum_interval_hours),
    max_routine_interval_decrease_fraction NUMERIC(8,6) NOT NULL DEFAULT 0.200000
        CHECK (max_routine_interval_decrease_fraction >= 0 AND max_routine_interval_decrease_fraction < 1),
    next_due_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    claimed_run_id TEXT REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE SET NULL,
    claimed_at TIMESTAMPTZ,
    last_scheduled_run_id TEXT,
    last_scheduled_at TIMESTAMPTZ,
    last_schedule_outcome TEXT,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    updated_by TEXT NOT NULL CHECK (length(updated_by) BETWEEN 1 AND 256),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (minimum_interval_hours <= current_interval_hours),
    CHECK (current_interval_hours <= maximum_interval_hours)
);

CREATE TABLE scout_recrawl_recommendation_imports (
    import_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recommendation_id TEXT UNIQUE,
    idempotency_key TEXT UNIQUE,
    source_id TEXT,
    generation_id TEXT,
    payload_json TEXT NOT NULL,
    evidence_status TEXT,
    evidence_scope_sha256 TEXT,
    expected_policy_version TEXT,
    policy_version_at_import TEXT,
    current_interval_hours NUMERIC(12,3),
    proposed_interval_hours NUMERIC(12,3),
    validation_status TEXT NOT NULL CHECK (validation_status IN (
        'valid', 'provisional_review_required', 'unsupported', 'invalid', 'stale', 'out_of_scope'
    )),
    validation_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    outcome TEXT NOT NULL CHECK (outcome IN (
        'disabled', 'pending_review', 'ready_to_schedule', 'scheduled',
        'schedule_failed', 'rejected', 'invalid', 'stale', 'out_of_scope', 'unsupported'
    )),
    applied_policy_version TEXT,
    scheduling_outcome TEXT,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    CHECK (recommendation_id IS NULL OR recommendation_id ~ '^sha256:[a-f0-9]{64}$'),
    CHECK (idempotency_key IS NULL OR idempotency_key ~ '^sha256:[a-f0-9]{64}$'),
    CHECK (recommendation_id IS NULL OR idempotency_key IS NULL OR recommendation_id = idempotency_key)
);

CREATE INDEX scout_recrawl_imports_review_queue
    ON scout_recrawl_recommendation_imports(imported_at, import_id)
    WHERE outcome = 'pending_review';

CREATE TABLE scout_scheduled_run_sources (
    crawl_run_id TEXT NOT NULL REFERENCES scout_crawl_runs(crawl_run_id) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    scope_sha256 TEXT NOT NULL CHECK (scope_sha256 ~ '^[a-f0-9]{64}$'),
    interval_hours NUMERIC(12,3) NOT NULL CHECK (interval_hours > 0),
    work_completed BOOLEAN NOT NULL DEFAULT FALSE,
    completion_stop_reason TEXT,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (crawl_run_id, source_id)
);

CREATE TABLE scout_recrawl_policy_decisions (
    recommendation_id TEXT PRIMARY KEY
        REFERENCES scout_recrawl_recommendation_imports(recommendation_id),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 256),
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'denied')),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 4000),
    scope_sha256_before TEXT NOT NULL CHECK (scope_sha256_before ~ '^[a-f0-9]{64}$'),
    scope_sha256_after TEXT NOT NULL CHECK (scope_sha256_after ~ '^[a-f0-9]{64}$'),
    policy_version TEXT NOT NULL,
    interval_hours_before NUMERIC(12,3) NOT NULL CHECK (interval_hours_before > 0),
    interval_hours_after NUMERIC(12,3) NOT NULL CHECK (interval_hours_after > 0),
    decided_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE scout_recrawl_policy_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recommendation_id TEXT NOT NULL UNIQUE
        REFERENCES scout_recrawl_recommendation_imports(recommendation_id),
    source_id TEXT NOT NULL,
    actor TEXT,
    decision TEXT NOT NULL CHECK (decision IN ('automatic', 'approved', 'denied')),
    policy_version_before TEXT NOT NULL,
    policy_version_after TEXT NOT NULL,
    interval_hours_before NUMERIC(12,3) NOT NULL CHECK (interval_hours_before > 0),
    interval_hours_after NUMERIC(12,3) NOT NULL CHECK (interval_hours_after > 0),
    scope_sha256_before TEXT NOT NULL CHECK (scope_sha256_before ~ '^[a-f0-9]{64}$'),
    scope_sha256_after TEXT NOT NULL CHECK (scope_sha256_after ~ '^[a-f0-9]{64}$'),
    outcome TEXT NOT NULL CHECK (outcome IN ('scheduled', 'schedule_failed', 'denied')),
    outcome_detail TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
