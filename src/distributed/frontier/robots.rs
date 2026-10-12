use super::{FrontierStore, RobotsDecision, validate_digest, validate_request_identity};
use crate::crawl::RobotsPolicy;
use crate::error::Error;
use crate::url::{normalize_origin, normalize_url};
use ::url::Url;
use std::time::Duration;

impl FrontierStore {
    /// Return a fresh shared decision or claim the one allowed robots refresh for this origin.
    #[cfg(test)]
    pub(crate) fn robots_decision(
        &mut self,
        run_id: &str,
        worker_id: &str,
        refresh_epoch: Option<i64>,
        origin: &str,
        path: &str,
    ) -> Result<RobotsDecision, Error> {
        self.robots_decision_inner(None, run_id, worker_id, refresh_epoch, origin, path)
    }

    pub(crate) fn robots_decision_for_task(
        &mut self,
        task: &super::ClaimedTask,
        refresh_epoch: Option<i64>,
        origin: &str,
        path: &str,
    ) -> Result<RobotsDecision, Error> {
        self.robots_decision_inner(
            Some(task),
            &task.crawl_run_id,
            &task.lease_owner,
            refresh_epoch,
            origin,
            path,
        )
    }

    fn robots_decision_inner(
        &mut self,
        task: Option<&super::ClaimedTask>,
        run_id: &str,
        worker_id: &str,
        refresh_epoch: Option<i64>,
        origin: &str,
        path: &str,
    ) -> Result<RobotsDecision, Error> {
        validate_request_identity(worker_id, "worker_id")?;
        let normalized_origin = normalize_origin(origin)?;
        if normalized_origin != origin || !path.starts_with('/') {
            return Err(Error::invalid("robots origin or path is not normalized"));
        }
        let _measurement = crate::distributed::metrics::DatabaseScope::new("robots_decision");
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin robots policy transaction: {error}")))?;
        let settings = tx
            .query_opt(
                "SELECT request_timeout_ms, permit_safety_margin_ms
                 FROM scout_crawl_request_policy WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("read robots policy settings: {error}")))?
            .ok_or_else(|| Error::invalid(format!("crawl run {run_id:?} has no request policy")))?;
        let lease_ms: i64 = settings
            .get::<_, i64>(0)
            .checked_add(settings.get::<_, i64>(1))
            .ok_or_else(|| Error::invalid("robots refresh lease is too large"))?;
        let inserted = tx
            .execute(
                "INSERT INTO scout_crawl_robots_policy(
                 crawl_run_id, origin, state, policy, refresh_owner, refresh_epoch, refresh_deadline
             ) VALUES ($1, $2, 'refreshing', '{\"groups\":[],\"sitemaps\":[]}', $3, 1,
                       clock_timestamp() + ($4::BIGINT * interval '1 millisecond'))
             ON CONFLICT (crawl_run_id, origin) DO NOTHING",
                &[&run_id, &origin, &worker_id, &lease_ms],
            )
            .map_err(|error| Error::storage(format!("create robots refresh state: {error}")))?
            == 1;
        if inserted {
            if let Some(task) = task {
                if !super::lock_live_task(&mut tx, task)? {
                    return Err(Error::stale_lease("task lease is stale or unknown"));
                }
            }
            tx.commit()
                .map_err(|error| Error::storage(format!("commit robots refresh claim: {error}")))?;
            return Ok(RobotsDecision::Refresh { epoch: 1 });
        }
        let row = tx
            .query_one(
                "SELECT state, policy, body_sha256, refresh_owner, refresh_epoch
                 FROM scout_crawl_robots_policy
                 WHERE crawl_run_id = $1 AND origin = $2 FOR UPDATE",
                &[&run_id, &origin],
            )
            .map_err(|error| Error::storage(format!("lock robots policy state: {error}")))?;
        if let Some(task) = task {
            if !super::lock_live_task(&mut tx, task)? {
                return Err(Error::stale_lease("task lease is stale or unknown"));
            }
        }
        // These deadlines can expire while either row lock is being acquired. Read clock time
        // only after the robots row and, for task RPCs, its lease row are both locked.
        let freshness = tx
            .query_one(
                "SELECT COALESCE(refresh_deadline > clock_timestamp(), FALSE),
                        COALESCE(expires_at > clock_timestamp(), FALSE)
                 FROM scout_crawl_robots_policy WHERE crawl_run_id=$1 AND origin=$2",
                &[&run_id, &origin],
            )
            .map_err(|error| Error::storage(format!("recheck robots policy deadlines: {error}")))?;
        let refresh_live: bool = freshness.get(0);
        let cache_fresh: bool = freshness.get(1);
        let state: String = row.get(0);
        match state.as_str() {
            "ready" | "denied" if cache_fresh => {
                let allowed = if state == "denied" {
                    false
                } else {
                    let serialized: String = row.get(1);
                    let policy: RobotsPolicy =
                        serde_json::from_str(&serialized).map_err(|error| {
                            Error::storage(format!("decode shared robots policy: {error}"))
                        })?;
                    policy.allows(path)
                };
                let sitemaps = if state == "ready" {
                    let serialized: String = row.get(1);
                    let policy: RobotsPolicy =
                        serde_json::from_str(&serialized).map_err(|error| {
                            Error::storage(format!("decode shared robots policy: {error}"))
                        })?;
                    policy.sitemaps
                } else {
                    Vec::new()
                };
                let decision = RobotsDecision::Cached {
                    allowed,
                    body_sha256: row.get(2),
                    sitemaps,
                };
                tx.commit()
                    .map_err(|error| Error::storage(format!("commit robots decision: {error}")))?;
                Ok(decision)
            }
            "refreshing" if refresh_live => {
                let owner: String = row.get(3);
                let epoch: i64 = row.get(4);
                if owner == worker_id && refresh_epoch == Some(epoch) {
                    tx.execute(
                        "UPDATE scout_crawl_robots_policy
                         SET refresh_deadline = clock_timestamp() + ($4::BIGINT * interval '1 millisecond')
                         WHERE crawl_run_id = $1 AND origin = $2 AND refresh_epoch = $3",
                        &[&run_id, &origin, &epoch, &lease_ms],
                    )
                    .map_err(|error| Error::storage(format!("renew robots refresh claim: {error}")))?;
                    tx.commit().map_err(|error| {
                        Error::storage(format!("commit robots refresh claim: {error}"))
                    })?;
                    Ok(RobotsDecision::Refresh { epoch })
                } else {
                    tx.commit().map_err(|error| {
                        Error::storage(format!("commit robots refresh wait: {error}"))
                    })?;
                    Ok(RobotsDecision::Waiting(Duration::from_millis(25)))
                }
            }
            "ready" | "denied" | "refreshing" => {
                let epoch: i64 = row.get(4);
                let next_epoch = epoch
                    .checked_add(1)
                    .ok_or_else(|| Error::storage("robots refresh epoch exhausted"))?;
                tx.execute(
                    "UPDATE scout_crawl_robots_policy
                     SET state = 'refreshing', refresh_owner = $3, refresh_epoch = $4,
                         refresh_deadline = clock_timestamp() + ($5::BIGINT * interval '1 millisecond'),
                         fetched_at = NULL, expires_at = NULL
                     WHERE crawl_run_id = $1 AND origin = $2",
                    &[&run_id, &origin, &worker_id, &next_epoch, &lease_ms],
                )
                .map_err(|error| Error::storage(format!("claim robots refresh: {error}")))?;
                tx.commit().map_err(|error| {
                    Error::storage(format!("commit robots refresh claim: {error}"))
                })?;
                Ok(RobotsDecision::Refresh { epoch: next_epoch })
            }
            other => Err(Error::storage(format!(
                "unknown robots policy state from PostgreSQL: {other}"
            ))),
        }
    }

    #[cfg(test)]
    pub(crate) fn commit_robots_refresh(
        &mut self,
        run_id: &str,
        worker_id: &str,
        origin: &str,
        epoch: i64,
        policy: &RobotsPolicy,
        body_sha256: &str,
    ) -> Result<bool, Error> {
        self.commit_robots_refresh_inner(
            None,
            run_id,
            worker_id,
            origin,
            epoch,
            policy,
            body_sha256,
        )
    }

    pub(crate) fn commit_robots_refresh_for_task(
        &mut self,
        task: &super::ClaimedTask,
        origin: &str,
        epoch: i64,
        policy: &RobotsPolicy,
        body_sha256: &str,
    ) -> Result<bool, Error> {
        self.commit_robots_refresh_inner(
            Some(task),
            &task.crawl_run_id,
            &task.lease_owner,
            origin,
            epoch,
            policy,
            body_sha256,
        )
    }

    fn commit_robots_refresh_inner(
        &mut self,
        task: Option<&super::ClaimedTask>,
        run_id: &str,
        worker_id: &str,
        origin: &str,
        epoch: i64,
        policy: &RobotsPolicy,
        body_sha256: &str,
    ) -> Result<bool, Error> {
        let normalized_origin = normalize_origin(origin)?;
        if normalized_origin != origin || epoch <= 0 {
            return Err(Error::invalid("robots refresh identity is invalid"));
        }
        validate_digest(body_sha256, "robots body digest")?;
        let mut policy = policy.clone();
        policy.sitemaps = policy
            .sitemaps
            .into_iter()
            .map(|url| {
                let parsed = Url::parse(&url)
                    .map_err(|error| Error::invalid(format!("invalid sitemap URL: {error}")))?;
                normalize_url(parsed).map_err(Error::invalid)
            })
            .collect::<Result<Vec<_>, _>>()?;
        policy.sitemaps.sort();
        policy.sitemaps.dedup();
        let serialized = serde_json::to_string(&policy)
            .map_err(|error| Error::storage(format!("encode shared robots policy: {error}")))?;
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin robots refresh commit: {error}")))?;
        if let Some(task) = task {
            if task.crawl_run_id != run_id || task.lease_owner != worker_id {
                return Err(Error::stale_lease("task lease is stale or unknown"));
            }
            tx.query_opt(
                "SELECT crawl_run_id FROM scout_crawl_robots_policy
                 WHERE crawl_run_id=$1 AND origin=$2 FOR UPDATE",
                &[&run_id, &origin],
            )
            .map_err(|error| Error::storage(format!("lock robots refresh state: {error}")))?;
            if !super::lock_live_task(&mut tx, task)? {
                return Err(Error::stale_lease("task lease is stale or unknown"));
            }
        }
        let changed = tx
            .execute(
                "UPDATE scout_crawl_robots_policy AS robots
                 SET state = 'ready', policy = $5, body_sha256 = $6,
                     fetched_at = clock_timestamp(),
                     expires_at = clock_timestamp() + (settings.robots_cache_ms * interval '1 millisecond'),
                     refresh_owner = NULL, refresh_deadline = NULL
                 FROM scout_crawl_request_policy AS settings
                 WHERE robots.crawl_run_id = $1 AND robots.origin = $2
                   AND robots.refresh_owner = $3 AND robots.refresh_epoch = $4
                   AND robots.state = 'refreshing' AND robots.refresh_deadline > clock_timestamp()
                   AND settings.crawl_run_id = robots.crawl_run_id",
                &[&run_id, &origin, &worker_id, &epoch, &serialized, &body_sha256],
            )
            .map_err(|error| Error::storage(format!("commit shared robots policy: {error}")))?;
        tx.commit().map_err(|error| {
            Error::storage(format!("commit robots refresh transaction: {error}"))
        })?;
        Ok(changed == 1)
    }

    #[cfg(test)]
    pub(crate) fn fail_robots_refresh(
        &mut self,
        run_id: &str,
        worker_id: &str,
        origin: &str,
        epoch: i64,
    ) -> Result<bool, Error> {
        self.fail_robots_refresh_inner(None, run_id, worker_id, origin, epoch)
    }

    pub(crate) fn fail_robots_refresh_for_task(
        &mut self,
        task: &super::ClaimedTask,
        origin: &str,
        epoch: i64,
    ) -> Result<bool, Error> {
        self.fail_robots_refresh_inner(
            Some(task),
            &task.crawl_run_id,
            &task.lease_owner,
            origin,
            epoch,
        )
    }

    fn fail_robots_refresh_inner(
        &mut self,
        task: Option<&super::ClaimedTask>,
        run_id: &str,
        worker_id: &str,
        origin: &str,
        epoch: i64,
    ) -> Result<bool, Error> {
        let normalized_origin = normalize_origin(origin)?;
        if normalized_origin != origin || epoch <= 0 {
            return Err(Error::invalid("robots refresh identity is invalid"));
        }
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin robots refresh failure: {error}")))?;
        if let Some(task) = task {
            if task.crawl_run_id != run_id || task.lease_owner != worker_id {
                return Err(Error::stale_lease("task lease is stale or unknown"));
            }
            tx.query_opt(
                "SELECT crawl_run_id FROM scout_crawl_robots_policy
                 WHERE crawl_run_id=$1 AND origin=$2 FOR UPDATE",
                &[&run_id, &origin],
            )
            .map_err(|error| Error::storage(format!("lock robots refresh state: {error}")))?;
            if !super::lock_live_task(&mut tx, task)? {
                return Err(Error::stale_lease("task lease is stale or unknown"));
            }
        }
        let changed = tx
            .execute(
                "UPDATE scout_crawl_robots_policy AS robots
                 SET state = 'denied', policy = '{\"groups\":[],\"sitemaps\":[]}',
                     body_sha256 = NULL, fetched_at = clock_timestamp(),
                     expires_at = clock_timestamp() + (settings.robots_cache_ms * interval '1 millisecond'),
                     refresh_owner = NULL, refresh_deadline = NULL
                 FROM scout_crawl_request_policy AS settings
                 WHERE robots.crawl_run_id = $1 AND robots.origin = $2
                   AND robots.refresh_owner = $3 AND robots.refresh_epoch = $4
                   AND robots.state = 'refreshing' AND robots.refresh_deadline > clock_timestamp()
                   AND settings.crawl_run_id = robots.crawl_run_id",
                &[&run_id, &origin, &worker_id, &epoch],
            )
            .map_err(|error| Error::storage(format!("fail shared robots refresh: {error}")))?;
        tx.commit().map_err(|error| {
            Error::storage(format!("commit robots failure transaction: {error}"))
        })?;
        Ok(changed == 1)
    }
}
