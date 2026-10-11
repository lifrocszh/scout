use super::results::enqueue_bounded_discoveries;
use super::{ClaimedTask, DiscoveredTask, FrontierStore, SitemapCompletion, validate_digest};
use crate::error::Error;

impl FrontierStore {
    /// A stopped frontier cannot recover an abandoned optional scan. Keep live in-flight
    /// owners untouched; record every orphan instead of leaving an unexplained running row.
    pub(crate) fn finalize_abandoned_sitemap_discovery(
        &mut self,
        run_id: &str,
    ) -> Result<u64, Error> {
        let _measurement =
            crate::distributed::metrics::DatabaseScope::new("finalize_sitemap_discovery");
        self.client.execute(
            "WITH scheduler AS MATERIALIZED (SELECT crawl_run_id FROM scout_crawl_task_scheduler WHERE crawl_run_id=$1 FOR UPDATE)
             UPDATE scout_crawl_sitemap_discovery scan SET state='failed',error_reason='frontier/run stopped before discovery committed',updated_at=clock_timestamp()
             WHERE scan.crawl_run_id=$1 AND scan.state IN ('running','pending')
               AND EXISTS (SELECT 1 FROM scheduler)
               AND NOT EXISTS (SELECT 1 FROM scout_crawl_tasks owner WHERE owner.task_id=scan.owner_task_id AND owner.status='leased'
                   AND owner.lease_epoch=scan.owner_lease_epoch AND owner.lease_deadline>clock_timestamp())",
            &[&run_id],
        ).map_err(storage)
    }
    /// Claim the origin tree only while this page task has a valid renewable lease.
    pub(crate) fn claim_sitemap_discovery(
        &mut self,
        task: &ClaimedTask,
        max_attempts: i32,
    ) -> Result<Option<i64>, Error> {
        if max_attempts <= 0 {
            return Err(Error::invalid("sitemap attempts must be positive"));
        }
        let _measurement =
            crate::distributed::metrics::DatabaseScope::new("claim_sitemap_discovery");
        let mut tx = self.client.transaction().map_err(storage)?;
        lock_frontier(&mut tx, &task.crawl_run_id)?;
        if !lock_live_task(&mut tx, task)? {
            return Ok(None);
        }
        let inserted = tx.execute(
            "INSERT INTO scout_crawl_sitemap_discovery(crawl_run_id,origin,state,owner_task_id,owner_lease_epoch,scan_epoch,attempt_count,max_attempts)
             VALUES($1,$2,'running',$3,$4,1,1,$5) ON CONFLICT DO NOTHING",
            &[&task.crawl_run_id,&task.origin,&task.task_id,&task.lease_epoch,&max_attempts],
        ).map_err(storage)?;
        if inserted == 1 {
            tx.commit().map_err(storage)?;
            return Ok(Some(1));
        }
        let row = tx.query_one(
            "SELECT scan.state,scan.owner_task_id,scan.owner_lease_epoch,scan.scan_epoch,
                    scan.attempt_count,scan.max_attempts,
                    COALESCE(owner.status='leased' AND owner.lease_epoch=scan.owner_lease_epoch
                        AND owner.lease_deadline>clock_timestamp(),FALSE)
             FROM scout_crawl_sitemap_discovery scan LEFT JOIN scout_crawl_tasks owner ON owner.task_id=scan.owner_task_id
             WHERE scan.crawl_run_id=$1 AND scan.origin=$2 FOR UPDATE OF scan",
            &[&task.crawl_run_id,&task.origin],
        ).map_err(storage)?;
        if row.get::<_, i32>(5) != max_attempts {
            return Err(Error::storage("sitemap attempt policy changed"));
        }
        let state: String = row.get(0);
        let epoch: i64 = row.get(3);
        if state == "running" && row.get::<_, bool>(6) {
            let same_owner =
                row.get::<_, i64>(1) == task.task_id && row.get::<_, i64>(2) == task.lease_epoch;
            tx.commit().map_err(storage)?;
            return Ok(same_owner.then_some(epoch));
        }
        if state == "completed" || state == "failed" {
            tx.commit().map_err(storage)?;
            return Ok(None);
        }
        if row.get::<_, i32>(4) >= max_attempts {
            tx.execute("UPDATE scout_crawl_sitemap_discovery SET state='failed',error_reason='owner lease lost after final discovery attempt',updated_at=clock_timestamp() WHERE crawl_run_id=$1 AND origin=$2", &[&task.crawl_run_id,&task.origin]).map_err(storage)?;
            tx.commit().map_err(storage)?;
            return Ok(None);
        }
        let next_epoch = epoch
            .checked_add(1)
            .ok_or_else(|| Error::storage("sitemap epoch exhausted"))?;
        tx.execute("UPDATE scout_crawl_sitemap_discovery SET state='running',owner_task_id=$3,owner_lease_epoch=$4,scan_epoch=$5,attempt_count=attempt_count+1,result_sha256=NULL,accepted_discovered_count=0,rejected_discovered_count=0,error_reason='',updated_at=clock_timestamp() WHERE crawl_run_id=$1 AND origin=$2", &[&task.crawl_run_id,&task.origin,&task.task_id,&task.lease_epoch,&next_epoch]).map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(Some(next_epoch))
    }

    /// Persist eligible links and terminal/retry state in one transaction, independent of the page result.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_sitemap_discovery(
        &mut self,
        task: &ClaimedTask,
        origin: &str,
        epoch: i64,
        result_sha256: &str,
        discovered: &[DiscoveredTask],
        initially_rejected: usize,
        max_task_attempts: i32,
        max_frontier: usize,
        error_reason: &str,
        retryable: bool,
    ) -> Result<Option<SitemapCompletion>, Error> {
        validate_digest(result_sha256, "sitemap result digest")?;
        if origin != task.origin
            || epoch <= 0
            || max_task_attempts <= 0
            || max_frontier == 0
            || error_reason.len() > 2048
        {
            return Err(Error::invalid("invalid sitemap discovery completion"));
        }
        let _measurement =
            crate::distributed::metrics::DatabaseScope::new("commit_sitemap_discovery");
        let mut tx = self.client.transaction().map_err(storage)?;
        lock_frontier(&mut tx, &task.crawl_run_id)?;
        // Lock task before discovery, matching claims and capture completion lock order.
        let live = lock_live_task(&mut tx, task)?;
        let row = tx.query_opt("SELECT owner_task_id,owner_lease_epoch,scan_epoch,state,result_sha256,attempt_count,max_attempts,accepted_discovered_count,rejected_discovered_count FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1 AND origin=$2 FOR UPDATE", &[&task.crawl_run_id,&origin]).map_err(storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.get::<_, i64>(0) != task.task_id
            || row.get::<_, i64>(1) != task.lease_epoch
            || row.get::<_, i64>(2) != epoch
        {
            return Ok(None);
        }
        let state: String = row.get(3);
        if let Some(prior) = row.get::<_, Option<String>>(4) {
            if prior != result_sha256 {
                return Err(Error::invalid(
                    "sitemap epoch reused with conflicting result",
                ));
            }
            tx.commit().map_err(storage)?;
            return Ok(Some(SitemapCompletion {
                terminal: state != "pending",
                accepted_discovered_count: row.get::<_, i64>(7) as usize,
                rejected_discovered_count: row.get::<_, i64>(8) as usize,
            }));
        }
        if !live || state != "running" {
            return Ok(None);
        }
        let terminal =
            error_reason.is_empty() || !retryable || row.get::<_, i32>(5) >= row.get::<_, i32>(6);
        let next_state = if error_reason.is_empty() {
            "completed"
        } else if terminal {
            "failed"
        } else {
            "pending"
        };
        let (accepted, rejected, _) = enqueue_bounded_discoveries(
            &mut tx,
            &task.crawl_run_id,
            discovered,
            i64::try_from(initially_rejected)
                .map_err(|_| Error::invalid("discovery count too large"))?,
            max_task_attempts,
            i64::try_from(max_frontier).map_err(|_| Error::invalid("frontier too large"))?,
        )?;
        let changed = tx.execute("UPDATE scout_crawl_sitemap_discovery SET state=$3,result_sha256=$4,accepted_discovered_count=$5,rejected_discovered_count=$6,error_reason=$7,updated_at=clock_timestamp() WHERE crawl_run_id=$1 AND origin=$2 AND EXISTS (SELECT 1 FROM scout_crawl_tasks task WHERE task.task_id=$8 AND task.status='leased' AND task.lease_epoch=$9 AND task.lease_owner=$10 AND task.lease_deadline>clock_timestamp())", &[&task.crawl_run_id,&origin,&next_state,&result_sha256,&accepted,&rejected,&error_reason,&task.task_id,&task.lease_epoch,&task.lease_owner]).map_err(storage)?;
        if changed != 1 {
            return Ok(None);
        }
        tx.commit().map_err(storage)?;
        Ok(Some(SitemapCompletion {
            terminal,
            accepted_discovered_count: accepted as usize,
            rejected_discovered_count: rejected as usize,
        }))
    }
}

fn lock_frontier(tx: &mut postgres::Transaction<'_>, run_id: &str) -> Result<(), Error> {
    tx.query_opt(
        "SELECT crawl_run_id FROM scout_crawl_task_scheduler WHERE crawl_run_id=$1 FOR UPDATE",
        &[&run_id],
    )
    .map_err(storage)?
    .ok_or_else(|| Error::storage("crawl task scheduler is missing"))?;
    Ok(())
}
fn lock_live_task(tx: &mut postgres::Transaction<'_>, task: &ClaimedTask) -> Result<bool, Error> {
    let row = tx.query_opt("SELECT crawl_run_id,source_id,origin,normalized_url,schedule_key,idempotency_key,status,lease_owner,lease_epoch,COALESCE(lease_deadline>clock_timestamp(),FALSE) FROM scout_crawl_tasks WHERE task_id=$1 FOR UPDATE", &[&task.task_id]).map_err(storage)?;
    Ok(row.is_some_and(|row| {
        row.get::<_, String>(0) == task.crawl_run_id
            && row.get::<_, String>(1) == task.source_id
            && row.get::<_, String>(2) == task.origin
            && row.get::<_, String>(3) == task.normalized_url
            && row.get::<_, String>(4) == task.schedule_key
            && row.get::<_, String>(5) == task.idempotency_key
            && row.get::<_, String>(6) == "leased"
            && row.get::<_, Option<String>>(7).as_deref() == Some(&task.lease_owner)
            && row.get::<_, i64>(8) == task.lease_epoch
            && row.get::<_, bool>(9)
    }))
}
fn storage(error: postgres::Error) -> Error {
    Error::storage(format!("shared sitemap discovery: {error}"))
}
