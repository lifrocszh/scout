//! Shared run/origin discovery is durably committed before ordinary page fetching.
use super::*;

pub(super) async fn discover_origin(
    ctx: &WorkerContext,
    lease: &proto::Lease,
    origin: &str,
    roots: &[String],
) -> Result<(), Error> {
    if roots.is_empty() {
        return Ok(());
    }
    loop {
        let measurement = super::super::metrics::RpcScope::new("claim_sitemap_discovery");
        let claim = ctx
            .rpc
            .clone()
            .claim_sitemap_discovery(proto::SitemapDiscoveryRequest {
                protocol_version: PROTOCOL_VERSION,
                lease: Some(lease.clone()),
                origin: origin.to_owned(),
            })
            .await
            .map_err(|error| Error::storage(format!("claim sitemap discovery: {error}")))?
            .into_inner();
        drop(measurement);
        if claim.protocol_version != PROTOCOL_VERSION {
            return Err(Error::invalid("sitemap discovery protocol changed"));
        }
        if !claim.claimed {
            return Ok(());
        }
        let mut discovered = BTreeSet::new();
        let mut visited = HashSet::new();
        let mut errors = Vec::new();
        let mut retryable = false;
        for root in roots {
            if let Err(error) =
                discover_sitemap(ctx, lease, root, 0, &mut discovered, &mut visited).await
            {
                // Coordinator/storage failures remain fatal. HTTP and optional discovery limits
                // produce durable bounded scan outcomes without discarding the ordinary page.
                if error.code == "storage_failed" || error.code == "invalid_configuration" {
                    return Err(error);
                }
                retryable |= error.code == "fetch_failed";
                errors.push(error.message);
            }
        }
        let error_reason = errors
            .join("; ")
            .char_indices()
            .take_while(|(index, character)| index + character.len_utf8() <= 2048)
            .map(|(_, character)| character)
            .collect::<String>();
        let measurement = super::super::metrics::RpcScope::new("commit_sitemap_discovery");
        let committed = ctx
            .rpc
            .clone()
            .commit_sitemap_discovery(proto::CommitSitemapDiscoveryRequest {
                protocol_version: PROTOCOL_VERSION,
                lease: Some(lease.clone()),
                origin: origin.to_owned(),
                scan_epoch: claim.scan_epoch,
                discovered_urls: discovered.into_iter().collect(),
                error_reason,
                retryable,
            })
            .await
            .map_err(|error| Error::storage(format!("commit sitemap discovery: {error}")))?
            .into_inner();
        drop(measurement);
        if committed.protocol_version != PROTOCOL_VERSION || !committed.committed {
            return Err(Error::safety("sitemap discovery lease became stale"));
        }
        if committed.terminal {
            return Ok(());
        }
        // Finish bounded retries under this renewable page lease even when peers have drained
        // the frontier. Another page worker may take the pending retry meanwhile.
        tokio::time::sleep(Duration::from_millis(retry_delay_ms(1, &ctx.config))).await;
    }
}

async fn discover_sitemap(
    ctx: &WorkerContext,
    lease: &proto::Lease,
    value: &str,
    depth: usize,
    discovered: &mut BTreeSet<String>,
    visited: &mut HashSet<String>,
) -> Result<(), Error> {
    let Some(normalized) = normalize_sitemap_url(value) else {
        return Ok(());
    };
    if !sitemap_url_allowed_worker(ctx, &normalized) || visited.contains(&normalized) {
        return Ok(());
    }
    if depth > 5
        || discovered.len() >= ctx.config.max_frontier
        || visited.len() >= ctx.config.max_frontier
    {
        return Err(Error::safety(
            "sitemap discovery depth/frontier limit reached",
        ));
    }
    visited.insert(normalized.clone());
    let robots = ensure_robots(ctx, lease, &normalized).await?;
    if !robots.allowed {
        return Err(Error::safety("sitemap robots denied"));
    }
    let response = request(
        ctx,
        lease,
        &normalized,
        "sitemap",
        ctx.config.max_body_bytes,
    )
    .await?;
    if !(200..300).contains(&response.status) {
        return Err(Error::safety(format!("sitemap HTTP {}", response.status)));
    }
    let xml = String::from_utf8_lossy(&response.body).into_owned();
    let (is_index, locations) = tokio::task::spawn_blocking(move || {
        (is_sitemap_index(&xml), extract_sitemap_locations(&xml))
    })
    .await
    .map_err(|error| Error::storage(format!("parse sitemap response in worker: {error}")))?;
    let mut failure = None;
    for location in locations {
        if is_index {
            if let Err(error) = Box::pin(discover_sitemap(
                ctx,
                lease,
                &location,
                depth + 1,
                discovered,
                visited,
            ))
            .await
            {
                if error.code == "storage_failed" || error.code == "invalid_configuration" {
                    return Err(error);
                }
                // Continue siblings; keep every successfully parsed link for atomic publication.
                if failure
                    .as_ref()
                    .is_none_or(|prior: &Error| prior.code != "fetch_failed")
                {
                    failure = Some(error);
                }
            }
        } else if let Some(page) = normalize_sitemap_url(&location) {
            discovered.insert(page);
        }
        if discovered.len() >= ctx.config.max_frontier {
            break;
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
