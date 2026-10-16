//! Local storage guard for coordinator task assignment.
use crate::error::Error;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const DEFAULT_MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;
const SQLITE_WRITE_OVERHEAD_BYTES: u64 = 1024 * 1024;
const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
const REPORT_INTERVAL: Duration = Duration::from_secs(30);
pub(super) const RECHECK_INTERVAL: Duration = SAMPLE_INTERVAL;

pub(super) struct StoragePressure {
    crawl_root: PathBuf,
    export_root: PathBuf,
    archive_root: PathBuf,
    min_free_bytes: u64,
    quota_bytes: Option<u64>,
    max_body_bytes: u64,
}

pub(super) struct StoragePressureState {
    sample: Option<Result<DiskSample, String>>,
    sampled_at: Option<Instant>,
    paused: bool,
    reported_paused: Option<bool>,
    reported_at: Option<Instant>,
}

impl Default for StoragePressureState {
    fn default() -> Self {
        Self {
            sample: None,
            sampled_at: None,
            // A process restart loses the previous hysteresis state. Start paused
            // until a fresh sample proves the two-capture resume margin is present.
            paused: true,
            reported_paused: None,
            reported_at: None,
        }
    }
}

pub(super) struct ClaimCapacity {
    pub(super) max_new_claims: usize,
    pub(super) paused: bool,
    pub(super) report: Option<serde_json::Value>,
}

#[derive(Clone, Debug)]
struct DiskSample {
    crawl_spool_bytes: u64,
    event_export_bytes: u64,
    archive_bytes: u64,
    crawl_free_bytes: u64,
    export_free_bytes: u64,
    archive_free_bytes: u64,
    export_shares_crawl_fs: bool,
    archive_shares_crawl_fs: bool,
}

impl StoragePressure {
    pub(super) fn new(data_dir: &Path, max_body_bytes: usize) -> Result<Self, Error> {
        let min_free_bytes =
            env_bytes("SCOUT_COORDINATOR_MIN_FREE_BYTES")?.unwrap_or(DEFAULT_MIN_FREE_BYTES);
        let quota_bytes = env_bytes("SCOUT_COORDINATOR_STORAGE_QUOTA_BYTES")?;
        if quota_bytes == Some(0) {
            return Err(Error::invalid(
                "SCOUT_COORDINATOR_STORAGE_QUOTA_BYTES must be greater than zero",
            ));
        }
        Ok(Self {
            crawl_root: data_dir.join("crawls"),
            export_root: data_dir.join("crawl-event-batches"),
            archive_root: data_dir.join("analytics-archive"),
            min_free_bytes,
            quota_bytes,
            max_body_bytes: max_body_bytes as u64,
        })
    }

    pub(super) fn claim_capacity(
        &self,
        state: &mut StoragePressureState,
        live_leases: usize,
        result_jobs: usize,
        now: Instant,
    ) -> ClaimCapacity {
        let should_sample = state
            .sampled_at
            .is_none_or(|sampled| now.duration_since(sampled) >= SAMPLE_INTERVAL);
        if should_sample {
            state.sample = Some(self.sample());
            state.sampled_at = Some(now);
        }

        // A live task can overlap its body-file write with SQLite page and WAL writes. A job
        // whose lease has already committed only needs the SQLite projection and WAL reserve.
        let live_reserve = (live_leases as u64).saturating_mul(self.per_capture_reserve());
        let accepted_jobs = result_jobs.saturating_sub(live_leases) as u64;
        let accepted_reserve = accepted_jobs.saturating_mul(self.per_projection_reserve());
        let in_flight_reserve = live_reserve.saturating_add(accepted_reserve);
        let new_claim_reserve = self.per_capture_reserve();

        let (available, reason, report) = match state.sample.as_ref() {
            Some(Ok(sample)) => {
                let shared_usage = sample
                    .crawl_spool_bytes
                    .saturating_add(if sample.export_shares_crawl_fs {
                        sample.event_export_bytes
                    } else {
                        0
                    })
                    .saturating_add(if sample.archive_shares_crawl_fs {
                        sample.archive_bytes
                    } else {
                        0
                    });
                let free_budget = sample
                    .crawl_free_bytes
                    .saturating_sub(self.min_free_bytes.saturating_add(in_flight_reserve));
                let free_capacity = free_budget / new_claim_reserve;
                let quota_capacity = self.quota_bytes.map_or(u64::MAX, |quota| {
                    quota.saturating_sub(shared_usage.saturating_add(in_flight_reserve))
                        / new_claim_reserve
                });
                let available = free_capacity.min(quota_capacity);
                let reason = if free_capacity == 0 {
                    Some("filesystem_reserve")
                } else if quota_capacity == 0 {
                    Some("storage_quota")
                } else {
                    None
                };
                let report = json!({
                    "crawl_spool_bytes": sample.crawl_spool_bytes,
                    "event_export_bytes": sample.event_export_bytes,
                    "analytics_archive_bytes": sample.archive_bytes,
                    "analytics_archive_shares_crawl_filesystem": sample.archive_shares_crawl_fs,
                    "event_export_shares_crawl_filesystem": sample.export_shares_crawl_fs,
                    "crawl_filesystem_free_bytes": sample.crawl_free_bytes,
                    "event_export_filesystem_free_bytes": sample.export_free_bytes,
                    "analytics_archive_filesystem_free_bytes": sample.archive_free_bytes,
                    "shared_storage_bytes": shared_usage,
                    "storage_quota_bytes": self.quota_bytes,
                    "min_free_bytes": self.min_free_bytes,
                    "in_flight_reserve_bytes": in_flight_reserve,
                    "new_capture_reserve_bytes": new_claim_reserve,
                    "available_claim_slots": available,
                    "pressure_reason": reason,
                });
                (available, reason, report)
            }
            Some(Err(error)) => (
                0,
                Some("measurement_unavailable"),
                json!({
                    "crawl_spool_bytes": null,
                    "event_export_bytes": null,
                    "analytics_archive_bytes": null,
                    "crawl_filesystem_free_bytes": null,
                    "event_export_filesystem_free_bytes": null,
                    "analytics_archive_filesystem_free_bytes": null,
                    "pressure_reason": "measurement_unavailable",
                    "measurement_error": error,
                    "in_flight_reserve_bytes": in_flight_reserve,
                    "storage_quota_bytes": self.quota_bytes,
                    "min_free_bytes": self.min_free_bytes,
                    "available_claim_slots": 0,
                }),
            ),
            None => unreachable!("storage sample is initialized before use"),
        };

        if state.paused {
            // Require one extra capture of headroom before automatic resume.
            if available >= 2 {
                state.paused = false;
            }
        } else if available == 0 {
            state.paused = true;
        }
        let paused = state.paused;
        let emit_report = state.reported_paused != Some(paused)
            || state
                .reported_at
                .is_none_or(|reported| now.duration_since(reported) >= REPORT_INTERVAL);
        let report = emit_report.then(|| {
            state.reported_paused = Some(paused);
            state.reported_at = Some(now);
            let mut report = report;
            report["paused"] = json!(paused);
            if paused && reason.is_none() {
                report["pressure_reason"] = json!("resume_hysteresis");
            }
            report["resume_condition"] = json!(if paused {
                "free space and quota must allow at least two additional maximum-size captures"
            } else {
                "capacity available"
            });
            report
        });
        ClaimCapacity {
            max_new_claims: if paused {
                0
            } else {
                available.min(usize::MAX as u64) as usize
            },
            paused,
            report,
        }
    }

    fn per_capture_reserve(&self) -> u64 {
        self.max_body_bytes
            .saturating_mul(3)
            .saturating_add(SQLITE_WRITE_OVERHEAD_BYTES)
    }

    fn per_projection_reserve(&self) -> u64 {
        self.max_body_bytes
            .saturating_mul(2)
            .saturating_add(SQLITE_WRITE_OVERHEAD_BYTES)
    }

    fn sample(&self) -> Result<DiskSample, String> {
        let crawl_spool_bytes = directory_bytes(&self.crawl_root)?;
        let event_export_bytes = directory_bytes(&self.export_root)?;
        let archive_bytes = directory_bytes(&self.archive_root)?;
        let crawl_free_bytes = filesystem_free_bytes(&self.crawl_root)?;
        let export_free_bytes = filesystem_free_bytes(&self.export_root)?;
        let archive_free_bytes = filesystem_free_bytes(&self.archive_root)?;
        let export_shares_crawl_fs = same_filesystem(&self.crawl_root, &self.export_root)?;
        let archive_shares_crawl_fs = same_filesystem(&self.crawl_root, &self.archive_root)?;
        Ok(DiskSample {
            crawl_spool_bytes,
            event_export_bytes,
            archive_bytes,
            crawl_free_bytes,
            export_free_bytes,
            archive_free_bytes,
            export_shares_crawl_fs,
            archive_shares_crawl_fs,
        })
    }
}

fn env_bytes(name: &str) -> Result<Option<u64>, Error> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| Error::invalid(format!("{name} must be a non-negative integer")))
        })
        .transpose()
}

fn directory_bytes(root: &Path) -> Result<u64, String> {
    let mut total = 0u64;
    let mut pending = vec![root.to_path_buf()];
    #[cfg(unix)]
    let root_device = match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(format!(
                "managed storage root {} is a symlink",
                root.display()
            ));
        }
        Ok(metadata) if metadata.is_dir() => {
            use std::os::unix::fs::MetadataExt;
            Some(metadata.dev())
        }
        Ok(_) => {
            return Err(format!(
                "managed storage root {} is not a directory",
                root.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("stat {}: {error}", root.display())),
    };
    while let Some(path) = pending.pop() {
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("stat {}: {error}", path.display())),
        };
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "managed storage path {} is a symlink",
                path.display()
            ));
        }
        #[cfg(unix)]
        if root_device.is_some_and(|device| {
            use std::os::unix::fs::MetadataExt;
            metadata.dev() != device
        }) {
            return Err(format!(
                "managed storage path {} is on a nested filesystem",
                path.display()
            ));
        }
        if metadata.is_dir() {
            let entries =
                fs::read_dir(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
            for entry in entries {
                let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
                pending.push(entry.path());
            }
        } else {
            total = total.saturating_add(allocated_bytes(&metadata));
        }
    }
    Ok(total)
}

fn filesystem_free_bytes(path: &Path) -> Result<u64, String> {
    let existing = nearest_existing(path);
    let path = fs::canonicalize(&existing).unwrap_or(existing);
    let output = Command::new("df")
        .args(["-Pk"])
        .arg(&path)
        .output()
        .map_err(|error| format!("run df for {}: {error}", path.display()))?;
    if !output.status.success() {
        return Err(format!(
            "df failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| format!("df returned no rows for {}", path.display()))?;
    let available_kib = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| format!("df returned an invalid row for {}", path.display()))?
        .parse::<u64>()
        .map_err(|_| {
            format!(
                "df returned an invalid free-space count for {}",
                path.display()
            )
        })?;
    Ok(available_kib.saturating_mul(1024))
}

fn nearest_existing(path: &Path) -> PathBuf {
    let mut candidate = path;
    while !candidate.exists() {
        let Some(parent) = candidate.parent() else {
            return PathBuf::from(".");
        };
        candidate = parent;
    }
    candidate.to_path_buf()
}

#[cfg(unix)]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

#[cfg(unix)]
fn same_filesystem(left: &Path, right: &Path) -> Result<bool, String> {
    use std::os::unix::fs::MetadataExt;
    let left = nearest_existing(left);
    let right = nearest_existing(right);
    let left_device = fs::metadata(&left)
        .map_err(|error| format!("stat {}: {error}", left.display()))?
        .dev();
    let right_device = fs::metadata(&right)
        .map_err(|error| format!("stat {}: {error}", right.display()))?
        .dev();
    Ok(left_device == right_device)
}

#[cfg(not(unix))]
fn same_filesystem(_left: &Path, _right: &Path) -> Result<bool, String> {
    // The coordinator's supported deployment targets are Unix; fail closed elsewhere.
    Err("filesystem identity is unavailable on this platform".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn guard() -> StoragePressure {
        StoragePressure {
            crawl_root: PathBuf::new(),
            export_root: PathBuf::new(),
            archive_root: PathBuf::new(),
            min_free_bytes: 100,
            quota_bytes: None,
            max_body_bytes: 100,
        }
    }

    fn sample(free: u64) -> DiskSample {
        DiskSample {
            crawl_spool_bytes: 0,
            event_export_bytes: 0,
            archive_bytes: 0,
            crawl_free_bytes: free,
            export_free_bytes: free,
            archive_free_bytes: free,
            export_shares_crawl_fs: true,
            archive_shares_crawl_fs: true,
        }
    }

    #[test]
    fn reserves_active_capture_and_durable_projection_space() {
        let guard = guard();
        let mut state = StoragePressureState {
            sample: Some(Ok(sample(100 + 2 * guard.per_capture_reserve()))),
            sampled_at: Some(Instant::now()),
            paused: false,
            ..StoragePressureState::default()
        };
        let capacity = guard.claim_capacity(&mut state, 1, 0, Instant::now());
        assert_eq!(capacity.max_new_claims, 1);

        state.sample = Some(Ok(sample(100 + guard.per_capture_reserve())));
        let capacity = guard.claim_capacity(&mut state, 1, 1, Instant::now());
        assert!(capacity.paused);
        assert_eq!(capacity.max_new_claims, 0);
        assert_eq!(
            capacity.report.unwrap()["in_flight_reserve_bytes"],
            json!(guard.per_capture_reserve())
        );

        state.reported_paused = None;
        state.reported_at = None;
        state.sample = Some(Ok(sample(100 + guard.per_projection_reserve())));
        let capacity = guard.claim_capacity(&mut state, 0, 1, Instant::now());
        assert_eq!(
            capacity.report.unwrap()["in_flight_reserve_bytes"],
            json!(guard.per_projection_reserve())
        );
    }

    #[test]
    fn resumes_only_after_hysteresis_headroom_returns() {
        let guard = guard();
        let now = Instant::now();
        let mut state = StoragePressureState {
            sample: Some(Ok(sample(100))),
            sampled_at: Some(now),
            ..StoragePressureState::default()
        };
        assert!(guard.claim_capacity(&mut state, 0, 0, now).paused);

        state.sample = Some(Ok(sample(100 + guard.per_capture_reserve())));
        state.sampled_at = Some(now + Duration::from_secs(6));
        assert!(
            guard
                .claim_capacity(&mut state, 0, 0, now + Duration::from_secs(6))
                .paused
        );

        state.sample = Some(Ok(sample(100 + 2 * guard.per_capture_reserve())));
        state.sampled_at = Some(now + Duration::from_secs(12));
        let resumed = guard.claim_capacity(&mut state, 0, 0, now + Duration::from_secs(12));
        assert!(!resumed.paused);
        assert!(resumed.max_new_claims >= 2);
    }

    #[test]
    fn restart_at_one_capture_slot_stays_paused_until_two_slots_exist() {
        let guard = guard();
        let now = Instant::now();
        let mut state = StoragePressureState {
            sample: Some(Ok(sample(100 + guard.per_capture_reserve()))),
            sampled_at: Some(now),
            ..StoragePressureState::default()
        };

        let still_paused = guard.claim_capacity(&mut state, 0, 0, now);
        assert!(still_paused.paused);
        assert_eq!(still_paused.max_new_claims, 0);

        state.sample = Some(Ok(sample(100 + 2 * guard.per_capture_reserve())));
        state.sampled_at = Some(now + Duration::from_secs(6));
        let resumed = guard.claim_capacity(&mut state, 0, 0, now + Duration::from_secs(6));
        assert!(!resumed.paused);
        assert!(resumed.max_new_claims >= 2);
    }

    #[test]
    fn samples_spool_export_archive_and_filesystem_free_space() {
        let root = std::env::temp_dir().join(format!(
            "scout-storage-pressure-{}",
            crate::storage::new_run_id()
        ));
        let crawl_root = root.join("crawls");
        let export_root = root.join("crawl-event-batches");
        let archive_root = root.join("analytics-archive");
        fs::create_dir_all(&crawl_root).unwrap();
        fs::create_dir_all(&export_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        fs::write(crawl_root.join("capture.bin"), vec![1; 8192]).unwrap();
        fs::write(export_root.join("batch.jsonl"), vec![2; 4096]).unwrap();
        fs::write(archive_root.join("archive.jsonl"), vec![3; 2048]).unwrap();
        let guard = StoragePressure {
            crawl_root,
            export_root,
            archive_root,
            min_free_bytes: 0,
            quota_bytes: Some(100 * 1024 * 1024),
            max_body_bytes: 1024,
        };

        let sample = guard.sample().unwrap();
        let shared_bytes = sample
            .crawl_spool_bytes
            .saturating_add(sample.event_export_bytes)
            .saturating_add(sample.archive_bytes);
        assert!(sample.crawl_spool_bytes >= 8192);
        assert!(sample.event_export_bytes >= 4096);
        assert!(sample.archive_bytes >= 2048);
        assert!(sample.crawl_free_bytes > 0);
        assert!(sample.export_free_bytes > 0);
        assert!(sample.archive_free_bytes > 0);
        assert!(sample.export_shares_crawl_fs);
        assert!(sample.archive_shares_crawl_fs);

        let mut state = StoragePressureState {
            sample: Some(Ok(sample)),
            sampled_at: Some(Instant::now()),
            ..StoragePressureState::default()
        };
        let report = guard
            .claim_capacity(&mut state, 0, 0, Instant::now())
            .report
            .unwrap();
        assert!(report["crawl_spool_bytes"].as_u64().unwrap() >= 8192);
        assert!(report["event_export_bytes"].as_u64().unwrap() >= 4096);
        assert!(report["analytics_archive_bytes"].as_u64().unwrap() >= 2048);
        assert!(report["crawl_filesystem_free_bytes"].as_u64().unwrap() > 0);
        assert_eq!(report["shared_storage_bytes"].as_u64(), Some(shared_bytes));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn managed_storage_symlink_fails_closed() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "scout-storage-pressure-symlink-{}",
            crate::storage::new_run_id()
        ));
        let crawl_root = root.join("crawls");
        let export_root = root.join("crawl-event-batches");
        let archive_root = root.join("analytics-archive");
        fs::create_dir_all(&crawl_root).unwrap();
        fs::create_dir_all(&export_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        symlink(std::env::temp_dir(), crawl_root.join("linked-outside")).unwrap();
        let guard = StoragePressure {
            crawl_root,
            export_root,
            archive_root,
            min_free_bytes: 0,
            quota_bytes: None,
            max_body_bytes: 1,
        };

        assert!(guard.sample().unwrap_err().contains("symlink"));
        fs::remove_dir_all(root).unwrap();
    }
}
