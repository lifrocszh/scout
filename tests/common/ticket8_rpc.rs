use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const VERSION: u32 = 3;
pub const RESULT_PREFIX: &str = "TICKET8_RPC_RESULT:";

pub mod proto {
    tonic::include_proto!("scout.distributed.v1");
}

use proto::coordinator_client::CoordinatorClient;

#[path = "ticket8_rpc_child.rs"]
mod ticket8_rpc_child;

#[derive(Clone)]
pub struct SourceSpec {
    pub id: String,
    pub seeds: Vec<String>,
    pub group: Option<String>,
}

pub struct Run {
    pub child: Child,
    pub database: Client,
    pub database_url: String,
    pub dir: PathBuf,
    pub id: String,
    pub address: SocketAddr,
    pub coordinator_app_name: String,
}

impl Run {
    pub fn new(
        label: &str,
        sources: &[SourceSpec],
        global: usize,
        origin_limit: usize,
        spacing_ms: u64,
        attempts: usize,
    ) -> Self {
        Self::new_with_origin_overrides(
            label,
            sources,
            global,
            origin_limit,
            spacing_ms,
            attempts,
            &[],
        )
    }

    pub fn new_with_origin_overrides(
        label: &str,
        sources: &[SourceSpec],
        global: usize,
        origin_limit: usize,
        spacing_ms: u64,
        attempts: usize,
        overrides: &[(String, usize, u64)],
    ) -> Self {
        let raw_database_url = std::env::var("SCOUT_TEST_DATABASE_URL").expect(
            "set SCOUT_TEST_DATABASE_URL to the dedicated disposable Ticket 8 test database",
        );
        let (database_url, coordinator_app_name) = coordinator_database_url(&raw_database_url);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let id = format!(
            "run-{:x}",
            Sha256::digest(format!("{label}-{}-{nonce}", std::process::id()).as_bytes())
        );
        let dir = std::env::temp_dir().join(format!(
            "scout-ticket8-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let mut config = format!(
            "schema_version = 'scout.sources.v1'\ncontact_url = 'https://example.invalid/scout'\nuser_agent = 'Scout-Ticket8-Test/1 (+https://example.invalid/scout)'\npage_target = 500\nmin_content_chars = 20\nmax_frontier = 64\nglobal_concurrency = {global}\norigin_concurrency = {origin_limit}\nmin_start_spacing_ms = {spacing_ms}\nrequest_timeout_seconds = 5\nmax_attempts = {attempts}\nretry_backoff_ms = 1\nretry_max_delay_ms = 1\nmax_total_attempts = 100\nmax_duration_seconds = 60\nrobots_cache_seconds = 60\n"
        );
        for source in sources {
            assert!(!source.seeds.is_empty());
            let origins = source
                .seeds
                .iter()
                .map(|seed| origin(seed))
                .collect::<Vec<_>>();
            let origins = origins
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            config.push_str("\n[[sources]]\n");
            config.push_str(&format!("source_id = {}\n", toml_string(&source.id)));
            config.push_str(&format!(
                "seeds = [{}]\n",
                source
                    .seeds
                    .iter()
                    .map(|s| toml_string(s))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            config.push_str(&format!(
                "allowed_origins = [{}]\n",
                origins
                    .iter()
                    .map(|s| toml_string(s))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            config.push_str("path_prefixes = ['/']\n");
            if let Some(group) = &source.group {
                config.push_str(&format!("politeness_group = {}\n", toml_string(group)));
            }
        }
        for (override_origin, concurrency, spacing) in overrides {
            config.push_str(&format!("\n[[origins]]\norigin = {}\norigin_concurrency = {concurrency}\nmin_start_spacing_ms = {spacing}\n", toml_string(override_origin)));
        }
        fs::write(dir.join("sources.toml"), config).unwrap();
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_scout"))
            .args([
                "coordinator",
                "--config",
                dir.join("sources.toml").to_str().unwrap(),
                "--data-dir",
                dir.to_str().unwrap(),
                "--bind",
                &address.to_string(),
                "--run-id",
                &id,
            ])
            .env("SCOUT_DATABASE_URL", &database_url)
            .env("SCOUT_COORDINATOR_DB_LANES", "4")
            .env("SCOUT_COMPLETION_LANES", "4")
            .stdout(Stdio::from(
                fs::File::create(dir.join("coordinator.stdout")).unwrap(),
            ))
            .stderr(Stdio::from(
                fs::File::create(dir.join("coordinator.stderr")).unwrap(),
            ))
            .spawn()
            .unwrap();
        let mut run = Self {
            child,
            database: connect_named(&database_url, "scout-ticket8-test-observer"),
            database_url,
            dir,
            id,
            address,
            coordinator_app_name,
        };
        run.wait_ready();
        run
    }

    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(self.address).is_err() {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "coordinator exited: {}",
                fs::read_to_string(self.dir.join("coordinator.stderr")).unwrap_or_default()
            );
            assert!(Instant::now() < deadline, "coordinator startup timed out");
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn client(
        &self,
    ) -> (
        tokio::runtime::Runtime,
        CoordinatorClient<tonic::transport::Channel>,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(4),
                CoordinatorClient::connect(format!("http://{}", self.address)),
            )
            .await
            .unwrap()
            .unwrap()
        });
        (runtime, client)
    }

    pub fn register(
        &self,
        runtime: &tokio::runtime::Runtime,
        client: &mut CoordinatorClient<tonic::transport::Channel>,
        worker: &str,
    ) {
        let response = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(4),
                client.register_worker(proto::RegisterWorkerRequest {
                    worker_id: worker.into(),
                    supported_protocol_versions: vec![VERSION],
                }),
            )
            .await
            .unwrap()
            .unwrap()
            .into_inner()
        });
        assert_eq!(response.protocol_version, VERSION);
        assert_eq!(response.crawl_run_id, self.id);
    }

    pub fn claim(
        &self,
        runtime: &tokio::runtime::Runtime,
        client: &mut CoordinatorClient<tonic::transport::Channel>,
        worker: &str,
    ) -> Option<proto::Lease> {
        self.register(runtime, client, worker);
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(4),
                client.claim_one(proto::ClaimOneRequest {
                    protocol_version: VERSION,
                    worker_id: worker.into(),
                }),
            )
            .await
            .unwrap()
            .unwrap()
            .into_inner()
            .lease
        })
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = self.database.execute(
            "DELETE FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&self.id],
        );
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub fn connect_named(database_url: &str, application_name: &str) -> Client {
    let mut config = database_url.parse::<postgres::Config>().unwrap();
    config.application_name(application_name);
    config.connect(NoTls).unwrap()
}

fn coordinator_database_url(raw: &str) -> (String, String) {
    if let Ok(mut parsed) = url::Url::parse(raw) {
        if !matches!(parsed.scheme(), "postgres" | "postgresql") {
            panic!("SCOUT_TEST_DATABASE_URL must use postgres:// or postgresql://");
        }
        let existing = parsed
            .query_pairs()
            .find(|(key, _)| key == "application_name")
            .map(|(_, value)| value.into_owned());
        let name = existing.unwrap_or_else(|| "scout-ticket8-coordinator-test".into());
        if !parsed
            .query_pairs()
            .any(|(key, _)| key == "application_name")
        {
            parsed
                .query_pairs_mut()
                .append_pair("application_name", &name);
        }
        return (parsed.to_string(), name);
    }
    // libpq-style keyword connection strings default to the driver's `postgres` application name.
    (raw.into(), "postgres".into())
}

pub fn origin(url: &str) -> String {
    let parsed = url::Url::parse(url).unwrap();
    format!(
        "{}://{}{}",
        parsed.scheme(),
        parsed.host_str().unwrap(),
        parsed.port().map(|p| format!(":{p}")).unwrap_or_default()
    )
}

pub fn local_origin() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("http://{}", listener.local_addr().unwrap())
}

pub fn url(origin: &str, path: &str) -> String {
    format!("{}{path}", origin.trim_end_matches('/'))
}

pub fn lease_json(lease: &proto::Lease) -> String {
    serde_json::json!({
        "task_id": lease.task_id, "crawl_run_id": lease.crawl_run_id, "source_id": lease.source_id,
        "origin": lease.origin, "normalized_url": lease.normalized_url, "schedule_key": lease.schedule_key,
        "task_attempt_count": lease.task_attempt_count, "lease_owner": lease.lease_owner,
        "lease_epoch": lease.lease_epoch, "lease_deadline_unix_ms": lease.lease_deadline_unix_ms,
        "idempotency_key": lease.idempotency_key,
    }).to_string()
}

pub fn parse_result(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "RPC child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find(|line| line.starts_with(RESULT_PREFIX))
        .expect("RPC child result line");
    serde_json::from_str(&line[RESULT_PREFIX.len()..]).unwrap()
}

pub fn parse_lease(value: &serde_json::Value) -> proto::Lease {
    let encoded = value["lease"].as_str().expect("worker received a lease");
    let v: serde_json::Value = serde_json::from_str(encoded).unwrap();
    proto::Lease {
        task_id: v["task_id"].as_i64().unwrap(),
        crawl_run_id: v["crawl_run_id"].as_str().unwrap().into(),
        source_id: v["source_id"].as_str().unwrap().into(),
        origin: v["origin"].as_str().unwrap().into(),
        normalized_url: v["normalized_url"].as_str().unwrap().into(),
        schedule_key: v["schedule_key"].as_str().unwrap().into(),
        task_attempt_count: v["task_attempt_count"].as_i64().unwrap() as i32,
        lease_owner: v["lease_owner"].as_str().unwrap().into(),
        lease_epoch: v["lease_epoch"].as_i64().unwrap(),
        lease_deadline_unix_ms: v["lease_deadline_unix_ms"].as_i64().unwrap(),
        idempotency_key: v["idempotency_key"].as_str().unwrap().into(),
    }
}

pub struct Gate {
    pub path: PathBuf,
    pub dir: PathBuf,
}

impl Gate {
    pub fn new(run: &Run, name: &str) -> Self {
        Self {
            path: run.dir.join(format!("{name}.go")),
            dir: run.dir.clone(),
        }
    }
    pub fn ready_path(&self, worker: &str) -> PathBuf {
        self.dir.join(format!("{worker}.ready"))
    }
    pub fn wait_ready(&self, workers: &[&str]) {
        let deadline = Instant::now() + Duration::from_secs(8);
        while workers
            .iter()
            .any(|worker| !self.ready_path(worker).exists())
        {
            assert!(
                Instant::now() < deadline,
                "RPC processes did not reach start gate"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn release(&self) {
        fs::write(&self.path, "go").unwrap();
    }
}

pub struct RpcProcess {
    child: Option<Child>,
    pub worker: String,
}

impl RpcProcess {
    pub fn spawn(
        run: &Run,
        action: &str,
        worker: &str,
        lease: Option<&proto::Lease>,
        gate: Option<&Gate>,
        values: &[(&str, &str)],
    ) -> Self {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "rpc_client_process", "--nocapture"])
            .env("SCOUT_RPC_ACTION", action)
            .env("SCOUT_RPC_ENDPOINT", format!("http://{}", run.address))
            .env("SCOUT_RPC_WORKER", worker)
            .env("SCOUT_RPC_TIMEOUT_MS", "10000")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(lease) = lease {
            command.env("SCOUT_RPC_LEASE", lease_json(lease));
        }
        if let Some(gate) = gate {
            command.env("SCOUT_RPC_READY", gate.ready_path(worker));
            command.env("SCOUT_RPC_GO", &gate.path);
        }
        for (key, value) in values {
            command.env(key, value);
        }
        Self {
            child: Some(command.spawn().unwrap()),
            worker: worker.into(),
        }
    }

    pub fn wait(&mut self, timeout: Duration) -> Output {
        let deadline = Instant::now() + timeout;
        loop {
            let child = self.child.as_mut().unwrap();
            if child.try_wait().unwrap().is_some() {
                return self.child.take().unwrap().wait_with_output().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "RPC process {} did not finish within {timeout:?}",
                self.worker
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[allow(dead_code)]
    pub fn try_wait(&mut self) -> Option<Output> {
        if self.child.as_mut()?.try_wait().unwrap().is_some() {
            Some(self.child.take().unwrap().wait_with_output().unwrap())
        } else {
            None
        }
    }
}

impl Drop for RpcProcess {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub async fn child_rpc() {
    ticket8_rpc_child::child_rpc().await;
}

fn toml_string(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    )
}
