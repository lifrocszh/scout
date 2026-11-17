//! SIGTERM drain of a worker, with real coordinator and worker processes.
//! Run with a disposable SCOUT_TEST_DATABASE_URL and `--ignored --test-threads=1`.
//!
//! The fixture serves one page that answers after a delay, so the worker is mid-task when the
//! signal arrives. Two cases: the task finishes inside the drain window (the worker completes it
//! and exits 0), and the window is shorter than the task (the worker hands the lease back, exits 0,
//! and the task is pending again for another worker to claim).
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("scout-drain-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&path).expect("create temporary directory");
    path
}

fn free_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve local address")
        .local_addr()
        .expect("local address")
}

/// Serves robots.txt at once and `/slow` after `delay`. `slow_started` flips when `/slow` arrives.
struct Fixture {
    origin: String,
    slow_started: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

impl Fixture {
    fn start(delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture");
        listener.set_nonblocking(true).expect("nonblocking fixture");
        let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
        let slow_started = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let (flag, halt) = (slow_started.clone(), stop.clone());
        thread::spawn(move || {
            while !halt.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let flag = flag.clone();
                        thread::spawn(move || answer(stream, delay, &flag));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(10)),
                }
            }
        });
        Self {
            origin,
            slow_started,
            stop,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn answer(mut stream: TcpStream, delay: Duration, slow_started: &AtomicBool) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
    let mut head = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(count) => head.extend_from_slice(&buffer[..count]),
        }
    }
    let request = String::from_utf8_lossy(&head);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let (mime, body) = if path == "/robots.txt" {
        ("text/plain", "User-agent: *\nAllow: /\n".to_owned())
    } else {
        slow_started.store(true, Ordering::Relaxed);
        thread::sleep(delay);
        (
            "text/html; charset=utf-8",
            "<!doctype html><html><head><title>Slow fixture page</title></head><body><main><h1>Slow fixture page</h1><p>This deliberately slow page has enough useful documentation text to pass the page admission threshold while a worker is asked to stop in the middle of fetching it.</p></main></body></html>".to_owned(),
        )
    };
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn wait_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for {what}");
}

fn http_status(address: SocketAddr, path: &str) -> Option<u16> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    write!(stream, "GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.split_whitespace().nth(1)?.parse().ok()
}

fn events(dir: &Path) -> String {
    fs::read_dir(dir.join("logs"))
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| fs::read_to_string(entry.path()).ok())
                .collect::<String>()
        })
        .unwrap_or_default()
}

struct Stack {
    temp: PathBuf,
    coordinator: Child,
    coordinator_address: SocketAddr,
    database_url: String,
}

impl Stack {
    fn start(database_url: &str, origin: &str) -> Self {
        let temp = temp_dir();
        let config = temp.join("sources.toml");
        fs::write(
            &config,
            format!(
                "schema_version = \"scout.sources.v1\"\ncontact_url = \"https://example.invalid/scout\"\nuser_agent = \"Scout-Test/1 (+https://example.invalid/scout)\"\npage_target = 1\nmin_content_chars = 20\nglobal_concurrency = 2\norigin_concurrency = 1\nmin_start_spacing_ms = 10\nrequest_timeout_seconds = 30\nmax_attempts = 3\nretry_backoff_ms = 1\nretry_max_delay_ms = 1\nmax_total_attempts = 20\nmax_duration_seconds = 120\nrobots_cache_seconds = 60\nmax_frontier = 20\n\n[[sources]]\nsource_id = \"fixture\"\nseeds = [\"{origin}/slow\"]\nallowed_origins = [\"{origin}\"]\npath_prefixes = [\"/\"]\n"
            ),
        )
        .expect("write source config");
        let coordinator_address = free_address();
        let coordinator = Command::new(env!("CARGO_BIN_EXE_scout"))
            .args([
                "coordinator",
                "--config",
                config.to_str().expect("config path"),
                "--data-dir",
                temp.join("coordinator").to_str().expect("data path"),
                "--bind",
                &coordinator_address.to_string(),
            ])
            .env("SCOUT_DATABASE_URL", database_url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start coordinator");
        wait_until("the coordinator to listen", Duration::from_secs(20), || {
            TcpStream::connect(coordinator_address).is_ok()
        });
        Self {
            temp,
            coordinator,
            coordinator_address,
            database_url: database_url.to_owned(),
        }
    }

    fn worker(&self, drain_secs: u64, metrics: SocketAddr) -> Child {
        Command::new(env!("CARGO_BIN_EXE_scout"))
            .args([
                "worker",
                "--coordinator",
                &format!("http://{}", self.coordinator_address),
                "--data-dir",
                self.temp.join("worker").to_str().expect("data path"),
                "--worker-id",
                "drain-worker",
            ])
            .env("SCOUT_WORKER_DRAIN_SECS", drain_secs.to_string())
            .env("SCOUT_METRICS_BIND", metrics.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start worker")
    }

    fn task_rows(&self) -> Vec<(String, Option<String>)> {
        let mut client = postgres::Client::connect(&self.database_url, postgres::NoTls)
            .expect("connect to the test database");
        client
            .query(
                "SELECT status, lease_owner FROM scout_crawl_tasks WHERE normalized_url LIKE '%/slow' AND crawl_run_id = $1",
                &[&self.run_id()],
            )
            .expect("query tasks")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }

    fn run_id(&self) -> String {
        events(&self.temp.join("coordinator"))
            .lines()
            .find_map(|line| {
                let event: serde_json::Value = serde_json::from_str(line).ok()?;
                (event.get("event")?.as_str()? == "coordinator.started")
                    .then(|| Some(event.get("data")?.get("crawl_run_id")?.as_str()?.to_owned()))?
            })
            .expect("coordinator run ID")
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = self.coordinator.kill();
        let _ = self.coordinator.wait();
        if let Ok(run_id) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.run_id()))
            && let Ok(mut database) = postgres::Client::connect(&self.database_url, postgres::NoTls)
        {
            let _ = database.execute(
                "DELETE FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&run_id],
            );
        }
        let _ = fs::remove_dir_all(&self.temp);
    }
}

fn terminate(child: &Child) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .expect("run kill")
            .success()
    );
}

fn wait_exit(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("check worker") {
            return status;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    panic!("worker did not exit within {timeout:?} of SIGTERM");
}

fn database_url() -> String {
    std::env::var("SCOUT_TEST_DATABASE_URL")
        .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database")
}

#[test]
#[ignore = "requires a disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn sigterm_lets_an_in_flight_task_finish_inside_the_drain_window() {
    let fixture = Fixture::start(Duration::from_millis(1500));
    let stack = Stack::start(&database_url(), &fixture.origin);
    let metrics = free_address();
    let mut worker = stack.worker(20, metrics);
    wait_until("the worker to turn ready", Duration::from_secs(20), || {
        http_status(metrics, "/readyz") == Some(200)
    });
    wait_until("the page fetch to start", Duration::from_secs(20), || {
        fixture.slow_started.load(Ordering::Relaxed)
    });

    terminate(&worker);
    let status = wait_exit(&mut worker, Duration::from_secs(15));
    assert!(status.success(), "worker exit status {status}");

    let log = events(&stack.temp.join("worker"));
    assert!(log.contains("worker.task_completed"), "{log}");
    assert!(!log.contains("worker.task_released"), "{log}");
    assert_eq!(
        stack.task_rows(),
        vec![("succeeded".to_owned(), None)],
        "the in-flight task must be completed, not abandoned"
    );
}

#[test]
#[ignore = "requires a disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn sigterm_releases_the_lease_when_the_task_outlasts_the_drain_window() {
    // The page takes 20 s; the drain window is 1 s.
    let fixture = Fixture::start(Duration::from_secs(20));
    let stack = Stack::start(&database_url(), &fixture.origin);
    let metrics = free_address();
    let mut worker = stack.worker(1, metrics);
    wait_until("the worker to turn ready", Duration::from_secs(20), || {
        http_status(metrics, "/readyz") == Some(200)
    });
    wait_until("the page fetch to start", Duration::from_secs(20), || {
        fixture.slow_started.load(Ordering::Relaxed)
    });

    let signalled = Instant::now();
    terminate(&worker);
    // Unready as soon as the signal lands, while the drain window is still open.
    wait_until("the worker to turn unready", Duration::from_secs(1), || {
        http_status(metrics, "/readyz") != Some(200)
    });
    let status = wait_exit(&mut worker, Duration::from_secs(10));
    assert!(status.success(), "worker exit status {status}");
    assert!(
        signalled.elapsed() < Duration::from_secs(10),
        "the worker waited for the whole 20 s task"
    );

    let log = events(&stack.temp.join("worker"));
    assert!(
        log.lines()
            .any(|line| line.contains("\"event\":\"worker.task_released\"")
                && line.contains("\"outcome\":\"success\"")),
        "{log}"
    );
    // Back in the queue with no owner: another worker can claim it immediately.
    assert_eq!(
        stack.task_rows(),
        vec![("pending".to_owned(), None)],
        "the released task must be pending and unowned"
    );
}
