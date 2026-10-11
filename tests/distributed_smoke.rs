use std::collections::HashSet;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let path =
        std::env::temp_dir().join(format!("scout-distributed-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&path).expect("create temporary directory");
    path
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve local address");
    listener.local_addr().expect("local address")
}

#[derive(Default)]
struct FixtureMetrics {
    starts: Vec<Instant>,
    paths: Vec<String>,
    active: usize,
    max_active: usize,
}

fn fixture_server() -> (String, thread::JoinHandle<()>, Arc<Mutex<FixtureMetrics>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
    let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
    listener
        .set_nonblocking(true)
        .expect("set fixture server nonblocking");
    let metrics = Arc::new(Mutex::new(FixtureMetrics::default()));
    let server_metrics = metrics.clone();
    let thread = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut requests = 0;
        while requests < 4 && Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    requests += 1;
                    let request = read_request(&mut stream);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_owned();
                    {
                        let mut metrics = server_metrics.lock().expect("fixture metrics");
                        metrics.starts.push(Instant::now());
                        metrics.paths.push(path.clone());
                        metrics.active += 1;
                        metrics.max_active = metrics.max_active.max(metrics.active);
                    }
                    let metrics = server_metrics.clone();
                    thread::spawn(move || {
                        thread::sleep(Duration::from_millis(120));
                        if path == "/robots.txt" {
                            write_response(&mut stream, "text/plain", b"User-agent: *\nAllow: /\n");
                        } else if matches!(path.as_str(), "/one" | "/two" | "/three") {
                            let page = format!(
                                "<!doctype html><html><head><title>Distributed {path}</title></head><body><main><h1>Distributed {path}</h1><p>This unique local fixture contains enough useful documentation text to pass the crawler page admission threshold and test separate coordinator and worker processes.</p></main></body></html>"
                            );
                            write_response(
                                &mut stream,
                                "text/html; charset=utf-8",
                                page.as_bytes(),
                            );
                        } else {
                            write_response(&mut stream, "text/plain", b"missing");
                        }
                        metrics.lock().expect("fixture metrics").active -= 1;
                    });
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("accept fixture request: {error}"),
            }
        }
    });
    (origin, thread, metrics)
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set fixture read timeout");
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let count = stream.read(&mut buffer).expect("read fixture request");
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn write_response(stream: &mut TcpStream, content_type: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("write fixture headers");
    stream.write_all(body).expect("write fixture body");
}

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn stop_gracefully(child: &mut Child) -> Option<std::process::ExitStatus> {
    let process_id = child.id().to_string();
    let interrupted = Command::new("kill")
        .args(["-INT", &process_id])
        .status()
        .is_ok_and(|status| status.success());
    if !interrupted {
        stop(child);
        return child.try_wait().ok().flatten();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        thread::sleep(Duration::from_millis(25));
    }
    stop(child);
    child.try_wait().ok().flatten()
}

#[test]
#[ignore = "requires a disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn three_workers_share_coordinator_request_limits_for_local_pages() {
    let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
        .expect("set SCOUT_TEST_DATABASE_URL to disposable PostgreSQL database");
    let temp = temp_dir();
    let (origin, fixture, metrics) = fixture_server();
    let config = temp.join("sources.toml");
    let coordinator_stdout =
        fs::File::create(temp.join("coordinator.stdout")).expect("create coordinator stdout log");
    let coordinator_stderr =
        fs::File::create(temp.join("coordinator.stderr")).expect("create coordinator stderr log");
    fs::write(
        &config,
        format!(
            "schema_version = \"scout.sources.v1\"\ncontact_url = \"https://example.invalid/scout\"\nuser_agent = \"Scout-Test/1 (+https://example.invalid/scout)\"\npage_target = 3\nmin_content_chars = 20\nglobal_concurrency = 2\norigin_concurrency = 1\nmin_start_spacing_ms = 60\nrequest_timeout_seconds = 5\nmax_attempts = 1\nretry_backoff_ms = 1\nretry_max_delay_ms = 1\nmax_total_attempts = 20\nmax_duration_seconds = 60\nrobots_cache_seconds = 60\nmax_frontier = 20\n\n[[sources]]\nsource_id = \"fixture\"\nseeds = [\"{origin}/one\", \"{origin}/two\", \"{origin}/three\"]\nallowed_origins = [\"{origin}\"]\npath_prefixes = [\"/\"]\n"
        ),
    )
    .expect("write source config");
    let coordinator_addr = free_address();
    let mut coordinator = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "coordinator",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            temp.to_str().expect("temp path"),
            "--bind",
            &coordinator_addr.to_string(),
        ])
        .env("SCOUT_DATABASE_URL", &database_url)
        .stdout(Stdio::from(coordinator_stdout))
        .stderr(Stdio::from(coordinator_stderr))
        .spawn()
        .expect("start coordinator process");
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if TcpStream::connect(coordinator_addr).is_ok() {
            break;
        }
        if let Some(status) = coordinator.try_wait().expect("check coordinator") {
            panic!("coordinator exited before listening: {status}");
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        TcpStream::connect(coordinator_addr).is_ok(),
        "coordinator did not listen"
    );
    let mut workers = ["smoke-worker-a", "smoke-worker-b", "smoke-worker-c"]
        .into_iter()
        .map(|worker_id| {
            Command::new(env!("CARGO_BIN_EXE_scout"))
                .args([
                    "worker",
                    "--coordinator",
                    &format!("http://{coordinator_addr}"),
                    "--data-dir",
                    temp.to_str().expect("temp path"),
                    "--worker-id",
                    worker_id,
                    "--max-tasks",
                    "1",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("run worker process")
        })
        .collect::<Vec<_>>();
    let worker_pids = workers
        .iter()
        .map(|worker| worker.id())
        .collect::<HashSet<_>>();
    assert_eq!(
        worker_pids.len(),
        3,
        "workers must be separate OS processes"
    );
    assert!(
        !worker_pids.contains(&coordinator.id()),
        "coordinator must be a separate OS process"
    );
    let worker_deadline = Instant::now() + Duration::from_secs(20);
    let mut workers_timed_out = false;
    while workers
        .iter_mut()
        .any(|worker| worker.try_wait().expect("check worker process").is_none())
    {
        if Instant::now() >= worker_deadline {
            workers_timed_out = true;
            for worker in &mut workers {
                stop(worker);
            }
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let worker_outputs = workers
        .into_iter()
        .map(|worker| worker.wait_with_output().expect("collect worker output"))
        .collect::<Vec<_>>();
    let coordinator_status = stop_gracefully(&mut coordinator);
    fixture.join().expect("join fixture server");
    assert!(
        !workers_timed_out && worker_outputs.iter().all(|output| output.status.success()),
        "worker failed: {:?}\ncoordinator stdout: {}\ncoordinator stderr: {}",
        worker_outputs
            .iter()
            .map(|output| String::from_utf8_lossy(&output.stderr).to_string())
            .collect::<Vec<_>>(),
        fs::read_to_string(temp.join("coordinator.stdout")).unwrap_or_default(),
        fs::read_to_string(temp.join("coordinator.stderr")).unwrap_or_default()
    );
    assert!(
        coordinator_status.is_some_and(|status| status.success()),
        "coordinator did not shut down cleanly: {}",
        fs::read_to_string(temp.join("coordinator.stderr")).unwrap_or_default()
    );

    let logs = fs::read_dir(temp.join("logs"))
        .expect("read event logs")
        .filter_map(Result::ok)
        .map(|entry| fs::read_to_string(entry.path()).expect("read event log"))
        .collect::<Vec<_>>();
    let completed_workers = ["smoke-worker-a", "smoke-worker-b", "smoke-worker-c"]
        .into_iter()
        .filter(|worker_id| {
            logs.iter().any(|contents| {
                contents.contains("worker.task_completed")
                    && contents.contains(&format!("\"worker_id\":\"{worker_id}\""))
                    && contents.contains("\"capture_received\":true")
            })
        })
        .count();
    assert_eq!(
        completed_workers, 3,
        "all worker identities must complete a page"
    );
    let fixture_metrics = metrics.lock().expect("read fixture metrics");
    assert_eq!(fixture_metrics.paths.len(), 4, "robots plus three pages");
    assert_eq!(
        fixture_metrics.max_active, 1,
        "origin concurrency must stay at one"
    );
    let mut starts = fixture_metrics.starts.clone();
    starts.sort();
    for pair in starts.windows(2) {
        assert!(
            pair[1].duration_since(pair[0]) >= Duration::from_millis(45),
            "shared start spacing was violated"
        );
    }
    let coordinator_log = fs::read_dir(temp.join("logs"))
        .expect("read event logs")
        .filter_map(Result::ok)
        .map(|entry| fs::read_to_string(entry.path()).expect("read event log"))
        .find(|contents| contents.contains("coordinator.started"))
        .expect("coordinator run event");
    let run_id = coordinator_log
        .lines()
        .find_map(|line| {
            let event: serde_json::Value = serde_json::from_str(line).ok()?;
            (event.get("event")?.as_str()? == "coordinator.started").then(|| {
                event
                    .get("data")?
                    .get("crawl_run_id")?
                    .as_str()
                    .map(str::to_owned)
            })?
        })
        .expect("coordinator run ID");
    if let Ok(mut database) = postgres::Client::connect(&database_url, postgres::NoTls) {
        let _ = database.execute(
            "DELETE FROM scout_crawl_runs WHERE crawl_run_id = $1",
            &[&run_id],
        );
    }
    let _ = fs::remove_dir_all(&temp);
}
