//! Stop requests: SIGINT (Ctrl-C, Compose `stop_signal`) and SIGTERM (what Kubernetes and
//! `docker stop` send).
//!
//! A process running as PID 1 ignores a signal it has no handler for, so without a SIGTERM
//! handler a pod would sit out its whole termination grace period before being killed.

use std::io;
use std::thread;

/// Resolves when SIGINT or SIGTERM arrives. Handlers are installed on first poll.
pub(crate) async fn requested() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

/// For the synchronous `serve` loop: exit 0 as soon as a stop is requested. Search requests are
/// short and read-only, so there is nothing to drain.
pub(crate) fn exit_when_requested() -> io::Result<()> {
    thread::Builder::new()
        .name("scout-shutdown".into())
        .spawn(|| {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(requested());
            eprintln!("scout: stop requested; exiting");
            std::process::exit(0);
        })
        .map(|_| ())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Command;
    use std::time::Duration;

    fn signal_self(name: &str) {
        let status = Command::new("kill")
            .args([name, &std::process::id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sigterm_and_sigint_both_count_as_a_stop_request() {
        for name in ["-TERM", "-INT"] {
            let waiting = tokio::spawn(requested());
            // Give the handlers time to install before signalling.
            tokio::time::sleep(Duration::from_millis(200)).await;
            signal_self(name);
            tokio::time::timeout(Duration::from_secs(5), waiting)
                .await
                .unwrap_or_else(|_| panic!("{name} did not resolve requested()"))
                .expect("join");
        }
    }
}
