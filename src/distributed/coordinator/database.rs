//! Bounded synchronous PostgreSQL connections for coordinator control-plane work.
use super::FrontierStore;
use crate::error::Error;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

pub(super) struct FrontierPool {
    available: Mutex<Vec<FrontierStore>>,
    ready: Condvar,
}

struct Checkout<'a> {
    pool: &'a FrontierPool,
    store: Option<FrontierStore>,
}

impl FrontierPool {
    pub(super) fn lane_count() -> Result<usize, Error> {
        const DEFAULT_LANES: usize = 4;
        const MAX_LANES: usize = 8;
        let lanes = std::env::var("SCOUT_COORDINATOR_DB_LANES")
            .ok()
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| Error::invalid("SCOUT_COORDINATOR_DB_LANES must be an integer"))?
            .unwrap_or(DEFAULT_LANES);
        if !(2..=MAX_LANES).contains(&lanes) {
            return Err(Error::invalid(format!(
                "SCOUT_COORDINATOR_DB_LANES must be between 2 and {MAX_LANES}"
            )));
        }
        Ok(lanes)
    }

    pub(super) fn connect(database_url: &str, lanes: usize) -> Result<Self, Error> {
        let mut available = Vec::with_capacity(lanes);
        for _ in 0..lanes {
            // Keep the caller's PostgreSQL connection parameters, including application_name.
            available.push(FrontierStore::connect(database_url)?);
        }
        Ok(Self {
            available: Mutex::new(available),
            ready: Condvar::new(),
        })
    }

    pub(super) fn with_store<T>(
        &self,
        operation: impl FnOnce(&mut FrontierStore) -> T,
    ) -> (T, u128) {
        let waiting = Instant::now();
        let mut available = self.available.lock().expect("frontier pool lock");
        while available.is_empty() {
            available = self.ready.wait(available).expect("frontier pool lock");
        }
        let wait_ns = waiting.elapsed().as_nanos();
        let store = available.pop().expect("frontier pool connection");
        drop(available);
        let mut checkout = Checkout {
            pool: self,
            store: Some(store),
        };
        let result = operation(checkout.store.as_mut().expect("checked out connection"));
        drop(checkout);
        (result, wait_ns)
    }
}

impl Drop for Checkout<'_> {
    fn drop(&mut self) {
        if let Some(store) = self.store.take() {
            self.pool
                .available
                .lock()
                .expect("frontier pool lock")
                .push(store);
            self.pool.ready.notify_one();
        }
    }
}
