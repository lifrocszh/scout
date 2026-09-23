use super::{
    Arc, BufWriter, EVENT_SCHEMA, Error, File, Mutex, OpenOptions, Path, SecondsFormat, Serialize,
    Utc, Value, Write, fs,
};

#[derive(Debug, Serialize)]
struct Event {
    schema_version: &'static str,
    timestamp: String,
    level: &'static str,
    event: String,
    component: &'static str,
    run_id: String,
    outcome: &'static str,
    data: Value,
}

pub(crate) struct Events {
    run_id: String,
    component: &'static str,
    log: BufWriter<File>,
}

impl Events {
    pub(crate) fn open(
        data_dir: &Path,
        run_id: &str,
        component: &'static str,
    ) -> Result<Self, Error> {
        let dir = data_dir.join("logs");
        fs::create_dir_all(&dir)
            .map_err(|e| Error::storage(format!("create logs directory: {e}")))?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(format!("{run_id}.jsonl")))
            .map_err(|e| Error::storage(format!("create event log: {e}")))?;
        Ok(Self {
            run_id: run_id.into(),
            component,
            log: BufWriter::new(file),
        })
    }

    pub(crate) fn emit(&mut self, event: &str, outcome: &'static str, data: Value) {
        let record = Event {
            schema_version: EVENT_SCHEMA,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            level: if outcome == "failure" {
                "error"
            } else {
                "info"
            },
            event: event.into(),
            component: self.component,
            run_id: self.run_id.clone(),
            outcome,
            data,
        };
        if let Ok(line) = serde_json::to_string(&record) {
            let _ = writeln!(self.log, "{line}");
            let _ = self.log.flush();
            eprintln!("{line}");
        }
    }
}

pub(crate) fn emit_shared(
    events: &Arc<Mutex<Events>>,
    event: &str,
    outcome: &'static str,
    data: Value,
) {
    events
        .lock()
        .expect("shared event log lock")
        .emit(event, outcome, data);
}
