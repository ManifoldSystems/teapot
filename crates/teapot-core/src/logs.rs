//! Bounded local diagnostic history and retryable upload queue.
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: String,
    pub timestamp: u64,
    pub level: String,
    pub source: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct StoredLog {
    #[serde(flatten)]
    pub entry: LogEntry,
    pub cursor: u64,
    pub node_id: String,
}

#[derive(Default)]
struct State {
    queue: VecDeque<LogEntry>,
    seq: u64,
    writer: Option<std::sync::mpsc::SyncSender<DiskMessage>>,
}

enum DiskMessage {
    Entry(LogEntry),
    Flush(std::sync::mpsc::Sender<()>),
}

#[derive(Clone)]
pub struct LogBuffer {
    state: Arc<Mutex<State>>,
    session: String,
}
static BUFFER: OnceLock<LogBuffer> = OnceLock::new();

pub fn init(file: Option<PathBuf>) -> LogBuffer {
    BUFFER
        .get_or_init(|| LogBuffer {
            state: Arc::new(Mutex::new(State {
                writer: file.map(start_writer),
                ..State::default()
            })),
            session: format!(
                "{}-{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
                std::process::id()
            ),
        })
        .clone()
}

pub fn buffer() -> Option<LogBuffer> {
    BUFFER.get().cloned()
}

/// Wait briefly for accepted disk writes during a graceful shutdown.
pub fn flush() {
    if let Some(buffer) = buffer() {
        let writer = buffer.state.lock().unwrap().writer.clone();
        if let Some(writer) = writer {
            let (tx, rx) = std::sync::mpsc::channel();
            if writer.try_send(DiskMessage::Flush(tx)).is_ok() {
                let _ = rx.recv_timeout(std::time::Duration::from_secs(2));
            }
        }
    }
}

impl LogBuffer {
    pub fn batch(&self) -> Vec<LogEntry> {
        self.state
            .lock()
            .unwrap()
            .queue
            .iter()
            .take(20)
            .cloned()
            .collect()
    }
    pub fn ack(&self, batch: &[LogEntry]) {
        let ids: std::collections::HashSet<_> = batch.iter().map(|e| &e.id).collect();
        self.state
            .lock()
            .unwrap()
            .queue
            .retain(|e| !ids.contains(&e.id));
    }
    fn record(&self, level: String, source: String, message: String) {
        let mut state = self.state.lock().unwrap();
        state.seq += 1;
        let entry = LogEntry {
            id: format!("{}-{}", self.session, state.seq),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            level,
            source: source.chars().take(128).collect(),
            message: message.chars().take(1024).collect(),
        };
        if let Some(writer) = &state.writer {
            // Slow disks must never stall proxy traffic or the async executor.
            let _ = writer.try_send(DiskMessage::Entry(entry.clone()));
        }
        if state.queue.len() == 1000 {
            state.queue.pop_front();
        }
        state.queue.push_back(entry);
    }
}

fn start_writer(file: PathBuf) -> std::sync::mpsc::SyncSender<DiskMessage> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<DiskMessage>(1000);
    std::thread::spawn(move || {
        if let Some(parent) = file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        for message in rx {
            let entry = match message {
                DiskMessage::Entry(entry) => entry,
                DiskMessage::Flush(done) => {
                    let _ = done.send(());
                    continue;
                }
            };
            if std::fs::metadata(&file).is_ok_and(|m| m.len() > 2 * 1024 * 1024) {
                let _ = std::fs::rename(&file, file.with_extension("jsonl.1"));
            }
            let mut options = std::fs::OpenOptions::new();
            options.create(true).append(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            if let Ok(mut out) = options.open(&file) {
                let _ = serde_json::to_writer(&mut out, &entry);
                let _ = writeln!(out);
            }
        }
    });
    tx
}

#[derive(Default)]
struct Fields(String);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        if field.name() != "message" {
            self.0.push_str(field.name());
            self.0.push('=');
        }
        self.0.push_str(&format!("{value:?}"));
    }
}

impl<S: Subscriber> Layer<S> for LogBuffer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if !meta.target().starts_with("teapot") || *meta.level() == tracing::Level::TRACE {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.record(
            meta.level().as_str().to_lowercase(),
            meta.target().to_string(),
            fields.0,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ack_does_not_discard_events_added_during_upload() {
        let logs = LogBuffer {
            state: Arc::new(Mutex::new(State::default())),
            session: "test".into(),
        };
        logs.record("info".into(), "test".into(), "one".into());
        let sent = logs.batch();
        logs.record("warn".into(), "test".into(), "two".into());
        logs.ack(&sent);
        assert_eq!(logs.batch()[0].message, "two");
        for _ in 0..1100 {
            logs.record("info".into(), "test".into(), "bounded".into());
        }
        assert_eq!(logs.state.lock().unwrap().queue.len(), 1000);
    }
}
