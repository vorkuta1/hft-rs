use std::panic::{self, AssertUnwindSafe};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use spsc::{Consumer, Producer, channel};

pub struct EngineConfig {
    pub symbol: String,
    pub venue: String,
    pub core_id: usize,
}

const LOG_CAPACITY: usize = 1024;
pub type EngineLogger = Logger<LOG_CAPACITY>;

#[derive(Debug, Clone)]
pub enum LogEventKind {
    EngineStarted {
        venue: String,
        symbol: String,
    },
    EngineApplyLatency {
        micros: u64,
    },
    EngineMetricsFlushed,
    EngineLogQueue {
        depth: usize,
        capacity: usize,
    },
    HandlerEvent {
        venue: &'static str,
        symbol: String,
        event: HandlerEvent,
    },
    Text(String),
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum HandlerEvent {
    Connected,
    SubscriptionSent,
    Synchronized,
    SnapshotApplied {
        bids: usize,
        asks: usize,
    },
    DiffApplied {
        bids: usize,
        asks: usize,
    },
    Metrics {
        p50: u64,
        p99: u64,
        last_seq: Option<u64>,
    },
    Warning {
        detail: String,
    },
    Error {
        detail: String,
    },
}

#[derive(Debug, Clone)]
pub struct LogEvent {
    timestamp: Instant,
    kind: LogEventKind,
}

impl LogEvent {
    fn new(kind: LogEventKind) -> Self {
        Self {
            timestamp: Instant::now(),
            kind,
        }
    }
}

/// Owns the only producer handle; not `Clone`, and `!Sync` via the producer.
pub struct Logger<const N: usize> {
    tx: Producer<LogEvent, N>,
}

impl<const N: usize> Logger<N> {
    pub fn emit(&self, kind: LogEventKind) {
        if let Err(dropped) = self.tx.push(LogEvent::new(kind)) {
            eprintln!("[log drop] {:?}", dropped.kind);
        }
    }

    pub fn with_producer(tx: Producer<LogEvent, N>) -> Self {
        Self { tx }
    }

    pub fn handler_event(&self, venue: &'static str, symbol: &str, event: HandlerEvent) {
        self.emit(LogEventKind::HandlerEvent {
            venue,
            symbol: symbol.to_string(),
            event,
        });
    }

    pub fn queue_len(&self) -> usize {
        self.tx.len()
    }

    pub fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    pub fn shutdown(&self) {
        let mut event = LogEvent::new(LogEventKind::Shutdown);
        while let Err(back) = self.tx.push(event) {
            event = back;
            std::thread::yield_now();
        }
    }
}

fn logging_worker<const N: usize>(consumer: Consumer<LogEvent, N>) {
    let backoff = Duration::from_millis(1);
    loop {
        if let Some(event) = consumer.pop() {
            let delta = event.timestamp.elapsed();
            match event.kind {
                LogEventKind::EngineStarted { venue, symbol } => {
                    eprintln!(
                        "[engine] {}:{} started (+{}us)",
                        venue,
                        symbol,
                        delta.as_micros()
                    );
                }
                LogEventKind::EngineApplyLatency { micros } => {
                    eprintln!("[latency] apply {}us (+{}us)", micros, delta.as_micros());
                }
                LogEventKind::EngineMetricsFlushed => {
                    eprintln!("[metrics] flushed (+{}us)", delta.as_micros());
                }
                LogEventKind::EngineLogQueue { depth, capacity } => {
                    eprintln!(
                        "[log-queue] depth={} capacity={} (+{}us)",
                        depth,
                        capacity,
                        delta.as_micros()
                    );
                }
                LogEventKind::HandlerEvent {
                    venue,
                    symbol,
                    event,
                } => match event {
                    HandlerEvent::Connected => {
                        eprintln!(
                            "[{}:{}] connected (+{}us)",
                            venue,
                            symbol,
                            delta.as_micros()
                        );
                    }
                    HandlerEvent::SubscriptionSent => {
                        eprintln!(
                            "[{}:{}] subscription sent (+{}us)",
                            venue,
                            symbol,
                            delta.as_micros()
                        );
                    }
                    HandlerEvent::SnapshotApplied { bids, asks } => {
                        eprintln!(
                            "[{}:{}] snapshot applied bids={} asks={} (+{}us)",
                            venue,
                            symbol,
                            bids,
                            asks,
                            delta.as_micros()
                        );
                    }
                    HandlerEvent::Synchronized => {
                        eprintln!(
                            "[{}:{}] feed synchronized (+{}us)",
                            venue,
                            symbol,
                            delta.as_micros()
                        );
                    }
                    HandlerEvent::DiffApplied { bids, asks } => {
                        eprintln!(
                            "[{}:{}] diff applied bids={} asks={} (+{}us)",
                            venue,
                            symbol,
                            bids,
                            asks,
                            delta.as_micros()
                        );
                    }
                    HandlerEvent::Metrics { p50, p99, last_seq } => {
                        let seq_display = last_seq
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "n/a".to_string());
                        eprintln!(
                            "[{}:{}] latency p50={}us p99={}us last_seq={} (+{}us)",
                            venue,
                            symbol,
                            p50,
                            p99,
                            seq_display,
                            delta.as_micros()
                        );
                    }
                    HandlerEvent::Warning { detail } => {
                        eprintln!(
                            "[{}:{} WARN] {} (+{}us)",
                            venue,
                            symbol,
                            detail,
                            delta.as_micros()
                        );
                    }
                    HandlerEvent::Error { detail } => {
                        eprintln!(
                            "[{}:{} ERROR] {} (+{}us)",
                            venue,
                            symbol,
                            detail,
                            delta.as_micros()
                        );
                    }
                },
                LogEventKind::Text(text) => {
                    eprintln!("[log] {} (+{}us)", text, delta.as_micros());
                }
                LogEventKind::Shutdown => {
                    eprintln!("[log] shutdown requested (+{}us)", delta.as_micros());
                    break;
                }
            }
        } else {
            thread::sleep(backoff);
        }
    }
}

/// What the engine expects from a venue-specific handler.
pub trait EngineHandler {
    /// Do one non-blocking (or short-blocking) poll step.
    /// Return the time spent in the *apply/parse* part so we can histogram it.
    fn poll_once(&mut self, log: &EngineLogger) -> anyhow::Result<Option<Duration>>;

    /// Flush metrics (print/export) on a cadence.
    fn flush_metrics(&mut self, log: &EngineLogger);
}

/// Generic single-owner engine.
/// The handler owns its sockets, book, and hot path buffers.
/// We just pin the thread and spin the loop.
pub struct Engine<H: EngineHandler> {
    pub cfg: EngineConfig,
    pub handler: H,
}

impl<H: EngineHandler> Engine<H> {
    pub fn run(mut self) -> anyhow::Result<()> {
        let (log_tx, log_rx) = channel::<LogEvent, LOG_CAPACITY>();
        let logger = Logger::with_producer(log_tx);
        let mut logger_thread: Option<JoinHandle<()>> = Some(
            thread::Builder::new()
                .name("logger".into())
                .spawn(move || logging_worker(log_rx))
                .expect("spawn logger thread"),
        );

        let core = core_affinity::CoreId {
            id: self.cfg.core_id,
        };
        core_affinity::set_for_current(core);
        logger.emit(LogEventKind::EngineStarted {
            venue: self.cfg.venue.clone(),
            symbol: self.cfg.symbol.clone(),
        });

        let mut last_flush = Instant::now();
        loop {
            let poll_result =
                panic::catch_unwind(AssertUnwindSafe(|| self.handler.poll_once(&logger)));
            match poll_result {
                Ok(Ok(Some(latency))) => {
                    logger.emit(LogEventKind::EngineApplyLatency {
                        micros: latency.as_micros() as u64,
                    });
                }
                Ok(Ok(None)) => {}
                Ok(Err(err)) => {
                    logger.emit(LogEventKind::Text(format!("handler error: {err}")));
                    logger.shutdown();
                    if let Some(handle) = logger_thread.take() {
                        let _ = handle.join();
                    }
                    return Err(err);
                }
                Err(payload) => {
                    logger.emit(LogEventKind::Text("handler panic".into()));
                    logger.shutdown();
                    if let Some(handle) = logger_thread.take() {
                        let _ = handle.join();
                    }
                    panic::resume_unwind(payload);
                }
            }

            if last_flush.elapsed() >= Duration::from_secs(5) {
                self.handler.flush_metrics(&logger);
                logger.emit(LogEventKind::EngineMetricsFlushed);
                logger.emit(LogEventKind::EngineLogQueue {
                    depth: logger.queue_len(),
                    capacity: logger.capacity(),
                });
                last_flush = Instant::now();
            }
            // light backoff if nothing arrived (handler keeps this small)
            std::hint::spin_loop();
        }
    }
}
