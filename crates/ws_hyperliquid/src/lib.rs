use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use arrayvec::ArrayVec;
use engine_core::{EngineHandler, EngineLogger, HandlerEvent, LogEventKind};
use hdrhistogram::Histogram;
use serde_json::Value;
use simd_json::prelude::{ValueAsContainer, ValueAsScalar};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tungstenite::client::connect_with_config;
use tungstenite::http;
use tungstenite::protocol::WebSocketConfig;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

/// HL WS (testnet): wss://api.hyperliquid-testnet.xyz/ws
/// Subscriptions doc: l2Book, trades, etc. (we'll use l2Book "BTC").
/// https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
/// https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
const MAX_LEVELS_PER_MSG: usize = 512;

#[derive(Default)]
struct Book {
    bids: AHashMap<u64, f64>,
    asks: AHashMap<u64, f64>,
}

impl Book {
    fn with_capacity(cap: usize) -> Self {
        Self {
            bids: AHashMap::with_capacity(cap),
            asks: AHashMap::with_capacity(cap),
        }
    }
    #[inline]
    fn set_bid(&mut self, px: u64, qty: f64) {
        if qty == 0.0 {
            self.bids.remove(&px);
        } else {
            self.bids.insert(px, qty);
        }
    }
    #[inline]
    fn set_ask(&mut self, px: u64, qty: f64) {
        if qty == 0.0 {
            self.asks.remove(&px);
        } else {
            self.asks.insert(px, qty);
        }
    }
}

#[inline]
fn px_to_ticks(s: &str) -> Result<u64> {
    // HL prices are strings; pick 1e4 ticks for now.
    let v: f64 = lexical_core::parse(s.as_bytes()).map_err(|_| anyhow!("bad px"))?;
    Ok((v * 10_000.0).round() as u64)
}
#[inline]
fn sz_from_str(s: &str) -> Result<f64> {
    lexical_core::parse(s.as_bytes()).map_err(|_| anyhow!("bad sz"))
}

/// Minimal decoded WsBook: { coin, levels: [ [ {px, sz, n}... ], [ ... ] ], time }
/// We parse directly into owned (u64,f64) arrays for borrow freedom.
struct BookUpdate {
    bids: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
    asks: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
}

#[derive(Debug, Clone)]
pub struct CaptureConfig {
    pub root_dir: PathBuf,
    pub session_label: Option<String>,
}

#[derive(Debug)]
pub struct HyperConfig {
    pub coin: String,
    pub capture: Option<CaptureConfig>,
}

#[derive(serde::Serialize)]
struct CaptureMetadata {
    coin: String,
    started_at_ms: u64,
    ended_at_ms: Option<u64>,
    updates_path: String,
    last_timestamp_ms: Option<u64>,
}

struct CaptureWriter {
    session_dir: PathBuf,
    updates_writer: BufWriter<File>,
    metadata_path: PathBuf,
    metadata: CaptureMetadata,
}

impl CaptureWriter {
    fn new(coin: &str, cfg: CaptureConfig) -> Result<Self> {
        let session_label = cfg.session_label.unwrap_or_else(|| now_ms().to_string());
        let session_dir = cfg.root_dir.join(coin.to_lowercase()).join(session_label);
        fs::create_dir_all(&session_dir)?;

        let updates_path = session_dir.join("updates.jsonl");
        let metadata_path = session_dir.join("metadata.json");
        let updates_writer = BufWriter::new(File::create(&updates_path)?);
        let metadata = CaptureMetadata {
            coin: coin.to_string(),
            started_at_ms: now_ms(),
            ended_at_ms: None,
            updates_path: "updates.jsonl".into(),
            last_timestamp_ms: None,
        };

        let mut writer = Self {
            session_dir,
            updates_writer,
            metadata_path,
            metadata,
        };
        writer.flush_metadata()?;
        Ok(writer)
    }

    fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    fn record_message(&mut self, raw: &str) -> Result<()> {
        self.updates_writer.write_all(raw.as_bytes())?;
        if !raw.ends_with('\n') {
            self.updates_writer.write_all(b"\n")?;
        }
        self.updates_writer.flush()?;
        if let Some(timestamp) = extract_timestamp(raw) {
            self.metadata.last_timestamp_ms = Some(timestamp);
        }
        self.metadata.ended_at_ms = Some(now_ms());
        self.flush_metadata()
    }

    fn flush_metadata(&mut self) -> Result<()> {
        let mut file = File::create(&self.metadata_path)?;
        serde_json::to_writer_pretty(&mut file, &self.metadata)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

fn extract_timestamp(raw: &str) -> Option<u64> {
    // Hyperliquid includes "time":<millis> or "timestamp":; keep it simple.
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        if let Some(data) = value.get("data")
            && let Some(ts) = data.get("time").and_then(|v| v.as_u64())
        {
            return Some(ts);
        }
        if let Some(ts) = value.get("time").and_then(|v| v.as_u64()) {
            return Some(ts);
        }
    }
    None
}

fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => dur
            .as_secs()
            .saturating_mul(1_000)
            .saturating_add(dur.subsec_millis() as u64),
        Err(_) => 0,
    }
}

fn parse_wsbook_owned(buf: &mut [u8]) -> Result<Option<BookUpdate>> {
    let v: simd_json::BorrowedValue<'_> = simd_json::to_borrowed_value(buf)?;
    let root = if let Some(obj) = v.as_object() {
        obj
    } else {
        return Ok(None);
    };

    let payload = root.get("data").and_then(|d| d.as_object()).unwrap_or(root);

    let levels = match payload.get("levels").and_then(|x| x.as_array()) {
        Some(lvls) => lvls,
        None => return Ok(None),
    };
    let bids_a = match levels.first().and_then(|x| x.as_array()) {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let asks_a = match levels.get(1).and_then(|x| x.as_array()) {
        Some(arr) => arr,
        None => return Ok(None),
    };

    let mut bids = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();
    let mut asks = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();

    for e in bids_a.iter().take(MAX_LEVELS_PER_MSG) {
        if let Some(lvl) = e.as_object()
            && let (Some(px), Some(sz)) = (
                lvl.get("px").and_then(|x| x.as_str()),
                lvl.get("sz").and_then(|x| x.as_str()),
            )
        {
            bids.push((px_to_ticks(px)?, sz_from_str(sz)?));
        }
    }
    for e in asks_a.iter().take(MAX_LEVELS_PER_MSG) {
        if let Some(lvl) = e.as_object()
            && let (Some(px), Some(sz)) = (
                lvl.get("px").and_then(|x| x.as_str()),
                lvl.get("sz").and_then(|x| x.as_str()),
            )
        {
            asks.push((px_to_ticks(px)?, sz_from_str(sz)?));
        }
    }
    Ok(Some(BookUpdate { bids, asks }))
}

pub struct HyperHandler {
    coin: String, // e.g. "BTC"
    ws: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    book: Book,
    rx_buf: Vec<u8>,
    hist: Histogram<u64>,
    subscribed: bool,
    got_initial: bool,
    capture: Option<CaptureWriter>,
    capture_announced: bool,
}

impl HyperHandler {
    pub fn new(coin: String) -> Self {
        Self::with_config(HyperConfig {
            coin,
            capture: None,
        })
        .expect("capture disabled")
    }

    pub fn with_config(config: HyperConfig) -> Result<Self> {
        let HyperConfig { coin, capture } = config;
        let capture_writer = match capture {
            Some(cfg) => Some(CaptureWriter::new(&coin, cfg)?),
            None => None,
        };

        Ok(Self {
            coin,
            ws: None,
            book: Book::with_capacity(4096),
            rx_buf: vec![0u8; 1 << 16],
            hist: Histogram::new(3).unwrap(),
            subscribed: false,
            got_initial: false,
            capture: capture_writer,
            capture_announced: false,
        })
    }

    fn ws_url(&self) -> String {
        "wss://api.hyperliquid-testnet.xyz/ws".to_string()
    }

    fn ensure_ws(&mut self, log: &EngineLogger) -> Result<()> {
        if self.ws.is_some() {
            return Ok(());
        }

        let url = self.ws_url();
        // explicit request with Origin tends to be robust
        let uri = url.parse::<http::Uri>().context("parse ws url")?;
        let mut builder = http::Request::builder()
            .method("GET")
            .uri(uri.clone())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", "hl-hft-rs")
            .header("Origin", "https://hyperliquid.xyz");

        if let Some(host) = uri.authority() {
            builder = builder.header("Host", host.as_str());
        }

        let req = builder.body(()).map_err(|e| anyhow!("{e}"))?;

        let ws_cfg = WebSocketConfig {
            max_message_size: Some(1 << 22),
            max_frame_size: Some(1 << 22),
            ..Default::default()
        };
        let (ws, _resp) = connect_with_config(req, Some(ws_cfg), 0).map_err(|e| {
            log.handler_event(
                "hyperliquid",
                &self.coin,
                HandlerEvent::Error {
                    detail: format!("ws connect failure: {e}"),
                },
            );
            e
        })?;
        self.ws = Some(ws);
        self.subscribed = false;
        log.handler_event("hyperliquid", &self.coin, HandlerEvent::Connected);

        if !self.capture_announced {
            if let Some(capture) = self.capture.as_ref() {
                log.emit(LogEventKind::Text(format!(
                    "[hyperliquid:{}] capture writing to {}",
                    self.coin,
                    capture.session_dir().display()
                )));
            }
            self.capture_announced = true;
        }
        Ok(())
    }

    fn subscribe_l2(&mut self, log: &EngineLogger) -> Result<()> {
        if self.subscribed {
            return Ok(());
        }
        let ws = self.ws.as_mut().ok_or_else(|| anyhow!("no ws"))?;

        // { "method": "subscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
        // per docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
        let sub = format!(
            r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"{}"}}}}"#,
            self.coin
        );
        ws.send(Message::Text(sub)).context("send sub")?;
        self.subscribed = true;
        log.handler_event("hyperliquid", &self.coin, HandlerEvent::SubscriptionSent);
        Ok(())
    }

    fn apply_book(&mut self, up: &BookUpdate) -> (usize, usize, bool) {
        // HL sends full snapshot-ish levels each push; treat as set operations
        for (px, sz) in up.bids.iter() {
            self.book.set_bid(*px, *sz);
        }
        for (px, sz) in up.asks.iter() {
            self.book.set_ask(*px, *sz);
        }
        let was_initial = !self.got_initial;
        if was_initial {
            self.got_initial = true;
        }
        (up.bids.len(), up.asks.len(), was_initial)
    }
}

impl EngineHandler for HyperHandler {
    fn poll_once(&mut self, log: &EngineLogger) -> Result<Option<Duration>> {
        self.ensure_ws(log)?;
        self.subscribe_l2(log)?;

        let ws = self.ws.as_mut().unwrap();
        let msg = match ws.read() {
            Ok(m) => m,
            Err(tungstenite::Error::AlreadyClosed) => {
                log.handler_event(
                    "hyperliquid",
                    &self.coin,
                    HandlerEvent::Warning {
                        detail: "ws closed by peer".into(),
                    },
                );
                self.ws = None;
                return Ok(None);
            }
            Err(tungstenite::Error::Io(e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Ok(None);
            }
            Err(e) => {
                log.handler_event(
                    "hyperliquid",
                    &self.coin,
                    HandlerEvent::Error {
                        detail: format!("ws read error: {e}"),
                    },
                );
                return Err(e.into());
            }
        };

        match msg {
            Message::Text(txt) => {
                let len = txt.len().min(self.rx_buf.len());
                self.rx_buf[..len].copy_from_slice(&txt.as_bytes()[..len]);
                if let Some(capture) = self.capture.as_mut() {
                    capture.record_message(&txt)?;
                }
                // Only handle WsBook channel frames
                if txt.contains(r#""channel":"l2Book""#)
                    || txt.contains(r#""levels":"#)
                    || txt.contains(r#""levels":[["#)
                {
                    let t0 = Instant::now();
                    if let Some(up) = parse_wsbook_owned(&mut self.rx_buf[..len])? {
                        let (bids, asks, first) = self.apply_book(&up);
                        let dt = t0.elapsed();
                        self.hist.record(dt.as_micros() as u64).ok();
                        if first {
                            log.handler_event(
                                "hyperliquid",
                                &self.coin,
                                HandlerEvent::SnapshotApplied { bids, asks },
                            );
                        }
                        Ok(Some(dt))
                    } else {
                        log.handler_event(
                            "hyperliquid",
                            &self.coin,
                            HandlerEvent::Warning {
                                detail: "unexpected message without levels".into(),
                            },
                        );
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Message::Binary(bin) => {
                let len = bin.len().min(self.rx_buf.len());
                self.rx_buf[..len].copy_from_slice(&bin[..len]);
                if let Some(capture) = self.capture.as_mut() {
                    let raw = match std::str::from_utf8(&bin) {
                        Ok(text) => text.to_string(),
                        Err(_) => "".to_string(),
                    };
                    if !raw.is_empty() {
                        capture.record_message(&raw)?;
                    }
                }
                let t0 = Instant::now();
                if let Some(up) = parse_wsbook_owned(&mut self.rx_buf[..len])? {
                    let (bids, asks, first) = self.apply_book(&up);
                    let dt = t0.elapsed();
                    self.hist.record(dt.as_micros() as u64).ok();
                    if first {
                        log.handler_event(
                            "hyperliquid",
                            &self.coin,
                            HandlerEvent::SnapshotApplied { bids, asks },
                        );
                    }
                    Ok(Some(dt))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn flush_metrics(&mut self, log: &EngineLogger) {
        let p50 = self.hist.value_at_percentile(50.0);
        let p99 = self.hist.value_at_percentile(99.0);
        log.handler_event(
            "hyperliquid",
            &self.coin,
            HandlerEvent::Metrics {
                p50,
                p99,
                last_seq: None,
            },
        );
    }
}
