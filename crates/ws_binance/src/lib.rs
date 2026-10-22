use ahash::AHashMap;
use anyhow::{Result, anyhow};
use arrayvec::ArrayVec;
use engine_core::{EngineHandler, EngineLogger, HandlerEvent, LogEventKind};
use hdrhistogram::Histogram;
use reqwest::blocking::Client;
use serde::Serialize;
use simd_json::prelude::{ValueAsContainer, ValueAsScalar};
use std::fs;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tungstenite::client::IntoClientRequest;
use tungstenite::http;
use tungstenite::protocol::WebSocketConfig;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

/// Type alias for WebSocket connection result to reduce type complexity
type WebSocketConnectResult = (
    WebSocket<MaybeTlsStream<TcpStream>>,
    http::Response<Option<Vec<u8>>>,
);

fn ws_connect_follow_redirects(
    mut req: http::Request<()>,
    ws_cfg: WebSocketConfig,
    max_hops: usize,
) -> Result<WebSocketConnectResult> {
    use http::StatusCode;
    use tungstenite::client::connect_with_config as ws_connect;
    use tungstenite::error::Error as WsError;

    let mut hops = 0usize;
    loop {
        let cloned_req = req.clone();

        match ws_connect(cloned_req, Some(ws_cfg), 0) {
            Ok(ok) => return Ok(ok),
            Err(WsError::Http(resp)) => {
                let status = resp.status();
                let is_redirect = matches!(
                    status,
                    StatusCode::MOVED_PERMANENTLY
                        | StatusCode::FOUND
                        | StatusCode::TEMPORARY_REDIRECT
                        | StatusCode::PERMANENT_REDIRECT
                );

                if is_redirect
                    && hops < max_hops
                    && let Some(loc) = resp.headers().get(http::header::LOCATION)
                {
                    let loc = String::from_utf8_lossy(loc.as_bytes());
                    let new_uri = loc.trim();

                    let mut builder = http::Request::builder()
                        .method("GET")
                        .uri(new_uri)
                        .header("Connection", "Upgrade")
                        .header("Upgrade", "websocket")
                        .header("Sec-WebSocket-Version", "13")
                        .header("Sec-WebSocket-Key", "follow-redirect-key")
                        .header("Origin", "https://binance.com");

                    if let Some(host) = http::Uri::try_from(new_uri)
                        .ok()
                        .and_then(|uri| uri.authority().map(|a| a.to_string()))
                    {
                        builder = builder.header("Host", host);
                    }

                    req = builder.body(()).map_err(|e| anyhow!("{e}"))?;
                    hops += 1;
                    continue;
                }
                return Err(anyhow!("WS HTTP error: {status}"));
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Fixed-cap working buffers for zero/low-alloc parsing on hot path.
const MAX_LEVELS_PER_MSG: usize = 256;

/// Minimal order book using price -> qty map.
/// For a fast PoC, we pre-reserve and reuse; we avoid rehashing.
/// (True zero-alloc per tick would need a custom slab; this is good enough to start.)
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

/// Convert Binance "price as string" into integer ticks by multiplying by 1e4 (tick 0.0001).
/// Adjust if you want exact tick size; for BTCUSDT, 1e2 or 1e1 may be enough. Keep it simple here.
#[inline]
fn px_to_ticks(s: &str) -> Result<u64> {
    let v: f64 = lexical_core::parse(s.as_bytes()).map_err(|_| anyhow!("bad price"))?;
    Ok((v * 10_000.0).round() as u64)
}

#[inline]
fn qty_from_str(s: &str) -> Result<f64> {
    let v: f64 = lexical_core::parse(s.as_bytes()).map_err(|_| anyhow!("bad qty"))?;
    Ok(v)
}

/// Binance diff-depth JSON (subset)
#[derive(Debug)]
struct DiffOwned {
    first_id: u64, // U
    final_id: u64, // u
    bids: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
    asks: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
}

fn parse_diff_owned(buf: &mut [u8]) -> Result<DiffOwned> {
    let v: simd_json::BorrowedValue<'_> = simd_json::to_borrowed_value(buf)?;
    let obj = v.as_object().ok_or_else(|| anyhow!("not obj"))?;

    let first_id = obj
        .get("U")
        .and_then(|x| x.as_u64())
        .ok_or_else(|| anyhow!("no U"))?;
    let final_id = obj
        .get("u")
        .and_then(|x| x.as_u64())
        .ok_or_else(|| anyhow!("no u"))?;

    let mut bids = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();
    let mut asks = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();

    if let Some(bb) = obj.get("b").and_then(|x| x.as_array()) {
        for e in bb.iter().take(MAX_LEVELS_PER_MSG) {
            if let Some(pq) = e.as_array()
                && pq.len() >= 2
                && let (Some(p), Some(q)) = (pq[0].as_str(), pq[1].as_str())
            {
                bids.push((px_to_ticks(p)?, qty_from_str(q)?));
            }
        }
    }
    if let Some(aa) = obj.get("a").and_then(|x| x.as_array()) {
        for e in aa.iter().take(MAX_LEVELS_PER_MSG) {
            if let Some(pq) = e.as_array()
                && pq.len() >= 2
                && let (Some(p), Some(q)) = (pq[0].as_str(), pq[1].as_str())
            {
                asks.push((px_to_ticks(p)?, qty_from_str(q)?));
            }
        }
    }

    Ok(DiffOwned {
        first_id,
        final_id,
        bids,
        asks,
    })
}

/// REST snapshot shape (subset)
#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct DepthSnap {
    lastUpdateId: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CaptureConfig {
    pub root_dir: PathBuf,
    pub session_label: Option<String>,
}

#[derive(Debug)]
pub struct BinanceConfig {
    pub symbol: String,
    pub capture: Option<CaptureConfig>,
}

#[derive(Serialize)]
struct CaptureMetadata {
    symbol: String,
    started_at_ms: u64,
    ended_at_ms: Option<u64>,
    snapshot_path: String,
    diffs_path: String,
    snapshot_last_update_id: Option<u64>,
    last_sequence: Option<u64>,
}

struct CaptureWriter {
    session_dir: PathBuf,
    snapshot_path: PathBuf,
    diffs_writer: BufWriter<File>,
    metadata_path: PathBuf,
    metadata: CaptureMetadata,
    snapshot_written: bool,
}

impl CaptureWriter {
    fn new(symbol: &str, cfg: CaptureConfig) -> Result<Self> {
        let session_label = cfg.session_label.unwrap_or_else(|| now_ms().to_string());
        let session_dir = cfg.root_dir.join(symbol.to_lowercase()).join(session_label);
        fs::create_dir_all(&session_dir)?;

        let snapshot_path = session_dir.join("snapshot.json");
        let diffs_path = session_dir.join("diffs.jsonl");
        let metadata_path = session_dir.join("metadata.json");

        let diffs_writer = BufWriter::new(File::create(&diffs_path)?);
        let metadata = CaptureMetadata {
            symbol: symbol.to_string(),
            started_at_ms: now_ms(),
            ended_at_ms: None,
            snapshot_path: "snapshot.json".into(),
            diffs_path: "diffs.jsonl".into(),
            snapshot_last_update_id: None,
            last_sequence: None,
        };

        let mut writer = Self {
            session_dir,
            snapshot_path,
            diffs_writer,
            metadata_path,
            metadata,
            snapshot_written: false,
        };
        writer.flush_metadata()?;
        Ok(writer)
    }

    fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    fn record_snapshot(&mut self, raw: &str, last_update_id: u64) -> Result<()> {
        if !self.snapshot_written {
            let mut file = File::create(&self.snapshot_path)?;
            file.write_all(raw.as_bytes())?;
            file.write_all(b"\n")?;
            file.flush()?;
            self.snapshot_written = true;
        }
        self.metadata.snapshot_last_update_id = Some(last_update_id);
        self.metadata.last_sequence = Some(last_update_id);
        self.flush_metadata()
    }

    fn record_diff(&mut self, raw: &str, final_id: Option<u64>) -> Result<()> {
        self.diffs_writer.write_all(raw.as_bytes())?;
        if !raw.ends_with('\n') {
            self.diffs_writer.write_all(b"\n")?;
        }
        self.diffs_writer.flush()?;
        self.metadata.ended_at_ms = Some(now_ms());
        if let Some(id) = final_id {
            self.metadata.last_sequence = Some(id);
        }
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

fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => dur
            .as_secs()
            .saturating_mul(1_000)
            .saturating_add(dur.subsec_millis() as u64),
        Err(_) => 0,
    }
}

pub struct BinanceHandler {
    symbol: String, // e.g. BTCUSDT
    ws: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    http: Client,
    book: Book,
    last_update_id: u64,
    synced: bool,
    capture: Option<CaptureWriter>,
    capture_announced: bool,
    // metrics
    hist: Histogram<u64>,
    // buffers
    rx_buf: Vec<u8>,
}

impl BinanceHandler {
    pub fn new(symbol: String) -> Self {
        Self::with_config(BinanceConfig {
            symbol,
            capture: None,
        })
        .expect("capture disabled cannot fail")
    }

    pub fn with_config(config: BinanceConfig) -> Result<Self> {
        let BinanceConfig { symbol, capture } = config;
        let capture_writer = match capture {
            Some(cfg) => Some(CaptureWriter::new(&symbol, cfg)?),
            None => None,
        };

        let handler = Self {
            symbol,
            ws: None,
            http: Client::builder()
                .user_agent("hft-rs/0.1 binance")
                .build()
                .expect("http client"),
            book: Book::with_capacity(4096),
            last_update_id: 0,
            synced: false,
            capture: capture_writer,
            capture_announced: false,
            hist: Histogram::new(3).unwrap(),
            rx_buf: vec![0u8; 1 << 16], // 64 KiB
        };
        Ok(handler)
    }

    fn ws_url(&self) -> String {
        // Single-stream WS (prod)
        // Examples: wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
        let stream = format!("{}@depth@100ms", self.symbol.to_lowercase());
        format!("wss://stream.binance.com:9443/ws/{stream}")
    }

    fn snapshot_url(&self) -> String {
        // 1000 levels snapshot to minimize early resyncs
        format!(
            "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
            self.symbol.to_uppercase()
        )
    }

    fn ensure_ws(&mut self, log: &EngineLogger) -> Result<()> {
        if self.ws.is_some() {
            return Ok(());
        }

        // 1) Connect WS with small read timeout and TCP_NODELAY
        let url = self.ws_url();
        let req = url.clone().into_client_request()?;

        // Optional: add Origin header up-front (helps with some CF edges)
        let req = req;
        let mut builder = http::Request::builder()
            .method("GET")
            .uri(req.uri().clone())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", "binance-hft-rs")
            .header("Origin", "https://binance.com");

        if let Some(auth) = req.uri().authority() {
            builder = builder.header("Host", auth.as_str());
        }

        let req = builder.body(()).map_err(|e| anyhow!("{e}"))?;

        let ws_cfg = WebSocketConfig {
            max_message_size: Some(1 << 22), // 4 MiB
            max_frame_size: Some(1 << 22),
            ..Default::default()
        };

        let (ws, _resp) = ws_connect_follow_redirects(req, ws_cfg, 3).map_err(|e| {
            log.handler_event(
                "binance",
                &self.symbol,
                HandlerEvent::Error {
                    detail: format!("ws connect failure: {e}"),
                },
            );
            e
        })?;
        self.ws = Some(ws);
        log.handler_event("binance", &self.symbol, HandlerEvent::Connected);

        if !self.capture_announced {
            if let Some(capture) = self.capture.as_ref() {
                log.emit(LogEventKind::Text(format!(
                    "[binance:{}] capture writing to {}",
                    self.symbol,
                    capture.session_dir().display()
                )));
            }
            self.capture_announced = true;
        }

        // 2) Take REST snapshot
        let response = self.http.get(self.snapshot_url()).send().map_err(|e| {
            log.handler_event(
                "binance",
                &self.symbol,
                HandlerEvent::Error {
                    detail: format!("snapshot request failed: {e}"),
                },
            );
            e
        })?;

        let response = response.error_for_status().map_err(|e| {
            log.handler_event(
                "binance",
                &self.symbol,
                HandlerEvent::Error {
                    detail: format!("snapshot HTTP error: {e}"),
                },
            );
            e
        })?;

        let body = response.text().map_err(|e| {
            log.handler_event(
                "binance",
                &self.symbol,
                HandlerEvent::Error {
                    detail: format!("snapshot read failed: {e}"),
                },
            );
            e
        })?;

        let snap: DepthSnap = serde_json::from_str(&body).map_err(|e| {
            log.handler_event(
                "binance",
                &self.symbol,
                HandlerEvent::Error {
                    detail: format!("snapshot decode failed: {e}"),
                },
            );
            anyhow!(e)
        })?;

        if let Some(capture) = self.capture.as_mut() {
            capture.record_snapshot(&body, snap.lastUpdateId)?;
        }
        self.apply_snapshot(&snap)?;
        self.last_update_id = snap.lastUpdateId;
        self.synced = false; // need to roll forward until we see a diff with u >= lastUpdateId
        log.handler_event(
            "binance",
            &self.symbol,
            HandlerEvent::SnapshotApplied {
                bids: snap.bids.len(),
                asks: snap.asks.len(),
            },
        );

        Ok(())
    }

    fn apply_snapshot(&mut self, snap: &DepthSnap) -> Result<()> {
        self.book.bids.clear();
        self.book.asks.clear();
        for (p, q) in &snap.bids {
            let px = px_to_ticks(p)?;
            let qty = qty_from_str(q)?;
            if qty != 0.0 {
                self.book.set_bid(px, qty);
            }
        }
        for (p, q) in &snap.asks {
            let px = px_to_ticks(p)?;
            let qty = qty_from_str(q)?;
            if qty != 0.0 {
                self.book.set_ask(px, qty);
            }
        }
        Ok(())
    }

    fn process_payload(
        &mut self,
        raw: &str,
        len: usize,
        log: &EngineLogger,
    ) -> Result<Option<Duration>> {
        let t0 = Instant::now();
        match parse_diff_owned(&mut self.rx_buf[..len]) {
            Ok(diff) => {
                let final_id = diff.final_id;
                if let Some(capture) = self.capture.as_mut() {
                    capture.record_diff(raw, Some(final_id))?;
                }
                self.apply_diff(&diff, log)?;
                let elapsed = t0.elapsed();
                self.hist.record(elapsed.as_micros() as u64).ok();
                Ok(Some(elapsed))
            }
            Err(err) => {
                if let Some(capture) = self.capture.as_mut() {
                    capture.record_diff(raw, None)?;
                }
                Err(err)
            }
        }
    }

    fn apply_diff(&mut self, d: &DiffOwned, log: &EngineLogger) -> Result<()> {
        if !self.synced {
            if d.final_id < self.last_update_id + 1 {
                return Ok(());
            }
            if d.first_id <= self.last_update_id + 1 && self.last_update_id < d.final_id {
                self.synced = true;
                log.handler_event("binance", &self.symbol, HandlerEvent::Synchronized);
            } else {
                log.handler_event(
                    "binance",
                    &self.symbol,
                    HandlerEvent::Error {
                        detail: format!(
                            "seq gap before sync: have={}, diff[{}..{}]",
                            self.last_update_id, d.first_id, d.final_id
                        ),
                    },
                );
                return Err(anyhow!(
                    "seq gap before sync: have={}, diff[{}..{}]",
                    self.last_update_id,
                    d.first_id,
                    d.final_id
                ));
            }
        }

        for (px, qty) in d.bids.iter() {
            self.book.set_bid(*px, *qty);
        }
        for (px, qty) in d.asks.iter() {
            self.book.set_ask(*px, *qty);
        }
        self.last_update_id = d.final_id;
        Ok(())
    }
}

impl EngineHandler for BinanceHandler {
    fn poll_once(&mut self, log: &EngineLogger) -> Result<Option<Duration>> {
        self.ensure_ws(log)?;

        let ws = self.ws.as_mut().unwrap();
        // Read one frame with a small timeout. We set read_timeout on the socket,
        // so tungstenite will surface WouldBlock / io::ErrorKind::WouldBlock via the TLS layer.
        let msg = match ws.read() {
            Ok(m) => m,
            Err(tungstenite::Error::Io(e))
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                // nothing to do
                return Ok(None);
            }
            Err(tungstenite::Error::AlreadyClosed) => {
                self.ws = None;
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if let Message::Text(txt) = msg {
            let len = txt.len().min(self.rx_buf.len());
            self.rx_buf[..len].copy_from_slice(&txt.as_bytes()[..len]);
            return self.process_payload(&txt, len, log);
        } else if let Message::Binary(bin) = msg {
            let len = bin.len().min(self.rx_buf.len());
            self.rx_buf[..len].copy_from_slice(&bin[..len]);
            let raw_string = match std::str::from_utf8(&bin) {
                Ok(s) => s.to_owned(),
                Err(_) => String::from_utf8_lossy(&bin).into_owned(),
            };
            let result = self.process_payload(raw_string.as_str(), len, log);
            return result;
        }
        // Ignore pings/pongs/others for now
        Ok(None)
    }

    fn flush_metrics(&mut self, log: &EngineLogger) {
        let p50 = self.hist.value_at_percentile(50.0);
        let p99 = self.hist.value_at_percentile(99.0);
        log.handler_event(
            "binance",
            &self.symbol,
            HandlerEvent::Metrics {
                p50,
                p99,
                last_seq: Some(self.last_update_id),
            },
        );
    }
}
