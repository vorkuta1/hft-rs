//! Bitstamp ground truth: the public `live_orders_<pair>` channel publishes every
//! order create/change/delete with ids, and `/api/v2/order_book/<pair>?group=2`
//! returns the full book with ids in time priority. Messages carry an
//! `event_id`/`pre_event_id` chain, so gaps are detectable.
//!
//! Capture layout (`<root>/bitstamp/<pair>/<session>/`):
//! `snapshot.json` (raw REST body), `orders.jsonl` (raw WS frames), `metadata.json`.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::session::FeedEvent;
use crate::truth::{L3Event, L3Msg};
use crate::types::{Px, Qty, Scale, Side, Ts};

pub const WS_URL: &str = "wss://ws.bitstamp.net";
pub const DEFAULT_SCALE: Scale = Scale {
    px_decimals: 2,
    qty_decimals: 8,
};
/// Live messages buffered before the REST snapshot is requested, so the stream
/// is guaranteed to cover the snapshot instant.
const PRE_SNAPSHOT_BUFFER: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub venue: String,
    pub pair: String,
    pub scale: Scale,
    pub started_at_us: Ts,
    pub snapshot_ts: Ts,
    pub ended_at_us: Option<Ts>,
    pub messages: u64,
    pub end_reason: Option<String>,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    event: &'a str,
    #[serde(default)]
    event_id: Option<&'a str>,
    #[serde(default)]
    pre_event_id: Option<&'a str>,
    #[serde(borrow)]
    data: &'a RawValue,
}

#[derive(Deserialize)]
struct OrderData<'a> {
    id: u64,
    order_type: u8,
    microtimestamp: &'a str,
    amount_str: &'a str,
    amount_traded: &'a str,
    price_str: &'a str,
}

#[derive(Deserialize)]
struct Snapshot<'a> {
    microtimestamp: &'a str,
    #[serde(borrow)]
    bids: Vec<[&'a str; 3]>,
    #[serde(borrow)]
    asks: Vec<[&'a str; 3]>,
}

/// Parses the REST `group=2` body: `[price, amount, id]` rows, time priority within a price.
pub fn parse_snapshot(body: &str, scale: Scale) -> Result<FeedEvent> {
    let snap: Snapshot = serde_json::from_str(body).context("parse bitstamp snapshot")?;
    let mut orders = Vec::with_capacity(snap.bids.len() + snap.asks.len());
    for (side, rows) in [(Side::Bid, &snap.bids), (Side::Ask, &snap.asks)] {
        for [px, qty, id] in rows {
            orders.push((
                id.parse::<u64>()
                    .with_context(|| format!("order id {id:?}"))?,
                side,
                scale.px(px)?,
                scale.qty(qty)?,
            ));
        }
    }
    Ok(FeedEvent::TruthSnapshot {
        ts: snap.microtimestamp.parse()?,
        orders,
    })
}

pub fn snapshot_ts(body: &str) -> Result<Ts> {
    #[derive(Deserialize)]
    struct Ts<'a> {
        microtimestamp: &'a str,
    }
    let ts: Ts = serde_json::from_str(body).context("parse bitstamp snapshot")?;
    Ok(ts.microtimestamp.parse()?)
}

/// Stateful parser for `live_orders` frames: verifies the event chain and drops
/// events already reflected in the snapshot.
pub struct Parser {
    scale: Scale,
    snapshot_ts: Ts,
    last_event_id: Option<String>,
}

impl Parser {
    pub fn new(scale: Scale, snapshot_ts: Ts) -> Self {
        Self {
            scale,
            snapshot_ts,
            last_event_id: None,
        }
    }

    pub fn parse(&mut self, frame: &str) -> Result<Option<L3Event>> {
        let env: Envelope = serde_json::from_str(frame).context("parse bitstamp frame")?;
        let kind = match env.event {
            "order_created" | "order_changed" | "order_deleted" => env.event,
            "bts:request_reconnect" => bail!("venue requested reconnect; stream ends here"),
            _ => return Ok(None),
        };
        if let (Some(prev), Some(pre)) = (self.last_event_id.as_deref(), env.pre_event_id)
            && prev != pre
        {
            bail!("event chain broken: expected pre_event_id {prev}, got {pre}");
        }
        if let Some(id) = env.event_id {
            self.last_event_id = Some(id.to_owned());
        }

        let d: OrderData = serde_json::from_str(env.data.get()).context("parse order data")?;
        let ts: Ts = d.microtimestamp.parse()?;
        if ts <= self.snapshot_ts {
            return Ok(None);
        }
        let side = match d.order_type {
            0 => Side::Bid,
            1 => Side::Ask,
            other => bail!("unknown order_type {other}"),
        };
        let (id, px, qty): (u64, Px, Qty) = (
            d.id,
            self.scale.px(d.price_str)?,
            self.scale.qty(d.amount_str)?,
        );
        let traded_total = self.scale.qty(d.amount_traded)?;
        let msg = match kind {
            "order_created" => L3Msg::Add { id, side, px, qty },
            "order_changed" => L3Msg::Update {
                id,
                side,
                px,
                remaining: qty,
                traded_total,
            },
            _ => L3Msg::Delete { id, traded_total },
        };
        Ok(Some(L3Event { ts, msg }))
    }
}

/// Replays a capture directory as feed events.
pub struct CaptureReader {
    snapshot: Option<FeedEvent>,
    lines: Lines<BufReader<File>>,
    parser: Parser,
    line_no: usize,
}

impl CaptureReader {
    pub fn open(dir: &Path, scale: Scale) -> Result<Self> {
        let body = fs::read_to_string(dir.join("snapshot.json"))
            .with_context(|| format!("read {}/snapshot.json", dir.display()))?;
        let snapshot = parse_snapshot(&body, scale)?;
        let FeedEvent::TruthSnapshot { ts, .. } = snapshot else {
            unreachable!()
        };
        let file = File::open(dir.join("orders.jsonl"))
            .with_context(|| format!("open {}/orders.jsonl", dir.display()))?;
        Ok(Self {
            snapshot: Some(snapshot),
            lines: BufReader::new(file).lines(),
            parser: Parser::new(scale, ts),
            line_no: 0,
        })
    }

    pub fn read_metadata(dir: &Path) -> Result<Metadata> {
        let raw = fs::read_to_string(dir.join("metadata.json"))
            .with_context(|| format!("read {}/metadata.json", dir.display()))?;
        Ok(serde_json::from_str(&raw)?)
    }
}

impl Iterator for CaptureReader {
    type Item = Result<FeedEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(snap) = self.snapshot.take() {
            return Some(Ok(snap));
        }
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(e) => return Some(Err(e.into())),
            };
            self.line_no += 1;
            match self.parser.parse(&line) {
                Ok(Some(ev)) => return Some(Ok(FeedEvent::Truth(ev))),
                Ok(None) => continue,
                // A recorder killed mid-write leaves a torn final line.
                Err(_) if self.lines.next().is_none() => return None,
                Err(e) => {
                    return Some(Err(e.context(format!("orders.jsonl line {}", self.line_no))));
                }
            }
        }
    }
}

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

fn connect(pair: &str) -> Result<Socket> {
    let (mut ws, _) = tungstenite::connect(WS_URL).context("connect bitstamp websocket")?;
    let sub = format!(r#"{{"event":"bts:subscribe","data":{{"channel":"live_orders_{pair}"}}}}"#);
    ws.send(Message::Text(sub))?;
    Ok(ws)
}

/// Next text frame; answers pings, errors on close.
fn next_text(ws: &mut Socket) -> Result<String> {
    loop {
        match ws.read()? {
            Message::Text(t) => return Ok(t),
            Message::Ping(p) => ws.send(Message::Pong(p))?,
            Message::Close(frame) => bail!("websocket closed: {frame:?}"),
            _ => {}
        }
    }
}

pub fn fetch_snapshot(pair: &str) -> Result<String> {
    let url = format!("https://www.bitstamp.net/api/v2/order_book/{pair}/?group=2");
    let resp = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?
        .get(&url)
        .send()
        .with_context(|| format!("GET {url}"))?
        .error_for_status()?;
    Ok(resp.text()?)
}

/// Why a stream stopped.
pub enum StreamEnd {
    /// `on_frame` asked to stop.
    Requested,
    Error(anyhow::Error),
}

/// Connects, buffers a little, fetches the snapshot, then hands every frame to
/// `on_frame` (buffered ones first) until it returns `false` or the stream fails.
/// `on_snapshot` builds the state that `on_frame` receives.
pub fn stream<S>(
    pair: &str,
    on_snapshot: impl FnOnce(&str) -> Result<S>,
    mut on_frame: impl FnMut(&mut S, &str) -> Result<bool>,
) -> Result<StreamEnd> {
    let mut ws = connect(pair)?;
    let mut buffered = Vec::new();
    let t0 = Instant::now();
    while t0.elapsed() < PRE_SNAPSHOT_BUFFER {
        buffered.push(next_text(&mut ws)?);
    }
    let body = fetch_snapshot(pair)?;
    let mut state = on_snapshot(&body)?;
    for frame in &buffered {
        if !on_frame(&mut state, frame)? {
            return Ok(StreamEnd::Requested);
        }
    }
    drop(buffered);
    loop {
        let frame = match next_text(&mut ws) {
            Ok(f) => f,
            Err(e) => return Ok(StreamEnd::Error(e)),
        };
        if !on_frame(&mut state, &frame)? {
            return Ok(StreamEnd::Requested);
        }
    }
}

/// Live feed for the viewer: parses frames and forwards feed events until the
/// receiver hangs up or the stream ends.
pub fn live_feed(
    pair: &str,
    scale: Scale,
    tx: std::sync::mpsc::Sender<Result<FeedEvent>>,
) -> Result<()> {
    let end = stream(
        pair,
        |body| {
            let snap = parse_snapshot(body, scale)?;
            let FeedEvent::TruthSnapshot { ts, .. } = snap else {
                unreachable!()
            };
            tx.send(Ok(snap)).map_err(|_| anyhow!("viewer closed"))?;
            Ok(Parser::new(scale, ts))
        },
        |parser, frame| {
            Ok(match parser.parse(frame) {
                Ok(Some(ev)) => tx.send(Ok(FeedEvent::Truth(ev))).is_ok(),
                Ok(None) => true,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    false
                }
            })
        },
    )?;
    if let StreamEnd::Error(e) = end {
        let _ = tx.send(Err(e));
    }
    Ok(())
}

pub struct RecordOptions {
    pub pair: String,
    pub root: PathBuf,
    pub session: Option<String>,
    pub duration: Duration,
    pub scale: Scale,
}

/// Records a capture; returns its directory.
pub fn record(opts: &RecordOptions) -> Result<PathBuf> {
    let started_at_us = now_us();
    let session = opts
        .session
        .clone()
        .unwrap_or_else(|| format!("{}", started_at_us / 1000));
    let dir = opts.root.join("bitstamp").join(&opts.pair).join(session);
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;

    let mut meta = Metadata {
        venue: "bitstamp".into(),
        pair: opts.pair.clone(),
        scale: opts.scale,
        started_at_us,
        snapshot_ts: 0,
        ended_at_us: None,
        messages: 0,
        end_reason: None,
    };
    let mut out = BufWriter::new(File::create(dir.join("orders.jsonl"))?);
    let mut last_flush = Instant::now();
    let mut last_progress = Instant::now();
    let t0 = Instant::now();
    let mut chain = Parser::new(opts.scale, 0);
    let snap_ts = std::cell::Cell::new(0);
    let mut messages = 0u64;
    let mut invalid: Option<anyhow::Error> = None;

    let end = stream(
        &opts.pair,
        |body| {
            snap_ts.set(snapshot_ts(body)?);
            fs::write(dir.join("snapshot.json"), body)?;
            let mut m = meta.clone();
            m.snapshot_ts = snap_ts.get();
            write_metadata(&dir, &m)
        },
        |(), frame| {
            // Validate as we go: the capture stops at the first frame that would
            // break replay (chain gap, reconnect request), keeping everything before it.
            if let Err(e) = chain.parse(frame) {
                invalid = Some(e);
                return Ok(false);
            }
            out.write_all(frame.as_bytes())?;
            out.write_all(b"\n")?;
            messages += 1;
            if last_flush.elapsed() >= Duration::from_secs(1) {
                out.flush()?;
                last_flush = Instant::now();
            }
            if last_progress.elapsed() >= Duration::from_secs(10) {
                eprintln!(
                    "[record] {:>4}s {messages} messages",
                    t0.elapsed().as_secs()
                );
                last_progress = Instant::now();
            }
            Ok(t0.elapsed() < opts.duration)
        },
    );
    out.flush()?;
    meta.snapshot_ts = snap_ts.get();
    meta.messages = messages;
    meta.ended_at_us = Some(now_us());
    meta.end_reason = Some(match (&end, &invalid) {
        (_, Some(e)) => format!("stopped at invalid frame: {e:#}"),
        (Ok(StreamEnd::Requested), None) => "duration reached".into(),
        (Ok(StreamEnd::Error(e)), None) => format!("stream error: {e:#}"),
        (Err(e), None) => format!("error: {e:#}"),
    });
    write_metadata(&dir, &meta)?;
    let reason = meta.end_reason.as_deref().unwrap_or_default();
    match end {
        Ok(_) if meta.snapshot_ts > 0 => {
            eprintln!("[record] {messages} messages, {reason}");
            Ok(dir)
        }
        Ok(_) => bail!("no snapshot captured: {reason}"),
        Err(e) => Err(e),
    }
}

fn write_metadata(dir: &Path, meta: &Metadata) -> Result<()> {
    fs::write(
        dir.join("metadata.json"),
        serde_json::to_string_pretty(meta)?,
    )?;
    Ok(())
}

fn now_us() -> Ts {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as Ts)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DELETED: &str = r#"{"data":{"id":2063736692162560,"id_str":"2063736692162560","order_type":1,"order_subtype":4,"datetime":"1792676941","microtimestamp":"1792676940518000","amount":0,"amount_str":"0","amount_traded":"0.00006205","amount_at_create":"0.00006205","price":81406.6,"price_str":"81406.60","is_liquidation":false},"channel":"live_orders_btcusd","event":"order_deleted","event_id":"e1","pre_event_id":"e0","order_source":"orderbook"}"#;

    #[test]
    fn parses_frames_and_enforces_event_chain() {
        let mut p = Parser::new(DEFAULT_SCALE, 0);
        let ev = p.parse(DELETED).unwrap().unwrap();
        assert_eq!(
            ev,
            L3Event {
                ts: 1_792_676_940_518_000,
                msg: L3Msg::Delete {
                    id: 2_063_736_692_162_560,
                    traded_total: 6205
                }
            }
        );
        // Same frame again: its pre_event_id no longer matches the last event_id.
        let err = p.parse(DELETED).unwrap_err();
        assert!(format!("{err:#}").contains("chain broken"));
    }

    #[test]
    fn drops_events_covered_by_snapshot() {
        let mut p = Parser::new(DEFAULT_SCALE, 1_792_676_940_518_000);
        assert_eq!(p.parse(DELETED).unwrap(), None);
    }
}
