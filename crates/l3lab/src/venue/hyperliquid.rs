//! Hyperliquid: order-level ("L4") ground truth from a non-validating node through
//! [`order_book_server`](https://github.com/hyperliquid-dex/order_book_server)'s `l4Book`
//! channel, and the public API's `l2Book` (top levels with order counts) plus `trades`
//! as the venue's own L2 feed.
//!
//! # `l4Book` wire format (from the server source)
//! - After `{"method":"subscribe","subscription":{"type":"l4Book","coin":C}}` the server sends
//!   `subscriptionResponse`, then one
//!   `{"channel":"l4Book","data":{"Snapshot":{coin,time,height,levels:[bids,asks]}}}`.
//!   Bids run best (highest) price first, asks best (lowest) first, and within a level orders
//!   are in queue priority, front first (`OrderBook::to_snapshot` walks each level's linked
//!   list). `time` is the block time (ms) of `height`. Orders are camelCase `L4Order`s
//!   (`user, side "A"/"B", limitPx, sz, oid, timestamp` ms, `isTrigger`, `tif`, ...). The node
//!   snapshot is taken without trigger orders; untriggered ones are skipped here anyway.
//! - Then one `{"Updates":{time,height,order_statuses,book_diffs}}` per block that touched the
//!   coin. Blocks without activity for the coin are not sent, so heights legitimately skip;
//!   blocks at or below the snapshot height can still follow the snapshot (queued before it)
//!   and are dropped.
//! - `book_diffs` (`{user,oid,px,coin,raw_book_diff}`) are the book, in node order:
//!   `{"new":{"sz","insertBefore"?}}` rests an order (its size after taker execution) at the
//!   back of the level or directly in front of `insertBefore`; `{"update":{"origSz","newSz"}}`
//!   sets a resting order's size; `"remove"` takes it off. A `new` carries no side: like the
//!   server, it takes it from the block's opening status for that oid (`open` and neither
//!   trigger nor IOC, or `triggered`).
//!
//! Gap detection: since heights skip, the parser instead tracks every live oid (snapshot plus
//! diffs). A diff for an unknown oid, a `new` for a live oid or an unknown `insertBefore`
//! anchor means a block was missed and is an error, as is a height that does not increase.
//!
//! # Maker-fill attribution (heuristic)
//! `l4Book` carries no fills; exact attribution would need the node's `node_fills_by_block`.
//! Cumulative maker fills (`traded_total`) are inferred per oid from each block:
//! - an `update` that lowers the size at the same price is a fill of the decrease, unless the
//!   block also has a status for that oid other than `filled` and other than the opening status
//!   of a `new` earlier in the same block (a modify re-report or a cancel): then it is a size
//!   change without fill. Size increases and price changes are never fills;
//! - a `remove` is a fill of the remaining size if the block has a `filled` status for the oid,
//!   otherwise a cancel.
//!
//! Known misses: a partial fill followed by a cancel of the same order in one block counts the
//! fill as a size change, and fills of orders whose `filled` status lands in a later block than
//! their `remove` count as cancels.
//!
//! # Public feed
//! `l2Book` is subscribed with `"fast": true`, which publishes the top 5 levels per side every
//! ~0.5 s; the default stream publishes the top 20 but only every ~5.4 s (measured 2026-10;
//! `nLevels` is ignored), too coarse for queue inference. Every frame is a full snapshot of
//! the published window; the first becomes [`FeedEvent::L2Snapshot`], each later one an
//! [`L2Batch`] of the levels whose total or count changed plus the levels that left the window
//! (total 0, count 0: emptied or pushed out of view alike). `trades` are per taker
//! (`side` is the aggressor); each one with time in (previous frame, this frame] is booked
//! against the resting side in that frame's batch. A frame is emitted when the next one
//! arrives, so trades published just after the book frame of their block are still attributed;
//! trades older than the last emitted frame are dropped and counted.
//!
//! # Capture layout (`<root>/hyperliquid/<coin>/<session>/`)
//! `public.jsonl` (raw `l2Book` + `trades` frames from one socket, arrival order), optional
//! `l4.jsonl` (raw `l4Book` frames), `metadata.json`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Lines, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::l2::{L2Batch, LevelUpdate, Trade};
use crate::session::FeedEvent;
use crate::truth::{L3Event, L3Msg, SnapshotOrder};
use crate::types::{Owner, Px, Qty, Scale, Side, Ts};

pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
/// Perp prices carry at most `6 - szDecimals` decimals.
const PERP_MAX_DECIMALS: u32 = 6;
/// The public API closes sockets idle for 60 s.
const PING_EVERY: Duration = Duration::from_secs(30);
/// Socket read timeout: how often stop flags and deadlines are checked.
const POLL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub venue: String,
    pub coin: String,
    pub scale: Scale,
    pub started_at_us: Ts,
    pub ended_at_us: Option<Ts>,
    pub end_reason: Option<String>,
    pub public_messages: u64,
    pub l4_messages: u64,
    pub l4_server: Option<String>,
    /// The public `l2Book` subscription recorded (fixes depth and cadence).
    #[serde(default)]
    pub l2_subscription: String,
}

/// Fixed-point scale of a perp, from the `meta` info request.
pub fn fetch_scale(coin: &str) -> Result<Scale> {
    #[derive(Deserialize)]
    struct Meta {
        universe: Vec<Asset>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Asset {
        name: String,
        sz_decimals: u32,
    }
    let body = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?
        .post(INFO_URL)
        .header("content-type", "application/json")
        .body(r#"{"type":"meta"}"#)
        .send()
        .with_context(|| format!("POST {INFO_URL}"))?
        .error_for_status()?
        .text()?;
    let meta: Meta = serde_json::from_str(&body).context("parse meta")?;
    let asset = meta
        .universe
        .iter()
        .find(|a| a.name == coin)
        .ok_or_else(|| anyhow!("{coin} is not a Hyperliquid perp"))?;
    Ok(Scale {
        px_decimals: PERP_MAX_DECIMALS.saturating_sub(asset.sz_decimals),
        qty_decimals: asset.sz_decimals,
    })
}

#[derive(Deserialize)]
struct Envelope<'a> {
    channel: &'a str,
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
}

impl<'a> Envelope<'a> {
    fn parse(frame: &'a str) -> Result<Self> {
        let env: Self = serde_json::from_str(frame).context("parse hyperliquid frame")?;
        if env.channel == "error" {
            bail!("server error: {}", env.data.map_or("", RawValue::get));
        }
        Ok(env)
    }

    fn data(&self) -> Result<&'a str> {
        self.data
            .map(RawValue::get)
            .ok_or_else(|| anyhow!("{} frame without data", self.channel))
    }
}

/// Turns raw frames of one socket into feed events.
pub trait FrameParser {
    /// Appends the frame's events to `out`; returns whether it was a data frame (worth
    /// recording) rather than a control one (subscription ack, pong).
    fn parse(&mut self, frame: &str, out: &mut Vec<FeedEvent>) -> Result<bool>;
    /// Flushes anything held back at end of input.
    fn finish(&mut self, out: &mut Vec<FeedEvent>);
}

fn parse_side(raw: &str) -> Result<Side> {
    match raw {
        "B" => Ok(Side::Bid),
        "A" => Ok(Side::Ask),
        other => bail!("unknown side {other:?}"),
    }
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Bid => Side::Ask,
        Side::Ask => Side::Bid,
    }
}

// ---------------------------------------------------------------- l4Book

#[derive(Deserialize)]
enum L4Book<'a> {
    #[serde(borrow)]
    Snapshot(L4Snapshot<'a>),
    #[serde(borrow)]
    Updates(L4Updates<'a>),
}

#[derive(Deserialize)]
struct L4Snapshot<'a> {
    time: u64,
    height: u64,
    #[serde(borrow)]
    levels: [Vec<WireOrder<'a>>; 2],
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireOrder<'a> {
    /// Set in snapshots; null inside order statuses, which carry it outside.
    #[serde(borrow, default)]
    user: Option<&'a str>,
    side: &'a str,
    limit_px: &'a str,
    sz: &'a str,
    oid: u64,
    timestamp: u64,
    is_trigger: bool,
    #[serde(borrow, default)]
    tif: Option<&'a str>,
}

#[derive(Deserialize)]
struct L4Updates<'a> {
    time: u64,
    height: u64,
    #[serde(borrow)]
    order_statuses: Vec<WireStatus<'a>>,
    #[serde(borrow)]
    book_diffs: Vec<WireDiff<'a>>,
}

#[derive(Deserialize)]
struct WireStatus<'a> {
    status: &'a str,
    #[serde(borrow)]
    order: WireOrder<'a>,
}

impl WireStatus<'_> {
    /// The server's `is_inserted_into_book`: the status that goes with a `new` diff.
    fn opens(&self) -> bool {
        (self.status == "open" && !self.order.is_trigger && self.order.tif != Some("Ioc"))
            || (self.order.is_trigger && self.status == "triggered")
    }
}

#[derive(Deserialize)]
struct WireDiff<'a> {
    user: &'a str,
    oid: u64,
    px: &'a str,
    #[serde(borrow)]
    raw_book_diff: RawDiff<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum RawDiff<'a> {
    #[serde(rename_all = "camelCase")]
    New {
        sz: &'a str,
        #[serde(default)]
        insert_before: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: &'a str,
        new_sz: &'a str,
    },
    Remove,
}

#[derive(Clone, Copy, Debug)]
struct Live {
    side: Side,
    px: Px,
    qty: Qty,
    traded: Qty,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct L4Stats {
    /// Blocks applied after the snapshot.
    pub blocks: u64,
    /// Blocks at or below the snapshot height (already in it).
    pub stale_blocks: u64,
    /// `update` diffs whose `origSz` differs from the tracked size.
    pub size_mismatches: u64,
}

/// Stateful `l4Book` parser: tracks live orders for gap detection and fill attribution.
pub struct L4Parser {
    scale: Scale,
    snapshot_height: Option<u64>,
    last_height: u64,
    live: HashMap<u64, Live>,
    stats: L4Stats,
}

impl L4Parser {
    pub fn new(scale: Scale) -> Self {
        Self {
            scale,
            snapshot_height: None,
            last_height: 0,
            live: HashMap::new(),
            stats: L4Stats::default(),
        }
    }

    pub fn stats(&self) -> L4Stats {
        self.stats
    }

    fn snapshot(&mut self, snap: L4Snapshot<'_>, out: &mut Vec<FeedEvent>) -> Result<()> {
        self.live.clear();
        let mut orders = Vec::with_capacity(snap.levels[0].len() + snap.levels[1].len());
        for (side, rows) in [(Side::Bid, &snap.levels[0]), (Side::Ask, &snap.levels[1])] {
            for o in rows {
                if o.is_trigger {
                    continue;
                }
                if parse_side(o.side)? != side {
                    bail!(
                        "order {} listed on the {side:?} side has side {}",
                        o.oid,
                        o.side
                    );
                }
                let (px, qty) = (self.scale.px(o.limit_px)?, self.scale.qty(o.sz)?);
                let owner = o.user.map(Owner::parse).transpose()?;
                self.live.insert(
                    o.oid,
                    Live {
                        side,
                        px,
                        qty,
                        traded: 0,
                    },
                );
                orders.push(SnapshotOrder {
                    id: o.oid,
                    side,
                    px,
                    qty,
                    born: o.timestamp * 1000,
                    owner,
                });
            }
        }
        self.snapshot_height = Some(snap.height);
        self.last_height = snap.height;
        out.push(FeedEvent::TruthSnapshot {
            ts: snap.time * 1000,
            orders,
        });
        Ok(())
    }

    fn block(&mut self, u: L4Updates<'_>, out: &mut Vec<FeedEvent>) -> Result<()> {
        let Some(snapshot_height) = self.snapshot_height else {
            bail!("l4Book update before the snapshot");
        };
        if u.height <= snapshot_height {
            self.stats.stale_blocks += 1;
            return Ok(());
        }
        if u.height <= self.last_height {
            bail!(
                "l4Book height {} does not follow {}",
                u.height,
                self.last_height
            );
        }
        self.last_height = u.height;
        self.stats.blocks += 1;

        let ts = u.time * 1000;
        let mut statuses: HashMap<u64, Vec<&WireStatus>> = HashMap::new();
        for s in &u.order_statuses {
            statuses.entry(s.order.oid).or_default().push(s);
        }
        let mut opened: HashSet<u64> = HashSet::new();
        let status_of = |oid: u64| statuses.get(&oid).map_or(&[][..], Vec::as_slice);

        for d in &u.book_diffs {
            let id = d.oid;
            let px = self.scale.px(d.px)?;
            let msg = match d.raw_book_diff {
                RawDiff::New { sz, insert_before } => {
                    if self.live.contains_key(&id) {
                        bail!("new for live order {id}: a block was missed");
                    }
                    if let Some(anchor) = insert_before
                        && self.live.get(&anchor).is_none_or(|a| a.px != px)
                    {
                        bail!("insertBefore {anchor} is not resting at {}", d.px);
                    }
                    let status = status_of(id)
                        .iter()
                        .rev()
                        .find(|s| s.opens())
                        .ok_or_else(|| anyhow!("new for order {id} without an opening status"))?;
                    let side = parse_side(status.order.side)?;
                    let qty = self.scale.qty(sz)?;
                    opened.insert(id);
                    if qty == 0 {
                        continue;
                    }
                    self.live.insert(
                        id,
                        Live {
                            side,
                            px,
                            qty,
                            traded: 0,
                        },
                    );
                    L3Msg::Add {
                        id,
                        side,
                        px,
                        qty,
                        owner: Some(Owner::parse(d.user)?),
                        insert_before,
                    }
                }
                RawDiff::Update { orig_sz, new_sz } => {
                    let live = self.live.get_mut(&id).ok_or_else(|| {
                        anyhow!("update for unknown order {id}: a block was missed")
                    })?;
                    if self.scale.qty(orig_sz)? != live.qty {
                        self.stats.size_mismatches += 1;
                    }
                    let remaining = self.scale.qty(new_sz)?;
                    let modified = status_of(id)
                        .iter()
                        .any(|s| s.status != "filled" && !(opened.contains(&id) && s.opens()));
                    if px == live.px && !modified {
                        live.traded += live.qty.saturating_sub(remaining);
                    }
                    live.qty = remaining;
                    live.px = px;
                    let (side, traded_total) = (live.side, live.traded);
                    if remaining == 0 {
                        self.live.remove(&id);
                    }
                    L3Msg::Update {
                        id,
                        side,
                        px,
                        remaining,
                        traded_total,
                    }
                }
                RawDiff::Remove => {
                    let live = self.live.remove(&id).ok_or_else(|| {
                        anyhow!("remove for unknown order {id}: a block was missed")
                    })?;
                    let filled = status_of(id).iter().any(|s| s.status == "filled");
                    L3Msg::Delete {
                        id,
                        traded_total: live.traded + if filled { live.qty } else { 0 },
                    }
                }
            };
            out.push(FeedEvent::Truth(L3Event { ts, msg }));
        }
        Ok(())
    }
}

impl FrameParser for L4Parser {
    fn parse(&mut self, frame: &str, out: &mut Vec<FeedEvent>) -> Result<bool> {
        let env = Envelope::parse(frame)?;
        if env.channel != "l4Book" {
            return Ok(false);
        }
        match serde_json::from_str(env.data()?).context("parse l4Book data")? {
            L4Book::Snapshot(snap) => self.snapshot(snap, out)?,
            L4Book::Updates(u) => self.block(u, out)?,
        }
        Ok(true)
    }

    fn finish(&mut self, _out: &mut Vec<FeedEvent>) {}
}

// ---------------------------------------------------------------- public l2Book + trades

#[derive(Deserialize)]
struct WireBook<'a> {
    time: u64,
    #[serde(borrow)]
    levels: [Vec<WireLevel<'a>>; 2],
}

#[derive(Deserialize)]
struct WireLevel<'a> {
    px: &'a str,
    sz: &'a str,
    n: u32,
}

#[derive(Deserialize)]
struct WireTrade<'a> {
    side: &'a str,
    px: &'a str,
    sz: &'a str,
    time: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublicStats {
    pub book_frames: u64,
    /// Book frames not newer than the previous one (skipped).
    pub stale_frames: u64,
    pub trades: u64,
    /// Trades older than the last emitted book frame (dropped).
    pub late_trades: u64,
}

struct BookFrame {
    time_ms: u64,
    levels: Vec<LevelUpdate>,
}

/// Stateful parser for the public `l2Book` + `trades` socket.
pub struct PublicParser {
    scale: Scale,
    /// Levels of the last emitted frame: `(total, count)`.
    book: BTreeMap<(Side, Px), (Qty, u32)>,
    /// Time of the last emitted frame; `None` before the snapshot.
    emitted_ms: Option<u64>,
    pending: Option<BookFrame>,
    /// Trades not yet attributed: `(time ms, maker-side trade)`.
    trades: Vec<(u64, Trade)>,
    stats: PublicStats,
}

impl PublicParser {
    pub fn new(scale: Scale) -> Self {
        Self {
            scale,
            book: BTreeMap::new(),
            emitted_ms: None,
            pending: None,
            trades: Vec::new(),
            stats: PublicStats::default(),
        }
    }

    pub fn stats(&self) -> PublicStats {
        self.stats
    }

    fn on_book(&mut self, book: WireBook<'_>, out: &mut Vec<FeedEvent>) -> Result<()> {
        self.stats.book_frames += 1;
        let newest = self.pending.as_ref().map(|p| p.time_ms).or(self.emitted_ms);
        if newest.is_some_and(|t| book.time <= t) {
            self.stats.stale_frames += 1;
            return Ok(());
        }
        let mut levels = Vec::with_capacity(book.levels[0].len() + book.levels[1].len());
        for (side, rows) in [(Side::Bid, &book.levels[0]), (Side::Ask, &book.levels[1])] {
            for l in rows {
                levels.push(LevelUpdate {
                    side,
                    px: self.scale.px(l.px)?,
                    total: self.scale.qty(l.sz)?,
                    count: Some(l.n),
                });
            }
        }
        let frame = BookFrame {
            time_ms: book.time,
            levels,
        };
        if self.emitted_ms.is_none() {
            self.trades.retain(|(t, _)| *t > frame.time_ms);
            self.book = frame
                .levels
                .iter()
                .map(|l| ((l.side, l.px), (l.total, l.count.unwrap_or(0))))
                .collect();
            self.emitted_ms = Some(frame.time_ms);
            out.push(FeedEvent::L2Snapshot {
                ts: frame.time_ms * 1000,
                levels: frame.levels,
            });
        } else {
            if let Some(prev) = self.pending.take() {
                self.emit(prev, out);
            }
            self.pending = Some(frame);
        }
        Ok(())
    }

    fn on_trades(&mut self, trades: Vec<WireTrade<'_>>) -> Result<()> {
        for t in trades {
            self.stats.trades += 1;
            if self.emitted_ms.is_some_and(|e| t.time <= e) {
                self.stats.late_trades += 1;
                continue;
            }
            self.trades.push((
                t.time,
                Trade {
                    side: opposite(parse_side(t.side)?),
                    px: self.scale.px(t.px)?,
                    qty: self.scale.qty(t.sz)?,
                },
            ));
        }
        Ok(())
    }

    fn emit(&mut self, frame: BookFrame, out: &mut Vec<FeedEvent>) {
        let mut next = BTreeMap::new();
        let mut levels = Vec::new();
        for l in frame.levels {
            let key = (l.side, l.px);
            let value = (l.total, l.count.unwrap_or(0));
            if self.book.get(&key) != Some(&value) {
                levels.push(l);
            }
            next.insert(key, value);
        }
        for &(side, px) in self.book.keys() {
            if !next.contains_key(&(side, px)) {
                levels.push(LevelUpdate {
                    side,
                    px,
                    total: 0,
                    count: Some(0),
                });
            }
        }
        self.book = next;

        let mut trades: Vec<Trade> = Vec::new();
        self.trades.retain(|(time, t)| {
            if *time > frame.time_ms {
                return true;
            }
            match trades.iter_mut().find(|a| a.side == t.side && a.px == t.px) {
                Some(agg) => agg.qty += t.qty,
                None => trades.push(*t),
            }
            false
        });
        self.emitted_ms = Some(frame.time_ms);
        if levels.is_empty() && trades.is_empty() {
            return;
        }
        let ts = frame.time_ms * 1000;
        out.push(FeedEvent::L2(L2Batch {
            seq: 0,
            start: ts,
            end: ts,
            levels,
            trades,
        }));
    }
}

impl FrameParser for PublicParser {
    fn parse(&mut self, frame: &str, out: &mut Vec<FeedEvent>) -> Result<bool> {
        let env = Envelope::parse(frame)?;
        match env.channel {
            "l2Book" => {
                let book = serde_json::from_str(env.data()?).context("parse l2Book data")?;
                self.on_book(book, out)?;
            }
            "trades" => {
                let trades = serde_json::from_str(env.data()?).context("parse trades data")?;
                self.on_trades(trades)?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn finish(&mut self, out: &mut Vec<FeedEvent>) {
        if let Some(frame) = self.pending.take() {
            self.emit(frame, out);
        }
    }
}

// ---------------------------------------------------------------- capture replay

/// Events of one jsonl file, parsed lazily.
struct JsonlEvents<P> {
    name: &'static str,
    lines: Lines<BufReader<File>>,
    line_no: usize,
    parser: P,
    queue: VecDeque<FeedEvent>,
    buf: Vec<FeedEvent>,
    done: bool,
}

impl<P: FrameParser> JsonlEvents<P> {
    fn open(dir: &Path, name: &'static str, parser: P) -> Result<Self> {
        let file =
            File::open(dir.join(name)).with_context(|| format!("open {}/{name}", dir.display()))?;
        Ok(Self {
            name,
            lines: BufReader::new(file).lines(),
            line_no: 0,
            parser,
            queue: VecDeque::new(),
            buf: Vec::new(),
            done: false,
        })
    }

    /// Makes `queue` hold the next event unless the file is exhausted.
    fn fill(&mut self) -> Result<()> {
        while self.queue.is_empty() && !self.done {
            match self.lines.next() {
                None => {
                    self.done = true;
                    self.parser.finish(&mut self.buf);
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Err(e.into());
                }
                Some(Ok(line)) => {
                    self.line_no += 1;
                    if let Err(e) = self.parser.parse(&line, &mut self.buf) {
                        self.buf.clear();
                        self.done = true;
                        // A recorder killed mid-write leaves a torn final line.
                        if self.lines.next().is_some() {
                            return Err(e.context(format!("{} line {}", self.name, self.line_no)));
                        }
                        self.parser.finish(&mut self.buf);
                    }
                }
            }
            self.queue.extend(self.buf.drain(..));
        }
        Ok(())
    }
}

fn event_ts(ev: &FeedEvent) -> Ts {
    match ev {
        FeedEvent::TruthSnapshot { ts, .. } | FeedEvent::L2Snapshot { ts, .. } => *ts,
        FeedEvent::Truth(ev) => ev.ts,
        FeedEvent::L2(batch) => batch.end,
    }
}

/// Replays a capture as one time-ordered stream: L4 events at time `T` or earlier come
/// before a public event at `T`.
pub struct CaptureReader {
    public: JsonlEvents<PublicParser>,
    l4: Option<JsonlEvents<L4Parser>>,
    failed: bool,
}

impl CaptureReader {
    pub fn open(dir: &Path, scale: Scale) -> Result<Self> {
        Ok(Self {
            public: JsonlEvents::open(dir, "public.jsonl", PublicParser::new(scale))?,
            l4: if has_l4(dir) {
                Some(JsonlEvents::open(dir, "l4.jsonl", L4Parser::new(scale))?)
            } else {
                None
            },
            failed: false,
        })
    }

    pub fn read_metadata(dir: &Path) -> Result<Metadata> {
        let raw = fs::read_to_string(dir.join("metadata.json"))
            .with_context(|| format!("read {}/metadata.json", dir.display()))?;
        Ok(serde_json::from_str(&raw)?)
    }

    fn advance(&mut self) -> Result<Option<FeedEvent>> {
        self.public.fill()?;
        let Some(l4) = self.l4.as_mut() else {
            return Ok(self.public.queue.pop_front());
        };
        l4.fill()?;
        let take_l4 = match (l4.queue.front(), self.public.queue.front()) {
            (Some(a), Some(b)) => event_ts(a) <= event_ts(b),
            (a, _) => a.is_some(),
        };
        Ok(if take_l4 {
            l4.queue.pop_front()
        } else {
            self.public.queue.pop_front()
        })
    }
}

/// Whether a capture directory holds L4 frames.
pub fn has_l4(dir: &Path) -> bool {
    fs::metadata(dir.join("l4.jsonl")).is_ok_and(|m| m.len() > 0)
}

impl Iterator for CaptureReader {
    type Item = Result<FeedEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let next = self.advance();
        self.failed = next.is_err();
        next.transpose()
    }
}

// ---------------------------------------------------------------- sockets

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

/// How a socket is kept alive.
#[derive(Clone, Copy)]
enum KeepAlive {
    /// Public API: `{"method":"ping"}`, answered with a `pong` channel frame.
    AppPing,
    /// order_book_server only parses subscribe/unsubscribe: protocol-level pings.
    FramePing,
}

fn l2_subscription(coin: &str) -> String {
    format!(r#"{{"type":"l2Book","coin":"{coin}","fast":true}}"#)
}

fn public_subscriptions(coin: &str) -> Vec<String> {
    vec![
        format!(
            r#"{{"method":"subscribe","subscription":{}}}"#,
            l2_subscription(coin)
        ),
        format!(r#"{{"method":"subscribe","subscription":{{"type":"trades","coin":"{coin}"}}}}"#),
    ]
}

fn l4_subscriptions(coin: &str) -> Vec<String> {
    vec![format!(
        r#"{{"method":"subscribe","subscription":{{"type":"l4Book","coin":"{coin}"}}}}"#
    )]
}

fn connect(url: &str, subscriptions: &[String]) -> Result<Socket> {
    let (mut ws, _) = tungstenite::connect(url).with_context(|| format!("connect {url}"))?;
    let tcp = match ws.get_ref() {
        MaybeTlsStream::Plain(s) => s,
        MaybeTlsStream::Rustls(s) => s.get_ref(),
        _ => bail!("unsupported websocket stream for {url}"),
    };
    tcp.set_read_timeout(Some(POLL))?;
    for sub in subscriptions {
        ws.send(Message::Text(sub.clone()))?;
    }
    Ok(ws)
}

/// Why a stream stopped.
enum StreamEnd {
    /// `on_frame` asked to stop or the deadline passed.
    Requested,
    /// Another stream set the stop flag.
    Stopped,
    Error(anyhow::Error),
}

/// Hands every text frame to `on_frame` until it returns `false`, `until` passes, `stop` is
/// set or the socket fails, keeping the socket alive meanwhile.
fn pump(
    ws: &mut Socket,
    keep_alive: KeepAlive,
    until: Option<Instant>,
    stop: &AtomicBool,
    mut on_frame: impl FnMut(&str) -> Result<bool>,
) -> Result<StreamEnd> {
    let mut last_ping = Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(StreamEnd::Stopped);
        }
        if until.is_some_and(|t| Instant::now() >= t) {
            return Ok(StreamEnd::Requested);
        }
        if last_ping.elapsed() >= PING_EVERY {
            let ping = match keep_alive {
                KeepAlive::AppPing => Message::Text(r#"{"method":"ping"}"#.into()),
                KeepAlive::FramePing => Message::Ping(Vec::new()),
            };
            if let Err(e) = ws.send(ping) {
                return Ok(StreamEnd::Error(e.into()));
            }
            last_ping = Instant::now();
        }
        match ws.read() {
            Ok(Message::Text(frame)) => {
                if !on_frame(&frame)? {
                    return Ok(StreamEnd::Requested);
                }
            }
            Ok(Message::Close(frame)) => {
                return Ok(StreamEnd::Error(anyhow!("websocket closed: {frame:?}")));
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) => return Ok(StreamEnd::Error(e.into())),
        }
    }
}

/// Parses one socket's frames and forwards its events until the receiver hangs up or the
/// stream ends.
fn live(
    url: &str,
    subscriptions: &[String],
    keep_alive: KeepAlive,
    mut parser: impl FrameParser,
    tx: Sender<Result<FeedEvent>>,
) -> Result<()> {
    let mut ws = connect(url, subscriptions)?;
    let mut out = Vec::new();
    let end = pump(
        &mut ws,
        keep_alive,
        None,
        &AtomicBool::new(false),
        |frame| {
            Ok(match parser.parse(frame, &mut out) {
                Ok(_) => out.drain(..).all(|ev| tx.send(Ok(ev)).is_ok()),
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

/// Live public feed for the viewer: L2 snapshot and batches (with trades) only.
pub fn live_public(coin: &str, scale: Scale, tx: Sender<Result<FeedEvent>>) -> Result<()> {
    live(
        WS_URL,
        &public_subscriptions(coin),
        KeepAlive::AppPing,
        PublicParser::new(scale),
        tx,
    )
}

/// Live L4 truth from an `order_book_server` (e.g. `ws://host:8000/ws`): truth events only.
pub fn live_l4(
    server_url: &str,
    coin: &str,
    scale: Scale,
    tx: Sender<Result<FeedEvent>>,
) -> Result<()> {
    live(
        server_url,
        &l4_subscriptions(coin),
        KeepAlive::FramePing,
        L4Parser::new(scale),
        tx,
    )
}

// ---------------------------------------------------------------- recorder

pub struct RecordOptions {
    pub coin: String,
    pub root: PathBuf,
    pub session: Option<String>,
    pub duration: Duration,
    /// `order_book_server` websocket URL; without it only the public feed is recorded.
    pub l4_server: Option<String>,
}

struct Recorded {
    messages: u64,
    end: String,
}

/// Records one socket's data frames into `path`, validating them as it goes: recording
/// stops at the first frame that would break replay, keeping everything before it. Sets
/// `stop` when it ends so the other stream ends too.
#[allow(clippy::too_many_arguments)]
fn record_stream(
    label: &str,
    url: &str,
    subscriptions: &[String],
    keep_alive: KeepAlive,
    mut parser: impl FrameParser,
    path: &Path,
    until: Instant,
    stop: &AtomicBool,
) -> Result<Recorded> {
    let result = (|| -> Result<Recorded> {
        let mut ws = connect(url, subscriptions)?;
        let mut out = BufWriter::new(
            File::create(path).with_context(|| format!("create {}", path.display()))?,
        );
        let mut scratch = Vec::new();
        let mut messages = 0u64;
        let mut invalid: Option<anyhow::Error> = None;
        let mut last_flush = Instant::now();
        let mut last_progress = Instant::now();
        let t0 = Instant::now();
        let end = pump(&mut ws, keep_alive, Some(until), stop, |frame| {
            scratch.clear();
            match parser.parse(frame, &mut scratch) {
                Ok(false) => return Ok(true),
                Ok(true) => {}
                Err(e) => {
                    invalid = Some(e);
                    return Ok(false);
                }
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
                    "[record-hl] {label} {:>4}s {messages} frames",
                    t0.elapsed().as_secs()
                );
                last_progress = Instant::now();
            }
            Ok(true)
        });
        out.flush()?;
        let end = match (end, invalid) {
            (_, Some(e)) => format!("stopped at invalid frame: {e:#}"),
            (Ok(StreamEnd::Requested), None) => "duration reached".into(),
            (Ok(StreamEnd::Stopped), None) => "stopped with the other stream".into(),
            (Ok(StreamEnd::Error(e)), None) => format!("stream error: {e:#}"),
            (Err(e), None) => format!("error: {e:#}"),
        };
        Ok(Recorded { messages, end })
    })();
    stop.store(true, Ordering::Relaxed);
    result
}

/// Records a capture; returns its directory.
pub fn record(opts: &RecordOptions) -> Result<PathBuf> {
    let scale = fetch_scale(&opts.coin)?;
    let started_at_us = now_us();
    let session = opts
        .session
        .clone()
        .unwrap_or_else(|| format!("{}", started_at_us / 1000));
    let dir = opts.root.join("hyperliquid").join(&opts.coin).join(session);
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let mut meta = Metadata {
        venue: "hyperliquid".into(),
        coin: opts.coin.clone(),
        scale,
        started_at_us,
        ended_at_us: None,
        end_reason: None,
        public_messages: 0,
        l4_messages: 0,
        l4_server: opts.l4_server.clone(),
        l2_subscription: l2_subscription(&opts.coin),
    };
    write_metadata(&dir, &meta)?;
    eprintln!(
        "[record-hl] {} px_decimals={} qty_decimals={} -> {}",
        opts.coin,
        scale.px_decimals,
        scale.qty_decimals,
        dir.display()
    );

    let until = Instant::now() + opts.duration;
    let stop = AtomicBool::new(false);
    let (public, l4) = std::thread::scope(|s| {
        let l4 = opts.l4_server.as_deref().map(|url| {
            let (dir, stop, coin) = (&dir, &stop, &opts.coin);
            s.spawn(move || {
                record_stream(
                    "l4",
                    url,
                    &l4_subscriptions(coin),
                    KeepAlive::FramePing,
                    L4Parser::new(scale),
                    &dir.join("l4.jsonl"),
                    until,
                    stop,
                )
            })
        });
        let public = record_stream(
            "public",
            WS_URL,
            &public_subscriptions(&opts.coin),
            KeepAlive::AppPing,
            PublicParser::new(scale),
            &dir.join("public.jsonl"),
            until,
            &stop,
        );
        let l4 = l4.map(|h| {
            h.join()
                .unwrap_or_else(|_| Err(anyhow!("l4 recorder panicked")))
        });
        (public, l4)
    });

    let describe = |r: &Result<Recorded>| match r {
        Ok(r) => r.end.clone(),
        Err(e) => format!("error: {e:#}"),
    };
    meta.ended_at_us = Some(now_us());
    meta.public_messages = public.as_ref().map_or(0, |r| r.messages);
    meta.l4_messages = l4
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .map_or(0, |r| r.messages);
    let mut reason = format!("public: {}", describe(&public));
    if let Some(l4) = &l4 {
        reason.push_str(&format!("; l4: {}", describe(l4)));
    }
    meta.end_reason = Some(reason.clone());
    write_metadata(&dir, &meta)?;
    eprintln!(
        "[record-hl] public {} frames, l4 {} frames; {reason}",
        meta.public_messages, meta.l4_messages
    );
    public?;
    if let Some(l4) = l4 {
        l4?;
    }
    if meta.public_messages == 0 {
        bail!("no public frames captured: {reason}");
    }
    Ok(dir)
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

    const BTC: Scale = Scale {
        px_decimals: 1,
        qty_decimals: 5,
    };
    const ALICE: &str = "0x31ca8395cf837de08b24da3f660e77761dfb974b";
    const BOB: &str = "0xEcB63caA47c7c4E77F60f1cE858Cf28dC2B82b00";

    fn order(
        user: Option<&str>,
        side: &str,
        px: &str,
        sz: &str,
        oid: u64,
        trigger: bool,
    ) -> String {
        let user = user.map_or("null".to_owned(), |u| format!("\"{u}\""));
        format!(
            r#"{{"user":{user},"coin":"BTC","side":"{side}","limitPx":"{px}","sz":"{sz}","oid":{oid},"timestamp":1792680000123,"triggerCondition":"N/A","isTrigger":{trigger},"triggerPx":"0.0","isPositionTpsl":false,"reduceOnly":false,"orderType":"Limit","tif":"Gtc","cloid":null}}"#
        )
    }

    fn snapshot_frame() -> String {
        let bids = [
            order(Some(ALICE), "B", "81676", "0.5", 11, false),
            order(Some(BOB), "B", "81676", "0.25", 12, false),
            order(Some(ALICE), "B", "81675.5", "1", 13, false),
        ];
        let asks = [
            order(Some(BOB), "A", "81677", "0.1", 21, false),
            order(Some(BOB), "A", "81690", "0.2", 22, true),
        ];
        format!(
            r#"{{"channel":"l4Book","data":{{"Snapshot":{{"coin":"BTC","time":1792680001000,"height":900,"levels":[[{}],[{}]]}}}}}}"#,
            bids.join(","),
            asks.join(",")
        )
    }

    fn status(user: &str, status: &str, order_json: &str) -> String {
        format!(
            r#"{{"time":"2026-10-22T10:00:01.500123","user":"{user}","status":"{status}","order":{order_json}}}"#
        )
    }

    fn diff(user: &str, oid: u64, px: &str, raw: &str) -> String {
        format!(r#"{{"user":"{user}","oid":{oid},"px":"{px}","coin":"BTC","raw_book_diff":{raw}}}"#)
    }

    fn updates_frame(height: u64, time: u64, statuses: &[String], diffs: &[String]) -> String {
        format!(
            r#"{{"channel":"l4Book","data":{{"Updates":{{"time":{time},"height":{height},"order_statuses":[{}],"book_diffs":[{}]}}}}}}"#,
            statuses.join(","),
            diffs.join(",")
        )
    }

    fn truth_msgs(events: &[FeedEvent]) -> Vec<L3Msg> {
        events
            .iter()
            .map(|e| match e {
                FeedEvent::Truth(ev) => ev.msg.clone(),
                other => panic!("expected a truth event, got {other:?}"),
            })
            .collect()
    }

    fn seeded() -> L4Parser {
        let mut p = L4Parser::new(BTC);
        let mut out = Vec::new();
        assert!(p.parse(&snapshot_frame(), &mut out).unwrap());
        p
    }

    #[test]
    fn snapshot_keeps_priority_owners_and_skips_triggers() {
        let mut p = L4Parser::new(BTC);
        let mut out = Vec::new();
        assert!(!p.parse(r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}}"#, &mut out).unwrap());
        p.parse(&snapshot_frame(), &mut out).unwrap();
        let [FeedEvent::TruthSnapshot { ts, orders }] = out.as_slice() else {
            panic!("expected one snapshot, got {out:?}");
        };
        assert_eq!(*ts, 1_792_680_001_000_000);
        let rows: Vec<_> = orders
            .iter()
            .map(|o| (o.id, o.side, o.px, o.qty, o.owner.unwrap().to_string()))
            .collect();
        assert_eq!(
            rows,
            vec![
                (11, Side::Bid, 816_760, 50_000, ALICE.to_owned()),
                (12, Side::Bid, 816_760, 25_000, BOB.to_lowercase()),
                (13, Side::Bid, 816_755, 100_000, ALICE.to_owned()),
                (21, Side::Ask, 816_770, 10_000, BOB.to_lowercase()),
            ]
        );
        assert_eq!(orders[0].born, 1_792_680_000_123_000);
    }

    #[test]
    fn maps_diffs_and_attributes_fills() {
        let mut p = seeded();
        let mut out = Vec::new();
        let new_order = order(None, "B", "81676", "0.3", 30, false);
        let frame = updates_frame(
            901,
            1_792_680_002_000,
            &[
                status(ALICE, "open", &new_order),
                // 12 was modified down (re-reported open); 21 was canceled; 13 filled out.
                status(BOB, "open", &order(None, "B", "81676", "0.1", 12, false)),
                status(
                    BOB,
                    "canceled",
                    &order(None, "A", "81677", "0.1", 21, false),
                ),
                status(
                    ALICE,
                    "filled",
                    &order(None, "B", "81675.5", "0", 13, false),
                ),
            ],
            &[
                diff(
                    ALICE,
                    30,
                    "81676",
                    r#"{"new":{"sz":"0.3","insertBefore":12}}"#,
                ),
                // Partial fill of 11: no status for it.
                diff(
                    ALICE,
                    11,
                    "81676",
                    r#"{"update":{"origSz":"0.5","newSz":"0.2"}}"#,
                ),
                // Partial fill of the just-placed 30: its own opening status is not a modify.
                diff(
                    ALICE,
                    30,
                    "81676",
                    r#"{"update":{"origSz":"0.3","newSz":"0.25"}}"#,
                ),
                diff(
                    BOB,
                    12,
                    "81676",
                    r#"{"update":{"origSz":"0.25","newSz":"0.1"}}"#,
                ),
                diff(ALICE, 13, "81675.5", r#""remove""#),
                diff(BOB, 21, "81677", r#""remove""#),
            ],
        );
        p.parse(&frame, &mut out).unwrap();
        assert!(
            out.iter()
                .all(|e| matches!(e, FeedEvent::Truth(ev) if ev.ts == 1_792_680_002_000_000))
        );
        assert_eq!(
            truth_msgs(&out),
            vec![
                L3Msg::Add {
                    id: 30,
                    side: Side::Bid,
                    px: 816_760,
                    qty: 30_000,
                    owner: Some(Owner::parse(ALICE).unwrap()),
                    insert_before: Some(12),
                },
                L3Msg::Update {
                    id: 11,
                    side: Side::Bid,
                    px: 816_760,
                    remaining: 20_000,
                    traded_total: 30_000,
                },
                L3Msg::Update {
                    id: 30,
                    side: Side::Bid,
                    px: 816_760,
                    remaining: 25_000,
                    traded_total: 5_000,
                },
                L3Msg::Update {
                    id: 12,
                    side: Side::Bid,
                    px: 816_760,
                    remaining: 10_000,
                    traded_total: 0,
                },
                L3Msg::Delete {
                    id: 13,
                    traded_total: 100_000,
                },
                L3Msg::Delete {
                    id: 21,
                    traded_total: 0,
                },
            ]
        );

        // Cumulative: a later fill of 11 adds to the earlier one.
        out.clear();
        let frame = updates_frame(
            905,
            1_792_680_003_000,
            &[status(
                ALICE,
                "filled",
                &order(None, "B", "81676", "0", 11, false),
            )],
            &[diff(ALICE, 11, "81676", r#""remove""#)],
        );
        p.parse(&frame, &mut out).unwrap();
        assert_eq!(
            truth_msgs(&out),
            vec![L3Msg::Delete {
                id: 11,
                traded_total: 50_000,
            }]
        );
        assert_eq!(p.stats().size_mismatches, 0);
    }

    #[test]
    fn detects_missed_blocks_and_drops_stale_ones() {
        let mut p = seeded();
        let mut out = Vec::new();
        // At or below the snapshot height: already reflected in it.
        let stale = updates_frame(
            900,
            1_792_680_001_000,
            &[],
            &[diff(ALICE, 99, "81676", r#""remove""#)],
        );
        p.parse(&stale, &mut out).unwrap();
        assert!(out.is_empty());
        assert_eq!(p.stats().stale_blocks, 1);

        // Order 40 was placed in a block we never saw.
        let gap = updates_frame(
            903,
            1_792_680_002_000,
            &[],
            &[diff(
                ALICE,
                40,
                "81676",
                r#"{"update":{"origSz":"0.5","newSz":"0.2"}}"#,
            )],
        );
        let err = p.parse(&gap, &mut out).unwrap_err();
        assert!(format!("{err:#}").contains("block was missed"), "{err:#}");

        let mut p = seeded();
        p.parse(&updates_frame(902, 1_792_680_002_000, &[], &[]), &mut out)
            .unwrap();
        let err = p
            .parse(&updates_frame(902, 1_792_680_002_000, &[], &[]), &mut out)
            .unwrap_err();
        assert!(format!("{err:#}").contains("does not follow"), "{err:#}");

        let mut fresh = L4Parser::new(BTC);
        assert!(
            fresh
                .parse(&updates_frame(902, 1, &[], &[]), &mut out)
                .is_err()
        );
    }

    fn book_frame(time: u64, bids: &[(&str, &str, u32)], asks: &[(&str, &str, u32)]) -> String {
        let side = |rows: &[(&str, &str, u32)]| {
            rows.iter()
                .map(|(px, sz, n)| format!(r#"{{"px":"{px}","sz":"{sz}","n":{n}}}"#))
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            r#"{{"channel":"l2Book","data":{{"coin":"BTC","time":{time},"levels":[[{}],[{}]]}}}}"#,
            side(bids),
            side(asks)
        )
    }

    fn trades_frame(trades: &[(&str, &str, &str, u64)]) -> String {
        let rows: Vec<String> = trades
            .iter()
            .map(|(side, px, sz, time)| {
                format!(
                    r#"{{"coin":"BTC","side":"{side}","px":"{px}","sz":"{sz}","time":{time},"hash":"0x00","tid":1,"users":["{ALICE}","{BOB}"]}}"#
                )
            })
            .collect();
        format!(r#"{{"channel":"trades","data":[{}]}}"#, rows.join(","))
    }

    #[test]
    fn diffs_public_books_and_attributes_trades() {
        let mut p = PublicParser::new(BTC);
        let mut out = Vec::new();
        p.parse(
            &book_frame(
                1000,
                &[("100.0", "1.0", 2), ("99.0", "2.0", 3)],
                &[("101.0", "1.5", 1)],
            ),
            &mut out,
        )
        .unwrap();
        let [
            FeedEvent::L2Snapshot {
                ts: 1_000_000,
                levels,
            },
        ] = out.as_slice()
        else {
            panic!("expected the snapshot, got {out:?}");
        };
        assert_eq!(levels.len(), 3);
        out.clear();

        // Taker buy at 101 hits the ask; published before the book frame of its block.
        p.parse(&trades_frame(&[("B", "101.0", "0.5", 1200)]), &mut out)
            .unwrap();
        p.parse(
            &book_frame(
                1500,
                &[("100.0", "1.0", 2), ("98.0", "4.0", 1)],
                &[("101.0", "1.0", 1)],
            ),
            &mut out,
        )
        .unwrap();
        // Same block as the frame, published after it; plus a trade for the next frame.
        p.parse(
            &trades_frame(&[("B", "101.0", "0.25", 1500), ("A", "100.0", "0.1", 1600)]),
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty(), "frame held until the next one arrives");
        p.parse(
            &book_frame(
                2000,
                &[("100.0", "0.9", 2), ("98.0", "4.0", 1)],
                &[("101.0", "1.0", 1)],
            ),
            &mut out,
        )
        .unwrap();
        // Older than the emitted 1500 frame: dropped.
        p.parse(&trades_frame(&[("A", "100.0", "0.1", 1400)]), &mut out)
            .unwrap();
        p.finish(&mut out);

        let batches: Vec<&L2Batch> = out
            .iter()
            .map(|e| match e {
                FeedEvent::L2(b) => b,
                other => panic!("expected a batch, got {other:?}"),
            })
            .collect();
        assert_eq!(batches.len(), 2);
        let b = batches[0];
        assert_eq!((b.start, b.end), (1_500_000, 1_500_000));
        assert_eq!(
            b.levels,
            vec![
                LevelUpdate {
                    side: Side::Bid,
                    px: 980,
                    total: 400_000,
                    count: Some(1)
                },
                LevelUpdate {
                    side: Side::Ask,
                    px: 1010,
                    total: 100_000,
                    count: Some(1)
                },
                LevelUpdate {
                    side: Side::Bid,
                    px: 990,
                    total: 0,
                    count: Some(0)
                },
            ]
        );
        assert_eq!(
            b.trades,
            vec![Trade {
                side: Side::Ask,
                px: 1010,
                qty: 75_000
            }]
        );
        let b = batches[1];
        assert_eq!(
            b.levels,
            vec![LevelUpdate {
                side: Side::Bid,
                px: 1000,
                total: 90_000,
                count: Some(2)
            }]
        );
        assert_eq!(
            b.trades,
            vec![Trade {
                side: Side::Bid,
                px: 1000,
                qty: 10_000
            }]
        );
        assert_eq!(p.stats().late_trades, 1);
    }

    #[test]
    fn capture_merges_l4_and_public_by_time() {
        let dir = std::env::temp_dir().join(format!("l3lab-hl-merge-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let public = [
            book_frame(
                1_792_680_000_500,
                &[("81676.0", "0.75", 2)],
                &[("81677.0", "0.1", 1)],
            ),
            book_frame(
                1_792_680_002_000,
                &[("81676.0", "0.45", 2)],
                &[("81677.0", "0.1", 1)],
            ),
            book_frame(
                1_792_680_002_500,
                &[("81676.0", "0.45", 2)],
                &[("81677.0", "0.2", 1)],
            ),
        ];
        let l4 = [
            snapshot_frame(),
            updates_frame(
                901,
                1_792_680_002_000,
                &[],
                &[diff(
                    ALICE,
                    11,
                    "81676",
                    r#"{"update":{"origSz":"0.5","newSz":"0.2"}}"#,
                )],
            ),
            updates_frame(
                902,
                1_792_680_002_400,
                &[],
                &[diff(BOB, 12, "81676", r#""remove""#)],
            ),
        ];
        // The public file ends with a torn line, as after a killed recorder.
        fs::write(
            dir.join("public.jsonl"),
            format!("{}\n{{\"channel\":\"l2Bo", public.join("\n")),
        )
        .unwrap();
        fs::write(dir.join("l4.jsonl"), l4.join("\n") + "\n").unwrap();

        let events: Vec<FeedEvent> = CaptureReader::open(&dir, BTC)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        fs::remove_dir_all(&dir).unwrap();
        let kinds: Vec<(&str, Ts)> = events
            .iter()
            .map(|e| {
                let kind = match e {
                    FeedEvent::TruthSnapshot { .. } => "truth-snapshot",
                    FeedEvent::Truth(_) => "truth",
                    FeedEvent::L2Snapshot { .. } => "l2-snapshot",
                    FeedEvent::L2(_) => "l2",
                };
                (kind, event_ts(e))
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("l2-snapshot", 1_792_680_000_500_000),
                ("truth-snapshot", 1_792_680_001_000_000),
                ("truth", 1_792_680_002_000_000),
                ("l2", 1_792_680_002_000_000),
                ("truth", 1_792_680_002_400_000),
                ("l2", 1_792_680_002_500_000),
            ]
        );
    }
}
