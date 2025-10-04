use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    Snapshot { qty: f64 },
    New { qty: f64 },
    Fill { qty: f64, remaining: f64 },
    Underflow { missing_qty: f64 },
}

#[derive(Clone, Debug, Deserialize)]
pub struct InferredEvent {
    pub sequence: u64,
    #[serde(default)]
    pub timestamp_ms: Option<u64>,
    pub side: Side,
    pub price_ticks: u64,
    #[serde(default)]
    pub order_id: Option<u64>,
    pub kind: EventKind,
}

#[derive(Clone)]
pub struct EventLog {
    events: Arc<Vec<InferredEvent>>,
}

impl EventLog {
    pub fn load(path: &Path, limit: usize) -> Result<Self> {
        let events = match path.extension().and_then(|ext| ext.to_str()) {
            Some("jsonl") | Some("json") => load_jsonl(path, limit)?,
            Some("bin") | Some("bincode") => load_bincode(path, limit)?,
            _ => load_jsonl(path, limit)?,
        };
        if events.is_empty() {
            return Err(anyhow!("no events parsed from {path:?}"));
        }
        Ok(Self {
            events: Arc::new(events),
        })
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn event(&self, idx: usize) -> Option<&InferredEvent> {
        self.events.get(idx)
    }
}

fn load_jsonl(path: &Path, limit: usize) -> Result<Vec<InferredEvent>> {
    let file = File::open(path).with_context(|| format!("opening {path:?}"))?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: InferredEvent =
            serde_json::from_str(&line).with_context(|| format!("parsing event line: {line}"))?;
        events.push(event);
        if limit != 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

fn load_bincode(path: &Path, limit: usize) -> Result<Vec<InferredEvent>> {
    let file = File::open(path).with_context(|| format!("opening {path:?}"))?;
    let mut reader = BufReader::new(file);
    let mut events = Vec::new();
    while limit == 0 || events.len() < limit {
        match bincode::deserialize_from::<_, InferredEvent>(&mut reader) {
            Ok(event) => events.push(event),
            Err(err) => {
                if let bincode::ErrorKind::Io(ref io) = *err {
                    if io.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                }
                return Err(anyhow!("bincode decode error: {err}"));
            }
        }
    }
    Ok(events)
}

const EPS: f64 = 1e-9;

#[derive(Clone, Debug)]
struct OrderSlice {
    id: u64,
    qty: f64,
}

#[derive(Default, Clone, Debug)]
struct PriceLevel {
    queue: VecDeque<OrderSlice>,
    total_qty: f64,
}

impl PriceLevel {
    fn push(&mut self, order_id: u64, qty: f64) {
        self.queue.push_back(OrderSlice { id: order_id, qty });
        self.total_qty += qty;
    }

    fn apply_fill(&mut self, order_id: u64, fill_qty: f64, remaining: f64) {
        if let Some(position) = self.queue.iter().position(|slice| slice.id == order_id) {
            if fill_qty >= self.queue[position].qty - remaining + EPS {
                self.queue[position].qty = remaining;
            }
            if remaining <= EPS {
                self.queue.remove(position);
            }
            self.recompute_total();
        }
    }

    fn recompute_total(&mut self) {
        self.total_qty = self.queue.iter().map(|slice| slice.qty).sum();
    }
}

#[derive(Default, Clone)]
struct BookSide {
    levels: BTreeMap<u64, PriceLevel>,
}

impl BookSide {
    fn reset(&mut self) {
        self.levels.clear();
    }

    fn push_snapshot(&mut self, price: u64, order_id: u64, qty: f64) {
        if qty <= EPS {
            return;
        }
        let level = self.levels.entry(price).or_default();
        level.push(order_id, qty);
    }

    fn apply_fill(&mut self, price: u64, order_id: u64, qty: f64, remaining: f64) {
        if let Some(level) = self.levels.get_mut(&price) {
            level.apply_fill(order_id, qty, remaining);
            if level.total_qty <= EPS {
                self.levels.remove(&price);
            }
        }
    }

    fn snapshot(&self, reverse: bool, depth: usize) -> Vec<LevelRow> {
        let mut rows: Vec<LevelRow> = self
            .levels
            .iter()
            .map(|(price, level)| LevelRow {
                price_ticks: *price,
                total_qty: level.total_qty,
                queue_depth: level.queue.len(),
            })
            .collect();
        if reverse {
            rows.sort_by(|a, b| b.price_ticks.cmp(&a.price_ticks));
        } else {
            rows.sort_by(|a, b| a.price_ticks.cmp(&b.price_ticks));
        }
        if depth > 0 {
            rows.truncate(depth);
        }
        rows
    }
}

#[derive(Clone, Debug)]
pub struct LevelRow {
    pub price_ticks: u64,
    pub total_qty: f64,
    pub queue_depth: usize,
}

#[derive(Clone, Debug)]
pub struct BookSnapshot {
    pub bids: Vec<LevelRow>,
    pub asks: Vec<LevelRow>,
}

pub struct PlaybackBook {
    log: EventLog,
    bids: BookSide,
    asks: BookSide,
    current_idx: usize,
}

impl PlaybackBook {
    pub fn new(log: EventLog) -> Self {
        Self {
            log,
            bids: BookSide::default(),
            asks: BookSide::default(),
            current_idx: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.log.len()
    }

    pub fn event(&self, idx: usize) -> Option<&InferredEvent> {
        self.log.event(idx)
    }

    pub fn seek(&mut self, target: usize) {
        if self.log.len() == 0 {
            self.reset();
            return;
        }
        let capped = target.min(self.log.len() - 1);
        self.reset();
        for idx in 0..=capped {
            if let Some(event) = self.log.event(idx).cloned() {
                self.apply_event(&event);
            }
        }
        self.current_idx = capped;
    }

    pub fn snapshot(&self, depth: usize) -> BookSnapshot {
        BookSnapshot {
            bids: self.bids.snapshot(true, depth),
            asks: self.asks.snapshot(false, depth),
        }
    }

    fn reset(&mut self) {
        self.bids.reset();
        self.asks.reset();
        self.current_idx = 0;
    }

    fn apply_event(&mut self, event: &InferredEvent) {
        let side = match event.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        match &event.kind {
            EventKind::Snapshot { qty } | EventKind::New { qty } => {
                if let Some(order_id) = event.order_id {
                    side.push_snapshot(event.price_ticks, order_id, *qty);
                }
            }
            EventKind::Fill { qty, remaining } => {
                if let Some(order_id) = event.order_id {
                    side.apply_fill(event.price_ticks, order_id, *qty, *remaining);
                }
            }
            EventKind::Underflow { .. } => {
                // For now we do not adjust; they indicate data loss.
            }
        }
    }
}

impl Clone for PlaybackBook {
    fn clone(&self) -> Self {
        Self {
            log: self.log.clone(),
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            current_idx: self.current_idx,
        }
    }
}
