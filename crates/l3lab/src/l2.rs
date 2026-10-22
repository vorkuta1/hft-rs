//! Level-aggregated (L2) view: what a venue without order IDs publishes.

use crate::truth::{LevelEffect, TruthBook};
use crate::types::{Px, Qty, Side, Ts};

/// New absolute total at one price level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelUpdate {
    pub side: Side,
    pub px: Px,
    pub total: Qty,
}

/// Maker volume traded at one level; `side` is the resting side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    pub side: Side,
    pub px: Px,
    pub qty: Qty,
}

/// One published L2 message: every level that changed in `[start, end]`, plus trades.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct L2Batch {
    /// Assigned by the session; 0 is reserved for the snapshot.
    pub seq: u64,
    pub start: Ts,
    pub end: Ts,
    pub levels: Vec<LevelUpdate>,
    pub trades: Vec<Trade>,
}

impl L2Batch {
    pub fn traded_at(&self, side: Side, px: Px) -> Qty {
        self.trades
            .iter()
            .filter(|t| t.side == side && t.px == px)
            .map(|t| t.qty)
            .sum()
    }
}

/// Collapses truth-book effects into L2 batches the way a venue's diff feed does.
///
/// With `window_us == 0` every L3 event becomes its own batch (the most an L2
/// consumer could ever see). Otherwise events are bucketed into fixed windows
/// aligned to `ts / window_us`, like Binance's `@depth@100ms`; a level whose total is
/// back where it started and saw no trades is omitted, exactly as the venue would.
#[derive(Debug)]
pub struct Conflator {
    window_us: u64,
    open: Option<Bucket>,
    /// `(side, px, total at window start, traded)` per touched level.
    touched: Vec<(Side, Px, Qty, Qty)>,
}

#[derive(Clone, Copy, Debug)]
struct Bucket {
    id: u64,
    first_ts: Ts,
    last_ts: Ts,
}

impl Conflator {
    pub fn new(window_us: u64) -> Self {
        Self {
            window_us,
            open: None,
            touched: Vec::new(),
        }
    }

    pub fn window_us(&self) -> u64 {
        self.window_us
    }

    /// True if an event at `ts` falls outside the open window, so the open batch
    /// must be flushed *before* that event is applied.
    pub fn closes_before(&self, ts: Ts) -> bool {
        self.window_us > 0 && self.open.is_some_and(|b| self.bucket_id(ts) != b.id)
    }

    /// True if the open batch must be flushed right after the current event.
    pub fn closes_after(&self) -> bool {
        self.window_us == 0 && self.open.is_some()
    }

    pub fn record(&mut self, ts: Ts, effects: &[LevelEffect]) {
        if effects.is_empty() {
            return;
        }
        let id = self.bucket_id(ts);
        let bucket = self.open.get_or_insert(Bucket {
            id,
            first_ts: ts,
            last_ts: ts,
        });
        bucket.last_ts = ts;
        for e in effects {
            match self
                .touched
                .iter_mut()
                .find(|(side, px, _, _)| *side == e.side && *px == e.px)
            {
                Some(entry) => entry.3 += e.traded,
                None => self.touched.push((e.side, e.px, e.before, e.traded)),
            }
        }
    }

    /// Emits the open batch with totals read from `truth`; `seq` is left at 0.
    pub fn flush(&mut self, truth: &TruthBook) -> Option<L2Batch> {
        let bucket = self.open.take()?;
        let (start, end) = if self.window_us == 0 {
            (bucket.first_ts, bucket.last_ts)
        } else {
            let start = bucket.id * self.window_us;
            (start, start + self.window_us - 1)
        };
        let mut batch = L2Batch {
            seq: 0,
            start,
            end,
            ..L2Batch::default()
        };
        for (side, px, before, traded) in self.touched.drain(..) {
            let total = truth.total(side, px);
            if total != before || traded > 0 {
                batch.levels.push(LevelUpdate { side, px, total });
            }
            if traded > 0 {
                batch.trades.push(Trade {
                    side,
                    px,
                    qty: traded,
                });
            }
        }
        (!batch.levels.is_empty()).then_some(batch)
    }

    fn bucket_id(&self, ts: Ts) -> u64 {
        ts.checked_div(self.window_us).unwrap_or(0)
    }
}

/// Current L2 view of a truth book (used to seed models from a snapshot).
pub fn levels_of(truth: &TruthBook) -> Vec<LevelUpdate> {
    Side::BOTH
        .iter()
        .flat_map(|&side| {
            truth.levels(side).map(move |(px, level)| LevelUpdate {
                side,
                px,
                total: level.total(),
            })
        })
        .collect()
}
