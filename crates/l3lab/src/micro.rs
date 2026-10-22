//! Microstructure analytics over the L2 stream the models see (after
//! [binance_l3_est](https://github.com/OctopusTakopi/binance_l3_est)):
//!
//! * **CTR** (cancellation-to-trade) = cancelled volume / filled volume.
//! * **OTR** (order-to-trade) = added volume / filled volume.
//!
//! Both are *rolling*: every flow decays exponentially with a configurable
//! half-life on exchange time, so a ratio reflects the recent past instead of
//! everything since the snapshot, at O(1) cost. A ratio is undefined (`None`)
//! until enough fills have happened recently, which replaces a divide-by-almost-
//! zero spike with an honest gap. Split by side and by tier (the touch, and the
//! best 20 levels), sampled on exchange time. A level change of `net` with
//! `traded` volume filled there implies `net + traded` of hidden adds (if
//! positive) or cancels (if negative). Also keeps a depth-time history of the
//! best levels per side for a heatmap, and computes sweep cost curves.

use std::collections::{BTreeMap, VecDeque};

use crate::l2::{L2Batch, LevelUpdate};
use crate::types::{Px, Qty, Side, Ts};

const TOP_N: usize = 20;

#[derive(Clone, Copy, Debug)]
pub struct MicroConfig {
    /// Sampling period of the ratio and heatmap histories (exchange time).
    pub sample_us: u64,
    /// History kept for the ratio plots.
    pub history_us: u64,
    /// Half-life of the exponential decay applied to all flows.
    pub half_life_us: u64,
    /// Recent (decayed) fill events a side needs before its ratios are defined.
    pub min_fill_events: f64,
    /// Levels per side in each heatmap column.
    pub heat_levels: usize,
    /// Heatmap columns kept.
    pub heat_columns: usize,
}

impl Default for MicroConfig {
    fn default() -> Self {
        Self {
            sample_us: 500_000,
            history_us: 300_000_000,
            half_life_us: 30_000_000,
            min_fill_events: 3.0,
            heat_levels: 40,
            heat_columns: 600,
        }
    }
}

/// One sample of a ratio: seconds since the first batch, per side and combined;
/// `None` where too few recent fills make the ratio meaningless.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RatioPoint {
    pub t: f64,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub both: Option<f64>,
}

/// Best-first level totals per side at one instant (index 0 = touch).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeatColumn {
    pub t_us: Ts,
    pub bids: Vec<Qty>,
    pub asks: Vec<Qty>,
}

/// Exponentially decayed volumes (and fill event count) for one side and tier.
#[derive(Clone, Copy, Debug, Default)]
struct Flows {
    fills: f64,
    fill_events: f64,
    cancels: f64,
    inflows: f64,
}

impl Flows {
    fn scale(&mut self, k: f64) {
        self.fills *= k;
        self.fill_events *= k;
        self.cancels *= k;
        self.inflows *= k;
    }
}

/// Which ratio series to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ratio {
    CtrTop1,
    CtrTop20,
    OtrTop1,
    OtrTop20,
}

pub struct Microstructure {
    cfg: MicroConfig,
    bids: BTreeMap<Px, Qty>,
    asks: BTreeMap<Px, Qty>,
    /// `[side][tier]`, side 0 = bid, tier 0 = touch, 1 = top 20.
    flows: [[Flows; 2]; 2],
    /// Exchange time the flows are decayed to.
    decayed_to: Ts,
    start: Option<Ts>,
    next_sample: Ts,
    series: [VecDeque<RatioPoint>; 4],
    heat: VecDeque<HeatColumn>,
}

impl Microstructure {
    pub fn new(cfg: MicroConfig) -> Self {
        Self {
            cfg,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            flows: [[Flows::default(); 2]; 2],
            decayed_to: 0,
            start: None,
            next_sample: 0,
            series: Default::default(),
            heat: VecDeque::new(),
        }
    }

    /// Resets everything to a new snapshot.
    pub fn seed(&mut self, levels: &[LevelUpdate]) {
        *self = Self::new(self.cfg);
        for lu in levels {
            if lu.total > 0 {
                self.side_mut(lu.side).insert(lu.px, lu.total);
            }
        }
    }

    pub fn config(&self) -> MicroConfig {
        self.cfg
    }

    pub fn on_batch(&mut self, batch: &L2Batch) {
        let start = *self.start.get_or_insert(batch.start);
        if self.next_sample == 0 {
            self.next_sample = start + self.cfg.sample_us;
            self.decayed_to = start;
        }
        self.decay_to(batch.end);
        // Tiers are judged against the book before the batch.
        for t in &batch.trades {
            let rank = self.rank(t.side, t.px);
            self.add(t.side, rank, |f| {
                f.fills += t.qty as f64;
                f.fill_events += 1.0;
            });
        }
        let before: Vec<(usize, Qty)> = batch
            .levels
            .iter()
            .map(|lu| {
                let old = self.side(lu.side).get(&lu.px).copied().unwrap_or(0);
                (self.rank(lu.side, lu.px), old)
            })
            .collect();
        let worst_before = [self.worst(Side::Bid), self.worst(Side::Ask)];
        for lu in &batch.levels {
            let book = self.side_mut(lu.side);
            if lu.total == 0 {
                book.remove(&lu.px);
            } else {
                book.insert(lu.px, lu.total);
            }
        }
        let worst_after = [self.worst(Side::Bid), self.worst(Side::Ask)];
        for (lu, (rank, old)) in batch.levels.iter().zip(before) {
            // Depth-limited feeds (Hyperliquid's 5-level book) drop and reveal levels
            // at the far edge as the book moves; that is visibility, not order flow.
            let s = lu.side as usize;
            let left_view = lu.total == 0 && beyond(lu.side, lu.px, worst_after[s]);
            let came_into_view = old == 0 && beyond(lu.side, lu.px, worst_before[s]);
            if left_view || came_into_view {
                continue;
            }
            let flow = lu.total as f64 - old as f64 + batch.traded_at(lu.side, lu.px) as f64;
            if flow > 0.0 {
                self.add(lu.side, rank, |f| f.inflows += flow);
            } else if flow < 0.0 {
                self.add(lu.side, rank, |f| f.cancels -= flow);
            }
        }
        while batch.end >= self.next_sample {
            self.sample(self.next_sample, start);
            self.next_sample += self.cfg.sample_us;
        }
    }

    fn decay_to(&mut self, ts: Ts) {
        if ts <= self.decayed_to {
            return;
        }
        let k = 0.5f64.powf((ts - self.decayed_to) as f64 / self.cfg.half_life_us as f64);
        for f in self.flows.iter_mut().flatten() {
            f.scale(k);
        }
        self.decayed_to = ts;
    }
    /// Deepest known price on `side`.
    fn worst(&self, side: Side) -> Option<Px> {
        match side {
            Side::Bid => self.bids.keys().next().copied(),
            Side::Ask => self.asks.keys().next_back().copied(),
        }
    }

    /// Samples of one ratio, oldest first.
    pub fn series(&self, ratio: Ratio) -> &VecDeque<RatioPoint> {
        &self.series[ratio as usize]
    }

    /// Depth-time history, oldest first.
    pub fn heat(&self) -> &VecDeque<HeatColumn> {
        &self.heat
    }

    fn sample(&mut self, at: Ts, start: Ts) {
        let t = (at - start) as f64 / 1e6;
        let [bid, ask] = self.flows;
        let min = self.cfg.min_fill_events;
        let point = |tier: usize, num: fn(&Flows) -> f64| {
            let (b, a) = (bid[tier], ask[tier]);
            let ratio = |num: f64, fills: f64, events: f64| (events >= min).then(|| num / fills);
            RatioPoint {
                t,
                bid: ratio(num(&b), b.fills, b.fill_events),
                ask: ratio(num(&a), a.fills, a.fill_events),
                both: ratio(
                    num(&b) + num(&a),
                    b.fills + a.fills,
                    b.fill_events + a.fill_events,
                ),
            }
        };
        let points = [
            point(0, |f| f.cancels),
            point(1, |f| f.cancels),
            point(0, |f| f.inflows),
            point(1, |f| f.inflows),
        ];
        let horizon = t - self.cfg.history_us as f64 / 1e6;
        for (series, p) in self.series.iter_mut().zip(points) {
            series.push_back(p);
            while series.front().is_some_and(|p| p.t < horizon) {
                series.pop_front();
            }
        }
        let n = self.cfg.heat_levels;
        self.heat.push_back(HeatColumn {
            t_us: at,
            bids: self.bids.values().rev().take(n).copied().collect(),
            asks: self.asks.values().take(n).copied().collect(),
        });
        while self.heat.len() > self.cfg.heat_columns {
            self.heat.pop_front();
        }
    }

    /// Levels strictly better than `px` on `side`, capped at `TOP_N`.
    fn rank(&self, side: Side, px: Px) -> usize {
        match side {
            Side::Bid => self.bids.range(px + 1..).take(TOP_N).count(),
            Side::Ask => self.asks.range(..px).take(TOP_N).count(),
        }
    }

    fn add(&mut self, side: Side, rank: usize, f: impl Fn(&mut Flows)) {
        let s = &mut self.flows[side as usize];
        if rank == 0 {
            f(&mut s[0]);
        }
        if rank < TOP_N {
            f(&mut s[1]);
        }
    }

    fn side(&self, side: Side) -> &BTreeMap<Px, Qty> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<Px, Qty> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }
}

/// Whether `px` lies deeper than `worst` on `side`.
fn beyond(side: Side, px: Px, worst: Option<Px>) -> bool {
    worst.is_some_and(|w| match side {
        Side::Bid => px < w,
        Side::Ask => px > w,
    })
}

/// Cost of sweeping one side: `(notional, impact_bps)` after each level, where the
/// impact is the sweep VWAP's distance from `mid`. `levels` are best-first
/// `(price, qty)` in real units.
pub fn sweep_curve(levels: impl Iterator<Item = (f64, f64)>, mid: f64) -> Vec<(f64, f64)> {
    let mut out = vec![(0.0, 0.0)];
    let (mut qty, mut notional) = (0.0, 0.0);
    for (px, q) in levels {
        qty += q;
        notional += px * q;
        if qty > 0.0 {
            out.push((qty * mid, ((notional / qty - mid) / mid * 1e4).abs()));
        }
    }
    out
}

/// Impact in bps of executing `usd` notional against one side, or `None` if the
/// visible depth is too thin.
pub fn sweep_impact(levels: impl Iterator<Item = (f64, f64)>, mid: f64, usd: f64) -> Option<f64> {
    let mut left = usd / mid;
    let (mut qty, mut notional) = (0.0, 0.0);
    for (px, q) in levels {
        if left <= 0.0 {
            break;
        }
        let take = q.min(left);
        qty += take;
        notional += take * px;
        left -= take;
    }
    (left <= 1e-12 && qty > 0.0).then(|| ((notional / qty - mid) / mid * 1e4).abs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::l2::Trade;

    fn lu(side: Side, px: Px, total: Qty) -> LevelUpdate {
        LevelUpdate {
            side,
            px,
            total,
            count: None,
        }
    }

    /// No decay, one fill is enough evidence: exact cumulative arithmetic.
    fn exact() -> Microstructure {
        Microstructure::new(MicroConfig {
            sample_us: 10,
            half_life_us: u64::MAX,
            min_fill_events: 1.0,
            ..MicroConfig::default()
        })
    }

    fn trade(px: Px, qty: Qty) -> Trade {
        Trade {
            side: Side::Bid,
            px,
            qty,
        }
    }

    #[test]
    fn ratios_split_level_changes_into_fills_cancels_and_adds() {
        let mut m = exact();
        m.seed(&[lu(Side::Bid, 100, 10), lu(Side::Bid, 99, 10)]);
        // Touch: 4 traded, level fell by 6 → 2 cancelled. Level 99 grew by 5.
        m.on_batch(&L2Batch {
            seq: 1,
            start: 0,
            end: 10,
            levels: vec![lu(Side::Bid, 100, 4), lu(Side::Bid, 99, 15)],
            trades: vec![Trade {
                side: Side::Bid,
                px: 100,
                qty: 4,
            }],
        });
        let ctr1 = m.series(Ratio::CtrTop1).back().copied().unwrap();
        assert_eq!(
            (ctr1.bid, ctr1.ask, ctr1.both),
            (Some(0.5), None, Some(0.5))
        );
        let otr20 = m.series(Ratio::OtrTop20).back().copied().unwrap();
        assert_eq!(otr20.bid, Some(1.25));
        let col = m.heat().back().unwrap();
        assert_eq!(col.bids, vec![4, 15]);
    }

    #[test]
    fn levels_scrolling_through_a_depth_limited_view_are_not_flow() {
        let mut m = exact();
        // A 3-level feed: bids 100, 99, 98 visible.
        m.seed(&[
            lu(Side::Bid, 100, 10),
            lu(Side::Bid, 99, 10),
            lu(Side::Bid, 98, 10),
        ]);
        // A new best bid at 101 pushes 98 out of view.
        m.on_batch(&L2Batch {
            seq: 1,
            start: 0,
            end: 10,
            levels: vec![lu(Side::Bid, 101, 3), lu(Side::Bid, 98, 0)],
            trades: vec![Trade {
                side: Side::Bid,
                px: 100,
                qty: 1,
            }],
        });
        let ctr20 = m.series(Ratio::CtrTop20).back().copied().unwrap();
        let otr20 = m.series(Ratio::OtrTop20).back().copied().unwrap();
        assert_eq!((ctr20.bid, otr20.bid), (Some(0.0), Some(3.0)));
        // The book moves back: 101 is cancelled and 98 scrolls into view again.
        m.on_batch(&L2Batch {
            seq: 2,
            start: 20,
            end: 20,
            levels: vec![lu(Side::Bid, 101, 0), lu(Side::Bid, 98, 10)],
            trades: vec![],
        });
        let ctr20 = m.series(Ratio::CtrTop20).back().copied().unwrap();
        let otr20 = m.series(Ratio::OtrTop20).back().copied().unwrap();
        assert_eq!((ctr20.bid, otr20.bid), (Some(3.0), Some(3.0)));
    }

    #[test]
    fn old_flow_fades_and_thin_evidence_reads_as_undefined() {
        let mut m = Microstructure::new(MicroConfig {
            sample_us: 1_000_000,
            half_life_us: 1_000_000,
            min_fill_events: 3.0,
            ..MicroConfig::default()
        });
        m.seed(&[lu(Side::Bid, 100, 100)]);
        let batch = |t: Ts, total: Qty, trades: Vec<Trade>| L2Batch {
            seq: 0,
            start: t,
            end: t,
            levels: vec![lu(Side::Bid, 100, total)],
            trades,
        };
        // A burst: 4 fills of 1 and 36 cancelled → CTR 9.
        m.on_batch(&batch(
            0,
            60,
            vec![trade(100, 1), trade(100, 1), trade(100, 1), trade(100, 1)],
        ));
        m.on_batch(&batch(1_000_000, 60, vec![]));
        let ctr = m.series(Ratio::CtrTop1).back().copied().unwrap();
        // One half-life later: the ratio is unchanged, but only 2 recent fills remain.
        assert_eq!(ctr.bid, None);
        // Four seconds later another 4 fills and 4 cancelled: recent flow dominates.
        m.on_batch(&batch(
            5_000_000,
            52,
            vec![trade(100, 1), trade(100, 1), trade(100, 1), trade(100, 1)],
        ));
        let ctr = m.series(Ratio::CtrTop1).back().unwrap().bid.unwrap();
        // Old burst decayed by 2^-5: (36/32 + 4) / (4/32 + 4) ≈ 1.24, far from 9.
        assert!((ctr - (36.0 / 32.0 + 4.0) / (4.0 / 32.0 + 4.0)).abs() < 1e-9);
    }

    #[test]
    fn sweep_reports_vwap_impact_and_thin_books() {
        let asks = [(101.0, 1.0), (102.0, 1.0)];
        let curve = sweep_curve(asks.iter().copied(), 100.0);
        assert_eq!(curve.len(), 3);
        assert!((curve[2].1 - 150.0).abs() < 1e-9);
        assert!((sweep_impact(asks.iter().copied(), 100.0, 100.0).unwrap() - 100.0).abs() < 1e-9);
        assert_eq!(sweep_impact(asks.iter().copied(), 100.0, 1e6), None);
    }
}
