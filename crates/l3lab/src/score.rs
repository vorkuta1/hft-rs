//! Scores inferred queues against the ground-truth book.
//!
//! Sampled after every batch (event time) once warm-up has elapsed, over the top
//! `top_k` truth levels per side:
//!
//! * **Order count** – |true orders − inferred orders| per level.
//! * **Size match** – multiset overlap of order sizes (exact lot equality);
//!   precision is over inferred orders, recall over true orders.
//! * **Queue ahead** – every true order that joins a top-k level after warm-up is
//!   paired with a proxy: the n-th inferred order created at that level in the same
//!   batch is the proxy of the n-th true order that arrived there in that batch.
//!   Each sample compares quantity ahead of the true order vs. ahead of the proxy,
//!   relative to the level total. If the model already removed the proxy while the
//!   true order still rests, the sample counts as *lost*.
//!
//! On L4 feeds, size recall and queue-ahead are also broken down by order owner.

use std::collections::HashMap;

use serde::Serialize;

use crate::l2::L2Batch;
use crate::model::InferredBook;
use crate::truth::TruthBook;
use crate::types::{Owner, Px, Qty, Side, Ts};

const ERR_BINS: usize = 1000;

#[derive(Clone, Copy, Debug)]
pub struct ScoreConfig {
    pub top_k: usize,
    pub warmup_us: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ModelReport {
    pub model: String,
    pub level_samples: u64,
    pub count_mae: f64,
    pub count_exact: f64,
    pub size_precision: f64,
    pub size_recall: f64,
    pub size_f1: f64,
    pub ahead_samples: u64,
    pub ahead_lost: f64,
    pub ahead_err_mean: f64,
    pub ahead_err_p50: f64,
    pub ahead_err_p90: f64,
    /// Sampled levels whose inferred total disagreed with truth. With derived L2 this
    /// must be 0 (otherwise a pipeline bug); with venue L2 it measures feed misalignment.
    pub total_mismatches: u64,
}

/// One owner's orders, as seen by one model.
#[derive(Clone, Debug, Serialize)]
pub struct OwnerReport {
    pub owner: Owner,
    /// Level samples' true orders belonging to this owner.
    pub order_samples: u64,
    pub size_recall: f64,
    pub ahead_samples: u64,
    pub ahead_lost: f64,
    pub ahead_err_mean: f64,
}

#[derive(Clone, Debug)]
struct Ahead {
    lost: u64,
    err_sum: f64,
    /// Histogram of relative error in `1/ERR_BINS` buckets; last bucket is `>= 1`.
    hist: Vec<u64>,
}

impl Default for Ahead {
    fn default() -> Self {
        Self {
            lost: 0,
            err_sum: 0.0,
            hist: vec![0; ERR_BINS + 1],
        }
    }
}

impl Ahead {
    fn record(&mut self, err: Option<f64>) {
        match err {
            Some(err) => {
                self.err_sum += err;
                self.hist[((err * ERR_BINS as f64) as usize).min(ERR_BINS)] += 1;
            }
            None => self.lost += 1,
        }
    }

    fn measured(&self) -> u64 {
        self.hist.iter().sum()
    }

    fn mean(&self) -> f64 {
        ratio_f(self.err_sum, self.measured())
    }
}

#[derive(Clone, Debug, Default)]
struct OwnerAccum {
    true_orders: u64,
    matched: u64,
    ahead_measured: u64,
    ahead_lost: u64,
    ahead_err_sum: f64,
}

#[derive(Clone, Debug, Default)]
struct Accum {
    level_samples: u64,
    count_abs_err: u64,
    count_exact: u64,
    n_true: u64,
    n_inf: u64,
    size_matched: u64,
    ahead: Ahead,
    total_mismatches: u64,
    owners: HashMap<Owner, OwnerAccum>,
}

#[derive(Clone, Copy, Debug)]
struct Tracked {
    side: Side,
    px: Px,
    batch: u64,
    /// Arrival rank among true orders that joined this level in `batch`.
    nth: u32,
    owner: Option<Owner>,
}

pub struct Scorer {
    cfg: ScoreConfig,
    start: Option<Ts>,
    /// End of the previously observed batch; orders born after it are new.
    prev_end: Option<Ts>,
    tracked: HashMap<u64, Tracked>,
    accums: Vec<Accum>,
    scratch_true: Vec<(Qty, Option<Owner>)>,
    scratch_inf: Vec<Qty>,
}

impl Scorer {
    pub fn new(cfg: ScoreConfig) -> Self {
        Self {
            cfg,
            start: None,
            prev_end: None,
            tracked: HashMap::new(),
            accums: Vec::new(),
            scratch_true: Vec::new(),
            scratch_inf: Vec::new(),
        }
    }

    pub fn observe(&mut self, batch: &L2Batch, truth: &TruthBook, models: &[InferredBook]) {
        let start = *self.start.get_or_insert(batch.start);
        let prev_end = self.prev_end.replace(batch.end);
        if batch.end < start + self.cfg.warmup_us {
            return;
        }
        let Some(prev_end) = prev_end else {
            return;
        };
        self.accums.resize_with(models.len(), Accum::default);

        for side in Side::BOTH {
            for (px, level) in truth.levels(side).take(self.cfg.top_k) {
                self.scratch_true.clear();
                let mut arrivals = 0;
                for order in level.orders() {
                    self.scratch_true.push((order.qty, order.owner));
                    if order.born > prev_end && !self.tracked.contains_key(&order.id) {
                        self.tracked.insert(
                            order.id,
                            Tracked {
                                side,
                                px,
                                batch: batch.seq,
                                nth: arrivals,
                                owner: order.owner,
                            },
                        );
                        arrivals += 1;
                    }
                }
                self.scratch_true.sort_unstable();
                for (acc, model) in self.accums.iter_mut().zip(models) {
                    self.scratch_inf.clear();
                    let inf_total = match model.queue(side, px) {
                        Some(q) => {
                            self.scratch_inf.extend(q.orders().iter().map(|o| o.qty));
                            q.total()
                        }
                        None => 0,
                    };
                    self.scratch_inf.sort_unstable();
                    let (nt, ni) = (self.scratch_true.len(), self.scratch_inf.len());
                    acc.level_samples += 1;
                    acc.count_abs_err += nt.abs_diff(ni) as u64;
                    acc.count_exact += u64::from(nt == ni);
                    acc.n_true += nt as u64;
                    acc.n_inf += ni as u64;
                    acc.total_mismatches += u64::from(inf_total != level.total());
                    acc.size_matched +=
                        match_sizes(&self.scratch_true, &self.scratch_inf, &mut acc.owners);
                }
            }
        }

        self.tracked.retain(|&id, tr| {
            let Some(level) = truth.level(tr.side, tr.px) else {
                return false;
            };
            let Some(true_ahead) = level.qty_ahead(id) else {
                return false; // filled, cancelled or re-priced
            };
            let total = level.total() as f64;
            for (acc, model) in self.accums.iter_mut().zip(models) {
                let err = proxy_ahead(model, tr)
                    .map(|inf_ahead| true_ahead.abs_diff(inf_ahead) as f64 / total);
                acc.ahead.record(err);
                if let Some(owner) = tr.owner {
                    let o = acc.owners.entry(owner).or_default();
                    match err {
                        Some(e) => {
                            o.ahead_measured += 1;
                            o.ahead_err_sum += e;
                        }
                        None => o.ahead_lost += 1,
                    }
                }
            }
            true
        });
    }

    pub fn report(&self, models: &[InferredBook]) -> Vec<ModelReport> {
        models
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let acc = self.accums.get(i).cloned().unwrap_or_default();
                let precision = ratio(acc.size_matched, acc.n_inf);
                let recall = ratio(acc.size_matched, acc.n_true);
                let measured = acc.ahead.measured();
                ModelReport {
                    model: m.name().to_string(),
                    level_samples: acc.level_samples,
                    count_mae: ratio(acc.count_abs_err, acc.level_samples),
                    count_exact: ratio(acc.count_exact, acc.level_samples),
                    size_precision: precision,
                    size_recall: recall,
                    size_f1: if precision + recall == 0.0 {
                        0.0
                    } else {
                        2.0 * precision * recall / (precision + recall)
                    },
                    ahead_samples: measured + acc.ahead.lost,
                    ahead_lost: ratio(acc.ahead.lost, measured + acc.ahead.lost),
                    ahead_err_mean: acc.ahead.mean(),
                    ahead_err_p50: percentile(&acc.ahead.hist, 0.5),
                    ahead_err_p90: percentile(&acc.ahead.hist, 0.9),
                    total_mismatches: acc.total_mismatches,
                }
            })
            .collect()
    }

    /// Owners of model `model`'s sampled orders, most order samples first.
    pub fn owner_report(&self, model: usize, top: usize) -> Vec<OwnerReport> {
        let Some(acc) = self.accums.get(model) else {
            return Vec::new();
        };
        let mut rows: Vec<OwnerReport> = acc
            .owners
            .iter()
            .map(|(&owner, o)| OwnerReport {
                owner,
                order_samples: o.true_orders,
                size_recall: ratio(o.matched, o.true_orders),
                ahead_samples: o.ahead_measured + o.ahead_lost,
                ahead_lost: ratio(o.ahead_lost, o.ahead_measured + o.ahead_lost),
                ahead_err_mean: ratio_f(o.ahead_err_sum, o.ahead_measured),
            })
            .collect();
        rows.sort_by(|a, b| {
            b.order_samples
                .cmp(&a.order_samples)
                .then(a.owner.cmp(&b.owner))
        });
        rows.truncate(top);
        rows
    }
}

/// Quantity ahead of the inferred proxy for a tracked true order: the n-th order
/// the model created at that level in the same batch, or the last one it created
/// there if it created fewer (a merged blob still estimates the position).
fn proxy_ahead(model: &InferredBook, tr: &Tracked) -> Option<Qty> {
    let q = model.queue(tr.side, tr.px)?;
    let mut ahead = 0;
    let mut seen = 0;
    let mut last = None;
    for o in q.orders() {
        if o.born_batch == tr.batch {
            if seen == tr.nth {
                return Some(ahead);
            }
            last = Some(ahead);
            seen += 1;
        }
        ahead += o.qty;
    }
    last
}

/// Multiset overlap of sorted true sizes (with owners) and sorted inferred sizes;
/// each match is credited to the true order's owner.
fn match_sizes(
    truth: &[(Qty, Option<Owner>)],
    inferred: &[Qty],
    owners: &mut HashMap<Owner, OwnerAccum>,
) -> u64 {
    let (mut i, mut j, mut n) = (0, 0, 0);
    for &(_, owner) in truth {
        if let Some(owner) = owner {
            owners.entry(owner).or_default().true_orders += 1;
        }
    }
    while i < truth.len() && j < inferred.len() {
        match truth[i].0.cmp(&inferred[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                if let Some(owner) = truth[i].1 {
                    owners.entry(owner).or_default().matched += 1;
                }
                n += 1;
                i += 1;
                j += 1;
            }
        }
    }
    n
}

fn ratio(num: u64, den: u64) -> f64 {
    ratio_f(num as f64, den)
}

fn ratio_f(num: f64, den: u64) -> f64 {
    if den == 0 { 0.0 } else { num / den as f64 }
}

/// Lower edge of the bucket containing quantile `q` (resolution `1/ERR_BINS`, so an
/// exact match reports 0).
fn percentile(hist: &[u64], q: f64) -> f64 {
    let total: u64 = hist.iter().sum();
    if total == 0 {
        return 0.0;
    }
    let target = (q * total as f64).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (bin, &count) in hist.iter().enumerate() {
        seen += count;
        if seen >= target {
            return bin as f64 / ERR_BINS as f64;
        }
    }
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_overlap_counts_duplicates_once_and_credits_owners() {
        let a = Owner([1; 20]);
        let b = Owner([2; 20]);
        let truth = [(1, Some(a)), (2, Some(a)), (2, Some(b)), (3, None)];
        let mut owners = HashMap::new();
        assert_eq!(match_sizes(&truth, &[2, 2, 2, 4], &mut owners), 2);
        assert_eq!((owners[&a].true_orders, owners[&a].matched), (2, 1));
        assert_eq!((owners[&b].true_orders, owners[&b].matched), (1, 1));
    }
}
