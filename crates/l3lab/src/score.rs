//! Scores inferred queues against the ground-truth book.
//!
//! Sampled after every batch (event time) once warm-up has elapsed, over the top
//! `top_k` truth levels per side:
//!
//! * **Order count** – |true orders − inferred orders| per level.
//! * **Size match** – multiset overlap of order sizes (exact lot equality);
//!   precision is over inferred orders, recall over true orders.
//! * **Queue ahead** – for each true order that joined a top-k level after warm-up,
//!   the inferred order created at the same level in the same batch is its proxy.
//!   Each sample compares quantity ahead of the true order vs. ahead of the proxy,
//!   relative to the level total. If the model already removed the proxy while the
//!   true order still rests, the sample counts as *lost*.

use std::collections::HashMap;

use serde::Serialize;

use crate::l2::L2Batch;
use crate::model::InferredBook;
use crate::truth::TruthBook;
use crate::types::{Px, Qty, Side, Ts};

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
    /// Level totals that disagreed with truth; non-zero means a pipeline bug.
    pub total_mismatches: u64,
}

#[derive(Clone, Debug)]
struct Accum {
    level_samples: u64,
    count_abs_err: u64,
    count_exact: u64,
    n_true: u64,
    n_inf: u64,
    size_matched: u64,
    ahead_lost: u64,
    ahead_err_sum: f64,
    /// Histogram of relative queue-ahead error in `1/ERR_BINS` buckets; last bucket is `>= 1`.
    ahead_hist: Vec<u64>,
    total_mismatches: u64,
}

impl Default for Accum {
    fn default() -> Self {
        Self {
            level_samples: 0,
            count_abs_err: 0,
            count_exact: 0,
            n_true: 0,
            n_inf: 0,
            size_matched: 0,
            ahead_lost: 0,
            ahead_err_sum: 0.0,
            ahead_hist: vec![0; ERR_BINS + 1],
            total_mismatches: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Tracked {
    side: Side,
    px: Px,
    batch: u64,
}

pub struct Scorer {
    cfg: ScoreConfig,
    start: Option<Ts>,
    tracked: HashMap<u64, Tracked>,
    accums: Vec<Accum>,
    scratch_true: Vec<Qty>,
    scratch_inf: Vec<Qty>,
}

impl Scorer {
    pub fn new(cfg: ScoreConfig) -> Self {
        Self {
            cfg,
            start: None,
            tracked: HashMap::new(),
            accums: Vec::new(),
            scratch_true: Vec::new(),
            scratch_inf: Vec::new(),
        }
    }

    pub fn observe(&mut self, batch: &L2Batch, truth: &TruthBook, models: &[InferredBook]) {
        let start = *self.start.get_or_insert(batch.start);
        if batch.end < start + self.cfg.warmup_us {
            return;
        }
        self.accums.resize_with(models.len(), Accum::default);

        for side in Side::BOTH {
            for (px, level) in truth.levels(side).take(self.cfg.top_k) {
                self.scratch_true.clear();
                for order in level.orders() {
                    self.scratch_true.push(order.qty);
                    if order.born >= batch.start {
                        self.tracked.entry(order.id).or_insert(Tracked {
                            side,
                            px,
                            batch: batch.seq,
                        });
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
                    acc.size_matched += sorted_overlap(&self.scratch_true, &self.scratch_inf);
                    acc.total_mismatches += u64::from(inf_total != level.total());
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
                let proxy_ahead = model.queue(tr.side, tr.px).and_then(|q| {
                    let mut ahead = 0;
                    for o in q.orders() {
                        if o.born_batch == tr.batch {
                            return Some(ahead);
                        }
                        ahead += o.qty;
                    }
                    None
                });
                match proxy_ahead {
                    Some(inf_ahead) => {
                        let err = true_ahead.abs_diff(inf_ahead) as f64 / total;
                        acc.ahead_err_sum += err;
                        let bin = ((err * ERR_BINS as f64) as usize).min(ERR_BINS);
                        acc.ahead_hist[bin] += 1;
                    }
                    None => acc.ahead_lost += 1,
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
                let ratio = |num: u64, den: u64| {
                    if den == 0 {
                        0.0
                    } else {
                        num as f64 / den as f64
                    }
                };
                let precision = ratio(acc.size_matched, acc.n_inf);
                let recall = ratio(acc.size_matched, acc.n_true);
                let measured: u64 = acc.ahead_hist.iter().sum();
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
                    ahead_samples: measured + acc.ahead_lost,
                    ahead_lost: ratio(acc.ahead_lost, measured + acc.ahead_lost),
                    ahead_err_mean: if measured == 0 {
                        0.0
                    } else {
                        acc.ahead_err_sum / measured as f64
                    },
                    ahead_err_p50: percentile(&acc.ahead_hist, 0.5),
                    ahead_err_p90: percentile(&acc.ahead_hist, 0.9),
                    total_mismatches: acc.total_mismatches,
                }
            })
            .collect()
    }
}

/// Size of the multiset intersection of two sorted slices.
fn sorted_overlap(a: &[Qty], b: &[Qty]) -> u64 {
    let (mut i, mut j, mut n) = (0, 0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                n += 1;
                i += 1;
                j += 1;
            }
        }
    }
    n
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
    fn overlap_counts_duplicates_once_per_pair() {
        assert_eq!(sorted_overlap(&[1, 2, 2, 3], &[2, 2, 2, 4]), 2);
        assert_eq!(sorted_overlap(&[], &[1]), 0);
    }
}
