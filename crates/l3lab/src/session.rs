//! Drives truth book → conflator → models → scorer from a stream of feed events.

use anyhow::{Result, bail};

use crate::l2::{Conflator, L2Batch, LevelUpdate, levels_of};
use crate::model::{InferredBook, QueueModel};
use crate::score::{ModelReport, ScoreConfig, Scorer};
use crate::truth::{L3Event, LevelEffect, TruthBook};
use crate::types::{Px, Qty, Side, Ts};

/// Normalised input from any venue source (capture file or live socket).
#[derive(Clone, Debug)]
pub enum FeedEvent {
    /// Full L3 book; per-level priority follows order.
    TruthSnapshot {
        ts: Ts,
        orders: Vec<(u64, Side, Px, Qty)>,
    },
    Truth(L3Event),
    /// Full L2 book from an L2-only venue.
    L2Snapshot {
        ts: Ts,
        levels: Vec<LevelUpdate>,
    },
    L2(L2Batch),
}

pub struct Session {
    truth: Option<TruthBook>,
    conflator: Conflator,
    models: Vec<InferredBook>,
    scorer: Option<Scorer>,
    effects: Vec<LevelEffect>,
    /// Event that closed the previous window; applied on the next call so the truth
    /// book stays exactly at the batch boundary between calls.
    deferred: Option<L3Event>,
    seq: u64,
    last_ts: Ts,
    seeded: bool,
}

impl Session {
    pub fn new(models: Vec<Box<dyn QueueModel>>, conflate_us: u64) -> Self {
        Self {
            truth: None,
            conflator: Conflator::new(conflate_us),
            models: models.into_iter().map(InferredBook::new).collect(),
            scorer: None,
            effects: Vec::new(),
            deferred: None,
            seq: 0,
            last_ts: 0,
            seeded: false,
        }
    }

    pub fn with_scorer(mut self, cfg: ScoreConfig) -> Self {
        self.scorer = Some(Scorer::new(cfg));
        self
    }

    /// Feeds one event; returns how many L2 batches reached the models (0 or 1).
    /// Right after a batch is reported, the truth book sits exactly at that batch's
    /// end; inside an open window it runs ahead of the models, as a venue's would.
    pub fn ingest(&mut self, ev: FeedEvent) -> Result<usize> {
        match ev {
            FeedEvent::TruthSnapshot { ts, orders } => {
                let mut truth = TruthBook::default();
                truth.seed(ts, orders);
                let levels = levels_of(&truth);
                self.truth = Some(truth);
                self.seed_models(ts, &levels);
                Ok(0)
            }
            FeedEvent::L2Snapshot { ts, levels } => {
                self.truth = None;
                self.seed_models(ts, &levels);
                Ok(0)
            }
            FeedEvent::Truth(ev) => {
                if self.truth.is_none() {
                    bail!("L3 event before a truth snapshot");
                }
                self.apply_deferred();
                if self.conflator.closes_before(ev.ts) && self.flush() > 0 {
                    self.deferred = Some(ev);
                    return Ok(1);
                }
                Ok(self.apply_truth(&ev))
            }
            FeedEvent::L2(batch) => {
                if !self.seeded {
                    bail!("L2 batch before a snapshot");
                }
                self.last_ts = batch.end;
                self.apply_batch(batch);
                Ok(1)
            }
        }
    }

    /// Applies anything held back and flushes the open window (call at end of input).
    pub fn finish(&mut self) -> usize {
        self.apply_deferred();
        self.flush()
    }

    pub fn truth(&self) -> Option<&TruthBook> {
        self.truth.as_ref()
    }

    pub fn models(&self) -> &[InferredBook] {
        &self.models
    }

    pub fn batches(&self) -> u64 {
        self.seq
    }

    pub fn last_ts(&self) -> Ts {
        self.last_ts
    }

    pub fn conflate_us(&self) -> u64 {
        self.conflator.window_us()
    }

    pub fn report(&self) -> Option<Vec<ModelReport>> {
        Some(self.scorer.as_ref()?.report(&self.models))
    }

    fn seed_models(&mut self, ts: Ts, levels: &[LevelUpdate]) {
        self.conflator = Conflator::new(self.conflator.window_us());
        self.deferred = None;
        for model in &mut self.models {
            model.seed(ts, levels);
        }
        self.last_ts = ts;
        self.seeded = true;
    }

    fn apply_deferred(&mut self) {
        if let Some(ev) = self.deferred.take() {
            self.apply_truth(&ev);
        }
    }

    /// Applies one L3 event to the truth book; returns batches flushed after it.
    fn apply_truth(&mut self, ev: &L3Event) -> usize {
        let truth = self.truth.as_mut().expect("truth snapshot seen");
        self.effects.clear();
        truth.apply(ev, &mut self.effects);
        self.conflator.record(ev.ts, &self.effects);
        self.last_ts = ev.ts;
        if self.conflator.closes_after() {
            self.flush()
        } else {
            0
        }
    }

    fn flush(&mut self) -> usize {
        let Some(truth) = self.truth.as_ref() else {
            return 0;
        };
        match self.conflator.flush(truth) {
            Some(batch) => {
                self.apply_batch(batch);
                1
            }
            None => 0,
        }
    }

    fn apply_batch(&mut self, mut batch: L2Batch) {
        self.seq += 1;
        batch.seq = self.seq;
        for model in &mut self.models {
            model.apply(&batch);
        }
        if let (Some(scorer), Some(truth)) = (self.scorer.as_mut(), self.truth.as_ref()) {
            scorer.observe(&batch, truth, &self.models);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::all_models;
    use crate::truth::L3Msg;

    fn add(ts: Ts, id: u64, px: Px, qty: Qty) -> FeedEvent {
        FeedEvent::Truth(L3Event {
            ts,
            msg: L3Msg::Add {
                id,
                side: Side::Bid,
                px,
                qty,
            },
        })
    }

    fn cancel(ts: Ts, id: u64) -> FeedEvent {
        FeedEvent::Truth(L3Event {
            ts,
            msg: L3Msg::Delete {
                id,
                traded_total: 0,
            },
        })
    }

    fn snapshot() -> FeedEvent {
        FeedEvent::TruthSnapshot {
            ts: 0,
            orders: vec![(1, Side::Bid, 100, 10), (2, Side::Ask, 101, 10)],
        }
    }

    #[test]
    fn conflation_hides_round_trips_and_batches_by_window() {
        let mut s = Session::new(all_models(), 100);
        s.ingest(snapshot()).unwrap();
        // Window [0,100): add + cancel at 99 nets to zero; add at 98 persists.
        assert_eq!(s.ingest(add(10, 3, 99, 5)).unwrap(), 0);
        assert_eq!(s.ingest(cancel(20, 3)).unwrap(), 0);
        assert_eq!(s.ingest(add(30, 4, 98, 7)).unwrap(), 0);
        // First event of the next window closes the previous one and is held back,
        // so truth matches the models at the boundary.
        assert_eq!(s.ingest(add(150, 5, 100, 1)).unwrap(), 1);
        assert_eq!(s.truth().unwrap().total(Side::Bid, 100), 10);
        assert_eq!(s.finish(), 1);
        assert_eq!(s.truth().unwrap().total(Side::Bid, 100), 11);
        assert_eq!(s.batches(), 2);
        for m in s.models() {
            assert!(m.queue(Side::Bid, 99).is_none());
            assert_eq!(m.queue(Side::Bid, 98).unwrap().total(), 7);
            assert_eq!(m.queue(Side::Bid, 100).unwrap().total(), 11);
        }
    }

    #[test]
    fn unconflated_session_emits_one_batch_per_book_change() {
        let mut s = Session::new(all_models(), 0);
        s.ingest(snapshot()).unwrap();
        assert_eq!(s.ingest(add(10, 3, 99, 5)).unwrap(), 1);
        assert_eq!(s.ingest(cancel(20, 3)).unwrap(), 1);
        assert_eq!(
            s.ingest(cancel(20, 42)).unwrap(),
            0,
            "unknown id changes nothing"
        );
        assert_eq!(s.finish(), 0);
    }
}
