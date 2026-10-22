//! Drives truth book → L2 → models → scorer from a stream of feed events.

use anyhow::{Result, bail};

use crate::l2::{Conflator, L2Batch, LevelUpdate, levels_of};
use crate::model::{InferredBook, QueueModel};
use crate::score::{ModelReport, OwnerReport, ScoreConfig, Scorer};
use crate::truth::{L3Event, LevelEffect, SnapshotOrder, TruthBook};
use crate::types::Ts;

/// Normalised input from any venue source (capture file or live socket).
#[derive(Clone, Debug)]
pub enum FeedEvent {
    /// Full L3/L4 book; per-level priority follows order.
    TruthSnapshot { ts: Ts, orders: Vec<SnapshotOrder> },
    Truth(L3Event),
    /// Full L2 book as the venue publishes it.
    L2Snapshot { ts: Ts, levels: Vec<LevelUpdate> },
    L2(L2Batch),
}

/// Where the models' L2 input comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum L2Source {
    /// Derived from the truth book through a conflation window, optionally with
    /// per-level order counts. Venue L2 events are only used when the input has no
    /// truth (an L2-only capture).
    Derived { conflate_us: u64, counts: bool },
    /// The venue's own L2 events drive the models; truth events only feed the scorer.
    Venue,
}

pub struct Session {
    source: L2Source,
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
    last_batch: Option<L2Batch>,
}

impl Session {
    pub fn new(models: Vec<Box<dyn QueueModel>>, source: L2Source) -> Self {
        let (window, counts) = match source {
            L2Source::Derived {
                conflate_us,
                counts,
            } => (conflate_us, counts),
            L2Source::Venue => (0, false),
        };
        Self {
            source,
            truth: None,
            conflator: Conflator::new(window, counts),
            models: models.into_iter().map(InferredBook::new).collect(),
            scorer: None,
            effects: Vec::new(),
            deferred: None,
            seq: 0,
            last_ts: 0,
            seeded: false,
            last_batch: None,
        }
    }

    pub fn with_scorer(mut self, cfg: ScoreConfig) -> Self {
        self.scorer = Some(Scorer::new(cfg));
        self
    }

    /// Feeds one event; returns how many L2 batches reached the models (0 or 1).
    /// Right after a derived batch is reported, the truth book sits exactly at that
    /// batch's end; inside an open window it runs ahead of the models, as a venue's would.
    pub fn ingest(&mut self, ev: FeedEvent) -> Result<usize> {
        let derived = matches!(self.source, L2Source::Derived { .. });
        match ev {
            FeedEvent::TruthSnapshot { ts, orders } => {
                let mut truth = TruthBook::default();
                truth.seed(orders);
                if derived {
                    let levels = levels_of(&truth, self.conflator.counts());
                    self.seed_models(ts, &levels);
                }
                self.truth = Some(truth);
                self.last_ts = self.last_ts.max(ts);
                Ok(0)
            }
            FeedEvent::L2Snapshot { ts, levels } => {
                if !(derived && self.truth.is_some()) {
                    self.seed_models(ts, &levels);
                }
                Ok(0)
            }
            FeedEvent::Truth(ev) => {
                if self.truth.is_none() {
                    bail!("L3 event before a truth snapshot");
                }
                if !derived {
                    self.effects.clear();
                    let truth = self.truth.as_mut().expect("checked above");
                    truth.apply(&ev, &mut self.effects);
                    self.last_ts = self.last_ts.max(ev.ts);
                    return Ok(0);
                }
                self.apply_deferred();
                if self.conflator.closes_before(ev.ts) && self.flush() > 0 {
                    self.deferred = Some(ev);
                    return Ok(1);
                }
                Ok(self.apply_truth(&ev))
            }
            FeedEvent::L2(batch) => {
                if derived && self.truth.is_some() {
                    return Ok(0);
                }
                if !self.seeded {
                    bail!("L2 batch before an L2 snapshot");
                }
                self.last_ts = self.last_ts.max(batch.end);
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

    /// The batch most recently applied to the models (with its `seq` assigned).
    pub fn last_batch(&self) -> Option<&L2Batch> {
        self.last_batch.as_ref()
    }

    pub fn last_ts(&self) -> Ts {
        self.last_ts
    }

    pub fn l2_source(&self) -> L2Source {
        self.source
    }

    pub fn report(&self) -> Option<Vec<ModelReport>> {
        Some(self.scorer.as_ref()?.report(&self.models))
    }

    /// Per-owner queue-ahead accuracy for model `model`, busiest `top` owners first.
    pub fn owner_report(&self, model: usize, top: usize) -> Option<Vec<OwnerReport>> {
        Some(self.scorer.as_ref()?.owner_report(model, top))
    }

    fn seed_models(&mut self, ts: Ts, levels: &[LevelUpdate]) {
        self.conflator = Conflator::new(self.conflator.window_us(), self.conflator.counts());
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
        self.last_batch = Some(batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::l2::LevelUpdate;
    use crate::model::all_models;
    use crate::truth::L3Msg;
    use crate::types::{Px, Qty, Side};

    fn derived(conflate_us: u64) -> L2Source {
        L2Source::Derived {
            conflate_us,
            counts: false,
        }
    }

    fn add(ts: Ts, id: u64, px: Px, qty: Qty) -> FeedEvent {
        FeedEvent::Truth(L3Event {
            ts,
            msg: L3Msg::Add {
                id,
                side: Side::Bid,
                px,
                qty,
                owner: None,
                insert_before: None,
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
        let order = |id, side, px| SnapshotOrder {
            id,
            side,
            px,
            qty: 10,
            born: 0,
            owner: None,
        };
        FeedEvent::TruthSnapshot {
            ts: 0,
            orders: vec![order(1, Side::Bid, 100), order(2, Side::Ask, 101)],
        }
    }

    fn level(px: Px, total: Qty) -> LevelUpdate {
        LevelUpdate {
            side: Side::Bid,
            px,
            total,
            count: None,
        }
    }

    #[test]
    fn conflation_hides_round_trips_and_batches_by_window() {
        let mut s = Session::new(all_models(), derived(100));
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
        let mut s = Session::new(all_models(), derived(0));
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

    #[test]
    fn venue_mode_drives_models_from_venue_l2_only() {
        let mut s = Session::new(all_models(), L2Source::Venue);
        s.ingest(snapshot()).unwrap();
        s.ingest(FeedEvent::L2Snapshot {
            ts: 0,
            levels: vec![level(100, 10)],
        })
        .unwrap();
        assert_eq!(s.ingest(add(10, 3, 100, 5)).unwrap(), 0);
        assert_eq!(s.models()[0].queue(Side::Bid, 100).unwrap().total(), 10);
        let batch = L2Batch {
            start: 10,
            end: 10,
            levels: vec![level(100, 15)],
            ..L2Batch::default()
        };
        assert_eq!(s.ingest(FeedEvent::L2(batch)).unwrap(), 1);
        for m in s.models() {
            assert_eq!(m.queue(Side::Bid, 100).unwrap().total(), 15);
        }
        assert_eq!(s.truth().unwrap().total(Side::Bid, 100), 15);
    }
}
