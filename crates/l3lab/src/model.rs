//! L2→L3 inference: rebuild per-level order queues from level totals (and trades).

use std::collections::{BTreeMap, VecDeque};

use crate::l2::{L2Batch, LevelUpdate};
use crate::types::{BestFirst, Px, Qty, Side, Ts};

/// A synthetic order: one inferred unit of resting quantity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InfOrder {
    pub id: u64,
    pub qty: Qty,
    pub born: Ts,
    /// Batch that created it; 0 means it came from the snapshot.
    pub born_batch: u64,
}

/// Identity for the (at most one) order a model may create during `apply`.
#[derive(Clone, Copy, Debug)]
pub struct Birth {
    pub id: u64,
    pub ts: Ts,
    pub batch: u64,
}

/// Inferred orders at one price, front of the queue first.
#[derive(Clone, Debug, Default)]
pub struct Queue {
    orders: VecDeque<InfOrder>,
    total: Qty,
}

impl Queue {
    pub fn total(&self) -> Qty {
        self.total
    }

    pub fn orders(&self) -> &VecDeque<InfOrder> {
        &self.orders
    }

    pub fn push_back(&mut self, qty: Qty, birth: Birth) {
        if qty == 0 {
            return;
        }
        self.total += qty;
        self.orders.push_back(InfOrder {
            id: birth.id,
            qty,
            born: birth.ts,
            born_batch: birth.batch,
        });
    }

    /// Removes up to `qty` starting at the front (fills). Returns the amount removed.
    pub fn take_front(&mut self, qty: Qty) -> Qty {
        self.take(qty, true)
    }

    /// Removes up to `qty` starting at the back (newest first). Returns the amount removed.
    pub fn take_back(&mut self, qty: Qty) -> Qty {
        self.take(qty, false)
    }

    /// Removes the newest order of exactly `qty`, if any.
    pub fn remove_exact(&mut self, qty: Qty) -> bool {
        let Some(pos) = self.orders.iter().rposition(|o| o.qty == qty) else {
            return false;
        };
        self.orders.remove(pos);
        self.total -= qty;
        true
    }

    fn take(&mut self, qty: Qty, front: bool) -> Qty {
        let mut left = qty;
        while left > 0 {
            let slot = if front {
                self.orders.front_mut()
            } else {
                self.orders.back_mut()
            };
            let Some(order) = slot else { break };
            let cut = order.qty.min(left);
            order.qty -= cut;
            left -= cut;
            if order.qty == 0 {
                if front {
                    self.orders.pop_front();
                } else {
                    self.orders.pop_back();
                }
            }
        }
        let taken = qty - left;
        self.total -= taken;
        taken
    }
}

/// What one L2 update tells a model about a level.
#[derive(Clone, Copy, Debug)]
pub struct LevelChange {
    pub new_total: Qty,
    /// Maker volume traded at this level in the batch (0 if the feed has no trades).
    pub traded: Qty,
}

/// An inference rule. `apply` must leave `queue.total() == change.new_total`.
pub trait QueueModel: Send {
    fn name(&self) -> &'static str;
    fn apply(&mut self, queue: &mut Queue, change: LevelChange, birth: Birth);
}

/// Every increase joins the back; every decrease is a fill from the front.
/// The classic naive estimator.
pub struct Fifo;

/// Every increase joins the back; every decrease is a cancel of the newest quantity.
pub struct Lifo;

/// Decreases that exactly match a resting order's size cancel that order
/// (newest first); anything else is consumed from the front.
pub struct SizeMatch;

/// Uses trades: traded volume fills from the front, the rest of the change is
/// treated as adds (back) or cancels (exact size match, else newest first).
pub struct Hybrid;

impl QueueModel for Fifo {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, birth: Birth) {
        let old = queue.total();
        if change.new_total > old {
            queue.push_back(change.new_total - old, birth);
        } else {
            queue.take_front(old - change.new_total);
        }
    }
}

impl QueueModel for Lifo {
    fn name(&self) -> &'static str {
        "lifo"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, birth: Birth) {
        let old = queue.total();
        if change.new_total > old {
            queue.push_back(change.new_total - old, birth);
        } else {
            queue.take_back(old - change.new_total);
        }
    }
}

impl QueueModel for SizeMatch {
    fn name(&self) -> &'static str {
        "size-match"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, birth: Birth) {
        let old = queue.total();
        if change.new_total > old {
            queue.push_back(change.new_total - old, birth);
        } else {
            let cut = old - change.new_total;
            if !queue.remove_exact(cut) {
                queue.take_front(cut);
            }
        }
    }
}

impl QueueModel for Hybrid {
    fn name(&self) -> &'static str {
        "hybrid"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, birth: Birth) {
        queue.take_front(change.traded);
        let rest = queue.total();
        if change.new_total > rest {
            queue.push_back(change.new_total - rest, birth);
        } else {
            let cut = rest - change.new_total;
            if !queue.remove_exact(cut) {
                queue.take_back(cut);
            }
        }
    }
}

pub const MODEL_NAMES: [&str; 4] = ["fifo", "lifo", "size-match", "hybrid"];

pub fn model_by_name(name: &str) -> Option<Box<dyn QueueModel>> {
    Some(match name {
        "fifo" => Box::new(Fifo),
        "lifo" => Box::new(Lifo),
        "size-match" => Box::new(SizeMatch),
        "hybrid" => Box::new(Hybrid),
        _ => return None,
    })
}

pub fn all_models() -> Vec<Box<dyn QueueModel>> {
    MODEL_NAMES
        .iter()
        .map(|n| model_by_name(n).expect("registered model"))
        .collect()
}

/// A full book of inferred queues driven by one model.
pub struct InferredBook {
    model: Box<dyn QueueModel>,
    bids: BTreeMap<Px, Queue>,
    asks: BTreeMap<Px, Queue>,
    next_id: u64,
}

impl InferredBook {
    pub fn new(model: Box<dyn QueueModel>) -> Self {
        Self {
            model,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn name(&self) -> &'static str {
        self.model.name()
    }

    /// Resets to a snapshot: each level becomes one order of unknown composition.
    pub fn seed(&mut self, ts: Ts, levels: &[LevelUpdate]) {
        self.bids.clear();
        self.asks.clear();
        for lu in levels {
            let birth = self.birth(ts, 0);
            self.side_mut(lu.side)
                .entry(lu.px)
                .or_default()
                .push_back(lu.total, birth);
        }
    }

    pub fn apply(&mut self, batch: &L2Batch) {
        for lu in &batch.levels {
            let change = LevelChange {
                new_total: lu.total,
                traded: batch.traded_at(lu.side, lu.px),
            };
            let birth = self.birth(batch.end, batch.seq);
            let book = match lu.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let queue = book.entry(lu.px).or_default();
            self.model.apply(queue, change, birth);
            assert_eq!(
                queue.total(),
                lu.total,
                "model {} broke the level-total invariant",
                self.model.name()
            );
            if queue.total() == 0 {
                book.remove(&lu.px);
            }
        }
    }

    pub fn levels(&self, side: Side) -> BestFirst<'_, Queue> {
        BestFirst::new(side, &self.bids, &self.asks)
    }

    pub fn queue(&self, side: Side, px: Px) -> Option<&Queue> {
        match side {
            Side::Bid => self.bids.get(&px),
            Side::Ask => self.asks.get(&px),
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<Px, Queue> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    fn birth(&mut self, ts: Ts, batch: u64) -> Birth {
        let id = self.next_id;
        self.next_id += 1;
        Birth { id, ts, batch }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue_of(sizes: &[Qty]) -> Queue {
        let mut q = Queue::default();
        for (i, &s) in sizes.iter().enumerate() {
            q.push_back(
                s,
                Birth {
                    id: i as u64 + 1,
                    ts: 0,
                    batch: 0,
                },
            );
        }
        q
    }

    fn sizes(q: &Queue) -> Vec<Qty> {
        q.orders().iter().map(|o| o.qty).collect()
    }

    fn run(model: &mut dyn QueueModel, sizes_in: &[Qty], new_total: Qty, traded: Qty) -> Vec<Qty> {
        let mut q = queue_of(sizes_in);
        let birth = Birth {
            id: 99,
            ts: 1,
            batch: 1,
        };
        model.apply(&mut q, LevelChange { new_total, traded }, birth);
        assert_eq!(q.total(), new_total);
        sizes(&q)
    }

    #[test]
    fn models_allocate_a_decrease_differently() {
        // Queue [5, 3, 4]; level drops by 3.
        assert_eq!(run(&mut Fifo, &[5, 3, 4], 9, 0), vec![2, 3, 4]);
        assert_eq!(run(&mut Lifo, &[5, 3, 4], 9, 0), vec![5, 3, 1]);
        assert_eq!(run(&mut SizeMatch, &[5, 3, 4], 9, 0), vec![5, 4]);
        // No exact match: size-match falls back to the front.
        assert_eq!(run(&mut SizeMatch, &[5, 3, 4], 10, 0), vec![3, 3, 4]);
    }

    #[test]
    fn hybrid_splits_fills_from_cancels_and_sees_hidden_adds() {
        // 2 traded at the front, 4 cancelled by size, net change -6.
        assert_eq!(run(&mut Hybrid, &[5, 3, 4], 6, 2), vec![3, 3]);
        // 5 traded but the level grew by 1: 6 must have joined behind.
        assert_eq!(run(&mut Hybrid, &[5, 3, 4], 13, 5), vec![3, 4, 6]);
    }

    #[test]
    fn take_spans_orders_and_reports_shortfall() {
        let mut q = queue_of(&[2, 2]);
        assert_eq!(q.take_back(3), 3);
        assert_eq!(sizes(&q), vec![1]);
        assert_eq!(q.take_front(5), 1);
        assert_eq!(q.total(), 0);
        assert!(q.orders().is_empty());
    }
}
