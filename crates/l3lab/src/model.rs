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

impl InfOrder {
    fn new(qty: Qty, birth: Birth) -> Self {
        Self {
            id: birth.id,
            qty,
            born: birth.ts,
            born_batch: birth.batch,
        }
    }
}

/// Identity for an order a model creates during `apply`.
#[derive(Clone, Copy, Debug)]
pub struct Birth {
    pub id: u64,
    pub ts: Ts,
    pub batch: u64,
}

/// Hands out fresh order identities for one level change.
pub struct Births<'a> {
    next_id: &'a mut u64,
    ts: Ts,
    batch: u64,
}

impl<'a> Births<'a> {
    /// Ids come from `next_id`, which is advanced past every id handed out.
    pub fn new(next_id: &'a mut u64, ts: Ts, batch: u64) -> Self {
        Self { next_id, ts, batch }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Birth {
        let id = *self.next_id;
        *self.next_id += 1;
        Birth {
            id,
            ts: self.ts,
            batch: self.batch,
        }
    }
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
        self.orders.push_back(InfOrder::new(qty, birth));
    }

    /// Adds an order ahead of everything resting (a priority insert).
    pub fn push_front(&mut self, qty: Qty, birth: Birth) {
        if qty == 0 {
            return;
        }
        self.total += qty;
        self.orders.push_front(InfOrder::new(qty, birth));
    }

    /// Removes the order at `pos` (front = 0). Returns its quantity.
    pub fn remove_at(&mut self, pos: usize) -> Option<Qty> {
        let order = self.orders.remove(pos)?;
        self.total -= order.qty;
        Some(order.qty)
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
        match self.orders.iter().rposition(|o| o.qty == qty) {
            Some(pos) => self.remove_at(pos).is_some(),
            None => false,
        }
    }

    /// Removes the oldest order of exactly `qty`, if any.
    pub fn remove_exact_oldest(&mut self, qty: Qty) -> bool {
        match self.orders.iter().position(|o| o.qty == qty) {
            Some(pos) => self.remove_at(pos).is_some(),
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// Appends `qty` as `pieces` near-equal orders (the last takes the remainder);
    /// `pieces` is clamped to `1..=qty`.
    pub fn push_split(&mut self, qty: Qty, pieces: u64, births: &mut Births) {
        let n = pieces.clamp(1, qty.max(1));
        let each = qty / n;
        for i in 0..n {
            let q = if i + 1 == n {
                qty - each * (n - 1)
            } else {
                each
            };
            self.push_back(q, births.next());
        }
    }

    /// Shrinks the order at `pos` by `by` (< its size) without moving it.
    fn reduce_at(&mut self, pos: usize, by: Qty) {
        let order = &mut self.orders[pos];
        debug_assert!(order.qty > by);
        order.qty -= by;
        self.total -= by;
    }

    /// Replaces the order at `pos` with `pieces` new orders of the same total.
    fn split_at(&mut self, pos: usize, pieces: u64, births: &mut Births) {
        let Some(order) = self.orders.remove(pos) else {
            return;
        };
        let n = pieces.clamp(1, order.qty);
        let each = order.qty / n;
        for i in 0..n {
            let q = if i + 1 == n {
                order.qty - each * (n - 1)
            } else {
                each
            };
            self.orders
                .insert(pos + i as usize, InfOrder::new(q, births.next()));
        }
    }

    /// Merges the newest `n` orders into one new order at the back.
    fn merge_back(&mut self, n: usize, births: &mut Births) {
        let n = n.min(self.orders.len());
        if n < 2 {
            return;
        }
        let qty: Qty = self
            .orders
            .drain(self.orders.len() - n..)
            .map(|o| o.qty)
            .sum();
        self.orders.push_back(InfOrder::new(qty, births.next()));
    }

    /// Splits or merges orders until there are `count` of them, keeping the total
    /// (best effort: a queue of unit orders cannot grow further).
    fn reconcile_count(&mut self, count: usize, births: &mut Births) {
        if count == 0 {
            return;
        }
        if self.orders.len() > count {
            self.merge_back(self.orders.len() - count + 1, births);
        }
        while self.orders.len() < count {
            // Newest splittable order: the most likely home of unseen arrivals.
            let Some(pos) = self.orders.iter().rposition(|o| o.qty >= 2) else {
                break;
            };
            let want = (count - self.orders.len() + 1) as u64;
            self.split_at(pos, want, births);
        }
    }

    /// A shrink of `cut` that keeps the order count: a partial fill or reduction
    /// of the first order (from the front) larger than `cut`; failing that, the
    /// newest exact-size order, else quantity from the back.
    fn reduce(&mut self, cut: Qty) {
        if cut == 0 {
            return;
        }
        if let Some(pos) = self.orders.iter().position(|o| o.qty > cut) {
            self.reduce_at(pos, cut);
        } else if !self.remove_exact(cut) {
            self.take_back(cut);
        }
    }

    /// Removes up to `m` whole orders whose sizes best explain a drop of `cut`:
    /// an exact single (m = 1) or pair (m = 2) match, newest first; otherwise
    /// greedily the largest orders that still fit. Any unexplained rest is a
    /// [`reduce`](Self::reduce).
    fn remove_explaining(&mut self, cut: Qty, m: usize) {
        if m == 1 && self.remove_exact(cut) {
            return;
        }
        if m == 2 && self.remove_exact_pair(cut) {
            return;
        }
        let mut left = cut;
        for _ in 0..m {
            let best = self
                .orders
                .iter()
                .enumerate()
                .filter(|(_, o)| o.qty <= left)
                .max_by_key(|&(i, o)| (o.qty, i));
            let Some((pos, _)) = best else { break };
            left -= self.remove_at(pos).expect("position in range");
            if left == 0 {
                return;
            }
        }
        self.reduce(left);
    }

    /// Removes two orders summing to `cut`, preferring the newest such pair.
    fn remove_exact_pair(&mut self, cut: Qty) -> bool {
        let n = self.orders.len();
        for j in (1..n).rev() {
            let need = match cut.checked_sub(self.orders[j].qty) {
                Some(need) if need > 0 => need,
                _ => continue,
            };
            if let Some(i) = self.orders.range(..j).rposition(|o| o.qty == need) {
                self.remove_at(j);
                self.remove_at(i);
                return true;
            }
        }
        false
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
    /// Published order count after the change, if the feed has one.
    pub count: Option<u32>,
    /// Levels strictly better than this one on its side before the change
    /// (0 = touch), capped at `u8::MAX`.
    pub rank: u8,
}

/// An inference rule. `apply` must leave `queue.total() == change.new_total`;
/// every order it creates takes its identity from `births`.
pub trait QueueModel: Send {
    fn name(&self) -> &'static str;
    fn apply(&mut self, queue: &mut Queue, change: LevelChange, births: &mut Births);
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

/// Uses published order counts: with k = count − inferred orders (after traded
/// volume fills from the front), a drop with k < 0 removes |k| orders whose sizes
/// best explain it, a drop with k ≥ 0 is a partial fill/reduction near the front,
/// a rise with k > 0 is k new orders sharing it; leftover count mismatches are
/// reconciled by merging the newest orders or splitting the newest splittable one.
/// Without counts it behaves exactly like [`Hybrid`].
pub struct Count;

impl QueueModel for Fifo {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, births: &mut Births) {
        let old = queue.total();
        if change.new_total > old {
            queue.push_back(change.new_total - old, births.next());
        } else {
            queue.take_front(old - change.new_total);
        }
    }
}

impl QueueModel for Lifo {
    fn name(&self) -> &'static str {
        "lifo"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, births: &mut Births) {
        let old = queue.total();
        if change.new_total > old {
            queue.push_back(change.new_total - old, births.next());
        } else {
            queue.take_back(old - change.new_total);
        }
    }
}

impl QueueModel for SizeMatch {
    fn name(&self) -> &'static str {
        "size-match"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, births: &mut Births) {
        let old = queue.total();
        if change.new_total > old {
            queue.push_back(change.new_total - old, births.next());
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

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, births: &mut Births) {
        queue.take_front(change.traded);
        let rest = queue.total();
        if change.new_total > rest {
            queue.push_back(change.new_total - rest, births.next());
        } else {
            let cut = rest - change.new_total;
            if !queue.remove_exact(cut) {
                queue.take_back(cut);
            }
        }
    }
}

impl QueueModel for Count {
    fn name(&self) -> &'static str {
        "count"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, births: &mut Births) {
        let count = match change.count {
            Some(c) if c > 0 || change.new_total == 0 => c as usize,
            _ => return Hybrid.apply(queue, change, births),
        };
        queue.take_front(change.traded);
        let rest = queue.total();
        let k = count as i64 - queue.len() as i64;
        if change.new_total > rest {
            let add = change.new_total - rest;
            queue.push_split(add, k.max(1) as u64, births);
        } else if change.new_total < rest {
            let cut = rest - change.new_total;
            if k < 0 {
                queue.remove_explaining(cut, k.unsigned_abs() as usize);
            } else {
                queue.reduce(cut);
            }
        }
        queue.reconcile_count(count, births);
    }
}

pub const MODEL_NAMES: [&str; 5] = ["fifo", "lifo", "size-match", "hybrid", "count"];

pub fn model_by_name(name: &str) -> Option<Box<dyn QueueModel>> {
    Some(match name {
        "fifo" => Box::new(Fifo),
        "lifo" => Box::new(Lifo),
        "size-match" => Box::new(SizeMatch),
        "hybrid" => Box::new(Hybrid),
        "count" => Box::new(Count),
        _ => return None,
    })
}

pub fn all_models() -> Vec<Box<dyn QueueModel>> {
    MODEL_NAMES
        .iter()
        .map(|n| model_by_name(n).expect("registered model"))
        .collect()
}

/// Per-price inferred queues for both sides, without a model attached.
#[derive(Clone, Debug)]
pub struct Book {
    bids: BTreeMap<Px, Queue>,
    asks: BTreeMap<Px, Queue>,
    next_id: u64,
}

impl Default for Book {
    fn default() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            next_id: 1,
        }
    }
}

impl Book {
    /// Resets to a snapshot. Each level becomes one order of unknown composition,
    /// or `count` equal orders when the feed publishes counts.
    pub fn seed(&mut self, ts: Ts, levels: &[LevelUpdate]) {
        self.bids.clear();
        self.asks.clear();
        for lu in levels {
            let book = match lu.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let queue = book.entry(lu.px).or_default();
            let mut births = Births::new(&mut self.next_id, ts, 0);
            queue.push_split(lu.total, u64::from(lu.count.unwrap_or(1)), &mut births);
        }
    }

    /// Applies every level update of `batch` in order through `f`, which must
    /// leave the queue at the published total (`who` names the culprit if not).
    pub fn apply_with(
        &mut self,
        batch: &L2Batch,
        who: &str,
        mut f: impl FnMut(&LevelUpdate, &mut Queue, LevelChange, &mut Births),
    ) {
        for lu in &batch.levels {
            let book = match lu.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let better = match lu.side {
                Side::Bid => book.range(lu.px + 1..).take(u8::MAX as usize).count(),
                Side::Ask => book.range(..lu.px).take(u8::MAX as usize).count(),
            };
            let change = LevelChange {
                new_total: lu.total,
                traded: batch.traded_at(lu.side, lu.px),
                count: lu.count,
                rank: better as u8,
            };
            let mut births = Births::new(&mut self.next_id, batch.end, batch.seq);
            let queue = book.entry(lu.px).or_default();
            f(lu, queue, change, &mut births);
            assert_eq!(
                queue.total(),
                lu.total,
                "model {who} broke the level-total invariant"
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
}

/// A full book of inferred queues driven by one model.
pub struct InferredBook {
    model: Box<dyn QueueModel>,
    book: Book,
}

impl InferredBook {
    pub fn new(model: Box<dyn QueueModel>) -> Self {
        Self {
            model,
            book: Book::default(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.model.name()
    }

    /// Resets to a snapshot (see [`Book::seed`]).
    pub fn seed(&mut self, ts: Ts, levels: &[LevelUpdate]) {
        self.book.seed(ts, levels);
    }

    pub fn apply(&mut self, batch: &L2Batch) {
        let name = self.model.name();
        let model = &mut self.model;
        self.book
            .apply_with(batch, name, |_, queue, change, births| {
                model.apply(queue, change, births)
            });
    }

    pub fn levels(&self, side: Side) -> BestFirst<'_, Queue> {
        self.book.levels(side)
    }

    pub fn queue(&self, side: Side, px: Px) -> Option<&Queue> {
        self.book.queue(side, px)
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
        run_counted(model, sizes_in, new_total, traded, None)
    }

    fn run_counted(
        model: &mut dyn QueueModel,
        sizes_in: &[Qty],
        new_total: Qty,
        traded: Qty,
        count: Option<u32>,
    ) -> Vec<Qty> {
        let mut q = queue_of(sizes_in);
        let mut next_id = 99;
        let change = LevelChange {
            new_total,
            traded,
            count,
            rank: 0,
        };
        model.apply(&mut q, change, &mut Births::new(&mut next_id, 1, 1));
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
    fn count_model_uses_count_and_size_changes_together() {
        let count = |s: &[Qty], new_total, traded, n| {
            run_counted(&mut Count, s, new_total, traded, Some(n))
        };
        // One order fewer and an exact single match: that order was cancelled.
        assert_eq!(count(&[5, 3, 4], 9, 0, 2), vec![5, 4]);
        // Two fewer, no single match: the newest exact pair (3 + 4 = 7).
        assert_eq!(count(&[5, 3, 4, 1], 6, 0, 2), vec![5, 1]);
        // Same count, smaller total: a partial reduction at the front.
        assert_eq!(count(&[5, 3, 4], 10, 0, 3), vec![3, 3, 4]);
        // Two more orders: the increase is split between them.
        assert_eq!(count(&[5], 11, 0, 3), vec![5, 3, 3]);
        // Traded volume fills the front order; nothing else changed.
        assert_eq!(count(&[5, 3, 4], 7, 5, 2), vec![3, 4]);
        // Grew by 1 yet lost an order: newest orders merge into the arrival.
        assert_eq!(count(&[5, 3, 4], 13, 0, 2), vec![5, 8]);
        // Flat total, more orders: the newest order splits.
        assert_eq!(count(&[6], 6, 0, 3), vec![2, 2, 2]);
        // Without counts it is hybrid.
        assert_eq!(
            run(&mut Count, &[5, 3, 4], 6, 2),
            run(&mut Hybrid, &[5, 3, 4], 6, 2)
        );
    }

    #[test]
    fn count_model_keeps_total_and_reaches_feasible_counts() {
        let mut s: u64 = 0x2545_f491_4f6c_dd1d;
        let mut rnd = |n: u64| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s % n
        };
        for _ in 0..5000 {
            let sizes_in: Vec<Qty> = (0..rnd(7)).map(|_| 1 + rnd(6)).collect();
            let old: Qty = sizes_in.iter().sum();
            let new_total = rnd(old + 10);
            let traded = if rnd(3) == 0 { rnd(old + 1) } else { 0 };
            let n = if new_total == 0 {
                0
            } else {
                1 + rnd(new_total.min(9)) as u32
            };
            let out = run_counted(&mut Count, &sizes_in, new_total, traded, Some(n));
            assert_eq!(
                out.len(),
                n as usize,
                "{sizes_in:?} -> {new_total} (traded {traded})"
            );
        }
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
