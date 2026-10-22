//! Ground-truth order-by-order book built from a venue's L3/L4 feed.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::types::{BestFirst, Owner, Px, Qty, Side, Ts};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruthOrder {
    pub id: u64,
    pub qty: Qty,
    pub born: Ts,
    /// Placing account, when the venue publishes it (L4).
    pub owner: Option<Owner>,
}

/// Resting orders at one price, in priority order.
#[derive(Debug, Default)]
pub struct TruthLevel {
    orders: VecDeque<TruthOrder>,
    total: Qty,
}

impl TruthLevel {
    pub fn total(&self) -> Qty {
        self.total
    }

    pub fn orders(&self) -> &VecDeque<TruthOrder> {
        &self.orders
    }

    /// Quantity resting ahead of `id`, or `None` if `id` is not at this level.
    pub fn qty_ahead(&self, id: u64) -> Option<Qty> {
        let mut ahead = 0;
        for order in &self.orders {
            if order.id == id {
                return Some(ahead);
            }
            ahead += order.qty;
        }
        None
    }

    /// Inserts directly in front of `before` when it rests here, else at the back.
    /// Returns false if an anchor was given but not found.
    fn insert(&mut self, order: TruthOrder, before: Option<u64>) -> bool {
        self.total += order.qty;
        let Some(anchor) = before else {
            self.orders.push_back(order);
            return true;
        };
        match self.orders.iter().position(|o| o.id == anchor) {
            Some(pos) => {
                self.orders.insert(pos, order);
                true
            }
            None => {
                self.orders.push_back(order);
                false
            }
        }
    }

    fn remove(&mut self, id: u64) -> Option<TruthOrder> {
        let pos = self.orders.iter().position(|o| o.id == id)?;
        let order = self.orders.remove(pos)?;
        self.total -= order.qty;
        Some(order)
    }

    fn set_qty(&mut self, id: u64, qty: Qty) {
        if let Some(order) = self.orders.iter_mut().find(|o| o.id == id) {
            self.total = self.total - order.qty + qty;
            order.qty = qty;
        }
    }
}

/// One resting order in a venue snapshot; per-level priority follows snapshot order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotOrder {
    pub id: u64,
    pub side: Side,
    pub px: Px,
    pub qty: Qty,
    pub born: Ts,
    pub owner: Option<Owner>,
}

/// Venue-normalised L3 message. Quantities are absolute (not deltas) so applying a
/// message twice, or one already reflected in the snapshot, is harmless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum L3Msg {
    Add {
        id: u64,
        side: Side,
        px: Px,
        qty: Qty,
        owner: Option<Owner>,
        /// Rest directly in front of this resting order (priority insert) instead of
        /// at the back of the level.
        insert_before: Option<u64>,
    },
    /// Order still live with `remaining` open; `traded_total` is its cumulative fill.
    Update {
        id: u64,
        side: Side,
        px: Px,
        remaining: Qty,
        traded_total: Qty,
    },
    /// Order gone; `traded_total` is its cumulative fill (the rest was cancelled).
    Delete { id: u64, traded_total: Qty },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L3Event {
    pub ts: Ts,
    pub msg: L3Msg,
}

/// A resting price level touched by one L3 event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelEffect {
    pub side: Side,
    pub px: Px,
    /// Level total before the event.
    pub before: Qty,
    /// Level order count before the event.
    pub before_count: u32,
    /// Maker volume filled at this level by the event.
    pub traded: Qty,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TruthStats {
    /// Update/Delete for an id the book never saw (normal right after a snapshot).
    pub unknown_refs: u64,
    /// Add for an id already live (normal right after a snapshot).
    pub duplicate_adds: u64,
    /// Priority inserts whose anchor was not resting at that level (rested at the back).
    pub missing_anchors: u64,
}

#[derive(Debug)]
enum State {
    Resting,
    /// Marketable on arrival: it trades as taker and only rests once it stops crossing.
    /// Relies on the venue reporting the taker's own fills before the maker fills
    /// that clear the way (Bitstamp does), so taker volume never counts as maker volume.
    Pending {
        qty: Qty,
    },
}

#[derive(Debug)]
struct Meta {
    side: Side,
    px: Px,
    born: Ts,
    owner: Option<Owner>,
    traded: Qty,
    state: State,
}

/// An order about to enter the book.
struct Entry {
    id: u64,
    side: Side,
    px: Px,
    qty: Qty,
    born: Ts,
    owner: Option<Owner>,
    traded: Qty,
    insert_before: Option<u64>,
}

#[derive(Debug, Default)]
pub struct TruthBook {
    bids: BTreeMap<Px, TruthLevel>,
    asks: BTreeMap<Px, TruthLevel>,
    index: HashMap<u64, Meta>,
    pending: Vec<u64>,
    stats: TruthStats,
}

impl TruthBook {
    pub fn seed(&mut self, orders: impl IntoIterator<Item = SnapshotOrder>) {
        *self = Self::default();
        for o in orders {
            if o.qty == 0 || self.index.contains_key(&o.id) {
                continue;
            }
            self.index.insert(
                o.id,
                Meta {
                    side: o.side,
                    px: o.px,
                    born: o.born,
                    owner: o.owner,
                    traded: 0,
                    state: State::Resting,
                },
            );
            self.side_mut(o.side).entry(o.px).or_default().insert(
                TruthOrder {
                    id: o.id,
                    qty: o.qty,
                    born: o.born,
                    owner: o.owner,
                },
                None,
            );
        }
    }

    /// Applies one event, appending every resting level it touched to `out`.
    pub fn apply(&mut self, ev: &L3Event, out: &mut Vec<LevelEffect>) {
        match ev.msg {
            L3Msg::Add {
                id,
                side,
                px,
                qty,
                owner,
                insert_before,
            } => {
                if self.index.contains_key(&id) {
                    self.stats.duplicate_adds += 1;
                } else if qty > 0 {
                    self.place(
                        Entry {
                            id,
                            side,
                            px,
                            qty,
                            born: ev.ts,
                            owner,
                            traded: 0,
                            insert_before,
                        },
                        out,
                    );
                }
            }
            L3Msg::Update {
                id,
                side,
                px,
                remaining,
                traded_total,
            } => self.update(id, side, px, remaining, traded_total, out),
            L3Msg::Delete { id, traded_total } => self.delete(id, traded_total, out),
        }
        self.settle_pending(out);
    }

    pub fn levels(&self, side: Side) -> BestFirst<'_, TruthLevel> {
        BestFirst::new(side, &self.bids, &self.asks)
    }

    pub fn level(&self, side: Side, px: Px) -> Option<&TruthLevel> {
        self.side(side).get(&px)
    }

    pub fn total(&self, side: Side, px: Px) -> Qty {
        self.level(side, px).map_or(0, TruthLevel::total)
    }

    /// Side and price of a resting order.
    pub fn locate(&self, id: u64) -> Option<(Side, Px)> {
        let meta = self.index.get(&id)?;
        matches!(meta.state, State::Resting).then_some((meta.side, meta.px))
    }

    pub fn best(&self, side: Side) -> Option<Px> {
        self.levels(side).next().map(|(px, _)| px)
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn stats(&self) -> TruthStats {
        self.stats
    }

    fn side(&self, side: Side) -> &BTreeMap<Px, TruthLevel> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<Px, TruthLevel> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    fn crosses(&self, side: Side, px: Px) -> bool {
        match side {
            Side::Bid => self.best(Side::Ask).is_some_and(|ask| px >= ask),
            Side::Ask => self.best(Side::Bid).is_some_and(|bid| px <= bid),
        }
    }

    fn place(&mut self, e: Entry, out: &mut Vec<LevelEffect>) {
        let state = if self.crosses(e.side, e.px) {
            self.pending.push(e.id);
            State::Pending { qty: e.qty }
        } else {
            self.rest(&e, out);
            State::Resting
        };
        self.index.insert(
            e.id,
            Meta {
                side: e.side,
                px: e.px,
                born: e.born,
                owner: e.owner,
                traded: e.traded,
                state,
            },
        );
    }

    fn rest(&mut self, e: &Entry, out: &mut Vec<LevelEffect>) {
        let level = self.side_mut(e.side).entry(e.px).or_default();
        out.push(LevelEffect {
            side: e.side,
            px: e.px,
            before: level.total,
            before_count: level.orders.len() as u32,
            traded: 0,
        });
        let order = TruthOrder {
            id: e.id,
            qty: e.qty,
            born: e.born,
            owner: e.owner,
        };
        if !level.insert(order, e.insert_before) {
            self.stats.missing_anchors += 1;
        }
    }

    /// Removes a resting order, recording `traded` maker volume against its level.
    fn unrest(&mut self, id: u64, side: Side, px: Px, traded: Qty, out: &mut Vec<LevelEffect>) {
        let book = self.side_mut(side);
        let Some(level) = book.get_mut(&px) else {
            return;
        };
        out.push(LevelEffect {
            side,
            px,
            before: level.total,
            before_count: level.orders.len() as u32,
            traded,
        });
        level.remove(id);
        if level.orders.is_empty() {
            book.remove(&px);
        }
    }

    fn update(
        &mut self,
        id: u64,
        side: Side,
        px: Px,
        remaining: Qty,
        traded_total: Qty,
        out: &mut Vec<LevelEffect>,
    ) {
        let Some(meta) = self.index.get_mut(&id) else {
            self.stats.unknown_refs += 1;
            return;
        };
        let fill = traded_total.saturating_sub(meta.traded);
        meta.traded = meta.traded.max(traded_total);
        let (old_side, old_px, born, owner) = (meta.side, meta.px, meta.born, meta.owner);

        if let State::Pending { qty } = &mut meta.state {
            // Taker-side fills never touch the resting book.
            *qty = remaining;
            meta.side = side;
            meta.px = px;
            if remaining == 0 {
                self.drop_meta(id);
            }
            return;
        }

        if remaining == 0 {
            self.drop_meta(id);
            self.unrest(id, old_side, old_px, fill, out);
        } else if (old_side, old_px) != (side, px) {
            // Re-priced: loses priority at the old level and re-enters at the new one.
            self.index.remove(&id);
            self.unrest(id, old_side, old_px, fill, out);
            self.place(
                Entry {
                    id,
                    side,
                    px,
                    qty: remaining,
                    born,
                    owner,
                    traded: traded_total,
                    insert_before: None,
                },
                out,
            );
        } else {
            let level = self
                .side_mut(side)
                .get_mut(&px)
                .expect("resting order indexed at a missing level");
            out.push(LevelEffect {
                side,
                px,
                before: level.total,
                before_count: level.orders.len() as u32,
                traded: fill,
            });
            level.set_qty(id, remaining);
        }
    }

    fn delete(&mut self, id: u64, traded_total: Qty, out: &mut Vec<LevelEffect>) {
        let Some(meta) = self.drop_meta(id) else {
            self.stats.unknown_refs += 1;
            return;
        };
        if let State::Resting = meta.state {
            let fill = traded_total.saturating_sub(meta.traded);
            self.unrest(id, meta.side, meta.px, fill, out);
        }
    }

    fn drop_meta(&mut self, id: u64) -> Option<Meta> {
        let meta = self.index.remove(&id)?;
        if let State::Pending { .. } = meta.state {
            self.pending.retain(|&p| p != id);
        }
        Some(meta)
    }

    /// Rests any marketable order that no longer crosses the opposite side.
    fn settle_pending(&mut self, out: &mut Vec<LevelEffect>) {
        let mut i = 0;
        while i < self.pending.len() {
            let id = self.pending[i];
            let meta = &self.index[&id];
            let State::Pending { qty } = meta.state else {
                unreachable!("pending list holds a resting order");
            };
            let entry = Entry {
                id,
                side: meta.side,
                px: meta.px,
                qty,
                born: meta.born,
                owner: meta.owner,
                traded: meta.traded,
                insert_before: None,
            };
            if self.crosses(entry.side, entry.px) {
                i += 1;
                continue;
            }
            self.pending.swap_remove(i);
            self.index.get_mut(&id).expect("pending id indexed").state = State::Resting;
            self.rest(&entry, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(ts: Ts, msg: L3Msg) -> L3Event {
        L3Event { ts, msg }
    }

    fn ids(book: &TruthBook, side: Side, px: Px) -> Vec<u64> {
        book.level(side, px)
            .map(|l| l.orders().iter().map(|o| o.id).collect())
            .unwrap_or_default()
    }

    fn snap(orders: &[(u64, Side, Px, Qty)]) -> Vec<SnapshotOrder> {
        orders
            .iter()
            .map(|&(id, side, px, qty)| SnapshotOrder {
                id,
                side,
                px,
                qty,
                born: 0,
                owner: None,
            })
            .collect()
    }

    fn add(id: u64, side: Side, px: Px, qty: Qty, insert_before: Option<u64>) -> L3Msg {
        L3Msg::Add {
            id,
            side,
            px,
            qty,
            owner: None,
            insert_before,
        }
    }

    #[test]
    fn partial_fill_keeps_priority_and_reports_maker_volume() {
        let mut book = TruthBook::default();
        book.seed(snap(&[(1, Side::Bid, 100, 5), (2, Side::Bid, 100, 3)]));
        let mut out = Vec::new();
        book.apply(
            &ev(
                1,
                L3Msg::Update {
                    id: 1,
                    side: Side::Bid,
                    px: 100,
                    remaining: 2,
                    traded_total: 3,
                },
            ),
            &mut out,
        );
        assert_eq!(ids(&book, Side::Bid, 100), vec![1, 2]);
        assert_eq!(book.total(Side::Bid, 100), 5);
        assert_eq!(
            out,
            vec![LevelEffect {
                side: Side::Bid,
                px: 100,
                before: 8,
                before_count: 2,
                traded: 3
            }]
        );
    }

    #[test]
    fn marketable_order_trades_as_taker_then_rests_remainder() {
        let mut book = TruthBook::default();
        book.seed(snap(&[(1, Side::Ask, 101, 2), (2, Side::Bid, 99, 1)]));
        let mut out = Vec::new();
        // Buy 5 @ 101 crosses the 2 resting at 101.
        book.apply(&ev(1, add(9, Side::Bid, 101, 5, None)), &mut out);
        assert!(out.is_empty(), "taker must not touch the book on arrival");
        // Bitstamp reports the taker's own fill before the makers it hit.
        book.apply(
            &ev(
                1,
                L3Msg::Update {
                    id: 9,
                    side: Side::Bid,
                    px: 101,
                    remaining: 3,
                    traded_total: 2,
                },
            ),
            &mut out,
        );
        assert_eq!(book.pending_len(), 1, "still crosses the maker");
        book.apply(
            &ev(
                1,
                L3Msg::Delete {
                    id: 1,
                    traded_total: 2,
                },
            ),
            &mut out,
        );
        assert_eq!(book.best(Side::Bid), Some(101));
        assert_eq!(book.total(Side::Bid, 101), 3);
        assert_eq!(book.best(Side::Ask), None);
        assert_eq!(book.pending_len(), 0);
        let traded: Qty = out.iter().map(|e| e.traded).sum();
        assert_eq!(traded, 2, "only maker volume counts as traded");
    }

    #[test]
    fn replayed_messages_are_idempotent() {
        let mut book = TruthBook::default();
        book.seed(snap(&[(1, Side::Ask, 101, 2)]));
        let mut out = Vec::new();
        let add = ev(1, add(1, Side::Ask, 101, 2, None));
        book.apply(&add, &mut out);
        let del = ev(
            2,
            L3Msg::Delete {
                id: 7,
                traded_total: 0,
            },
        );
        book.apply(&del, &mut out);
        assert!(out.is_empty());
        assert_eq!(book.total(Side::Ask, 101), 2);
        assert_eq!(
            book.stats(),
            TruthStats {
                unknown_refs: 1,
                duplicate_adds: 1,
                missing_anchors: 0
            }
        );
    }

    #[test]
    fn priority_insert_jumps_ahead_of_anchor() {
        let mut book = TruthBook::default();
        book.seed(snap(&[(1, Side::Bid, 100, 5), (2, Side::Bid, 100, 3)]));
        let mut out = Vec::new();
        book.apply(&ev(1, add(3, Side::Bid, 100, 1, Some(2))), &mut out);
        assert_eq!(ids(&book, Side::Bid, 100), vec![1, 3, 2]);
        assert_eq!(book.level(Side::Bid, 100).unwrap().qty_ahead(2), Some(6));
        // Unknown anchor falls back to the back of the queue and is counted.
        book.apply(&ev(2, add(4, Side::Bid, 100, 1, Some(99))), &mut out);
        assert_eq!(ids(&book, Side::Bid, 100), vec![1, 3, 2, 4]);
        assert_eq!(book.stats().missing_anchors, 1);
    }
}
