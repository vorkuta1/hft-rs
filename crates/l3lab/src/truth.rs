//! Ground-truth order-by-order book built from a venue's L3 feed.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::types::{BestFirst, Px, Qty, Side, Ts};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruthOrder {
    pub id: u64,
    pub qty: Qty,
    pub born: Ts,
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

    fn push_back(&mut self, order: TruthOrder) {
        self.total += order.qty;
        self.orders.push_back(order);
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

/// Venue-normalised L3 message. Quantities are absolute (not deltas) so applying a
/// message twice, or one already reflected in the snapshot, is harmless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum L3Msg {
    Add {
        id: u64,
        side: Side,
        px: Px,
        qty: Qty,
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
    /// Maker volume filled at this level by the event.
    pub traded: Qty,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TruthStats {
    /// Update/Delete for an id the book never saw (normal right after a snapshot).
    pub unknown_refs: u64,
    /// Add for an id already live (normal right after a snapshot).
    pub duplicate_adds: u64,
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
    traded: Qty,
    state: State,
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
    /// Seeds resting orders; per-level priority follows iteration order.
    pub fn seed(&mut self, ts: Ts, orders: impl IntoIterator<Item = (u64, Side, Px, Qty)>) {
        *self = Self::default();
        for (id, side, px, qty) in orders {
            if qty == 0 || self.index.contains_key(&id) {
                continue;
            }
            self.index.insert(
                id,
                Meta {
                    side,
                    px,
                    born: ts,
                    traded: 0,
                    state: State::Resting,
                },
            );
            self.side_mut(side)
                .entry(px)
                .or_default()
                .push_back(TruthOrder { id, qty, born: ts });
        }
    }

    /// Applies one event, appending every resting level it touched to `out`.
    pub fn apply(&mut self, ev: &L3Event, out: &mut Vec<LevelEffect>) {
        match ev.msg {
            L3Msg::Add { id, side, px, qty } => {
                if self.index.contains_key(&id) {
                    self.stats.duplicate_adds += 1;
                } else if qty > 0 {
                    self.place(id, side, px, qty, ev.ts, 0, out);
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

    #[allow(clippy::too_many_arguments)]
    fn place(
        &mut self,
        id: u64,
        side: Side,
        px: Px,
        qty: Qty,
        born: Ts,
        traded: Qty,
        out: &mut Vec<LevelEffect>,
    ) {
        let state = if self.crosses(side, px) {
            self.pending.push(id);
            State::Pending { qty }
        } else {
            self.rest(id, side, px, qty, born, 0, out);
            State::Resting
        };
        self.index.insert(
            id,
            Meta {
                side,
                px,
                born,
                traded,
                state,
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn rest(
        &mut self,
        id: u64,
        side: Side,
        px: Px,
        qty: Qty,
        born: Ts,
        traded: Qty,
        out: &mut Vec<LevelEffect>,
    ) {
        let level = self.side_mut(side).entry(px).or_default();
        out.push(LevelEffect {
            side,
            px,
            before: level.total,
            traded,
        });
        level.push_back(TruthOrder { id, qty, born });
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
        let (old_side, old_px, born) = (meta.side, meta.px, meta.born);

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
            self.place(id, side, px, remaining, born, traded_total, out);
        } else {
            let level = self
                .side_mut(side)
                .get_mut(&px)
                .expect("resting order indexed at a missing level");
            out.push(LevelEffect {
                side,
                px,
                before: level.total,
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
            let (side, px, born) = (meta.side, meta.px, meta.born);
            let State::Pending { qty } = meta.state else {
                unreachable!("pending list holds a resting order");
            };
            if self.crosses(side, px) {
                i += 1;
                continue;
            }
            self.pending.swap_remove(i);
            self.index.get_mut(&id).expect("pending id indexed").state = State::Resting;
            self.rest(id, side, px, qty, born, 0, out);
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

    #[test]
    fn partial_fill_keeps_priority_and_reports_maker_volume() {
        let mut book = TruthBook::default();
        book.seed(0, [(1, Side::Bid, 100, 5), (2, Side::Bid, 100, 3)]);
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
                traded: 3
            }]
        );
    }

    #[test]
    fn marketable_order_trades_as_taker_then_rests_remainder() {
        let mut book = TruthBook::default();
        book.seed(0, [(1, Side::Ask, 101, 2), (2, Side::Bid, 99, 1)]);
        let mut out = Vec::new();
        // Buy 5 @ 101 crosses the 2 resting at 101.
        book.apply(
            &ev(
                1,
                L3Msg::Add {
                    id: 9,
                    side: Side::Bid,
                    px: 101,
                    qty: 5,
                },
            ),
            &mut out,
        );
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
        book.seed(0, [(1, Side::Ask, 101, 2)]);
        let mut out = Vec::new();
        let add = ev(
            1,
            L3Msg::Add {
                id: 1,
                side: Side::Ask,
                px: 101,
                qty: 2,
            },
        );
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
                duplicate_adds: 1
            }
        );
    }
}
