//! Upstream-style horizontal order book (after binance_l3_est's order book view):
//! price levels run outward from the mid (bids left, asks right), each level is a
//! vertical bar stacked bottom-to-top in queue order (front of the queue at the
//! bottom), one segment per order. Segments are coloured by order age, or by
//! k-means size cluster; the largest order per side is gold.

use std::ops::Range;

use egui::{Align2, Color32, FontId, Painter, Pos2, Rect, RichText, Sense, Stroke, Ui, pos2, vec2};
use l3lab::model::InferredBook;
use l3lab::truth::TruthBook;
use l3lab::types::{Owner, Px, Qty, Scale, Side, Ts};

use crate::fmt;
use crate::kmeans::KMeans;

pub const MAX_LEVELS: usize = 200;
const TABLE_ROWS: usize = 20;
const AGE_MIN_S: f64 = 0.1;
const AGE_MAX_S: f64 = 600.0;
const AXIS_W: f32 = 44.0;
const AXIS_H: f32 = 22.0;
const LABEL_SPACING: f32 = 90.0;

pub const BID: Color32 = Color32::from_rgb(80, 220, 80);
pub const ASK: Color32 = Color32::from_rgb(230, 60, 50);
const GRID: Color32 = Color32::from_gray(48);
const AXIS: Color32 = Color32::from_gray(150);

/// binance_l3_est's k-means palettes, light (small orders) to dark (large).
const BID_COLORS: [Color32; 10] = [
    Color32::from_rgb(222, 235, 247),
    Color32::from_rgb(204, 227, 245),
    Color32::from_rgb(158, 202, 225),
    Color32::from_rgb(129, 189, 231),
    Color32::from_rgb(107, 174, 214),
    Color32::from_rgb(78, 157, 202),
    Color32::from_rgb(49, 130, 189),
    Color32::from_rgb(33, 113, 181),
    Color32::from_rgb(16, 96, 168),
    Color32::from_rgb(8, 81, 156),
];
const ASK_COLORS: [Color32; 10] = [
    Color32::from_rgb(254, 230, 206),
    Color32::from_rgb(253, 216, 186),
    Color32::from_rgb(253, 174, 107),
    Color32::from_rgb(253, 159, 88),
    Color32::from_rgb(253, 141, 60),
    Color32::from_rgb(245, 126, 47),
    Color32::from_rgb(230, 85, 13),
    Color32::from_rgb(204, 75, 12),
    Color32::from_rgb(179, 65, 10),
    Color32::from_rgb(166, 54, 3),
];

/// Which queues the chart shows when there is ground truth.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum View {
    Truth,
    Inferred,
    /// Two thin bars per level: truth left, inferred right.
    Both,
}

/// Inputs for one frame.
pub struct Books<'a> {
    pub truth: Option<&'a TruthBook>,
    pub model: &'a InferredBook,
    pub scale: Scale,
    /// Exchange time used to age orders.
    pub now: Ts,
}

#[derive(Clone, Copy)]
struct Seg {
    id: u64,
    qty: Qty,
    born: Ts,
    /// `Some(batch)` for inferred orders.
    born_batch: Option<u64>,
    owner: Option<Owner>,
}

#[derive(Clone)]
struct Level {
    px: Px,
    total: Qty,
    /// Range into the queue's segment list (`Queue::segs`).
    segs: Range<usize>,
}

/// One book's displayed side: levels best-first, their orders, cluster labels and
/// the largest order.
#[derive(Default)]
struct Queue {
    levels: Vec<Level>,
    segs: Vec<Seg>,
    labels: Vec<u8>,
    largest: Option<usize>,
}

impl Queue {
    fn clear(&mut self) {
        self.levels.clear();
        self.segs.clear();
        self.labels.clear();
        self.largest = None;
    }

    fn push(&mut self, px: Px, total: Qty, orders: impl Iterator<Item = Seg>) {
        let start = self.segs.len();
        self.segs.extend(orders);
        self.levels.push(Level {
            px,
            total,
            segs: start..self.segs.len(),
        });
    }

    /// Finds the largest order and, when `km` is given, clusters order sizes.
    fn finish(&mut self, km: Option<&mut KMeans>) {
        self.largest = self
            .segs
            .iter()
            .enumerate()
            .max_by(|(ia, a), (ib, b)| a.qty.cmp(&b.qty).then(ib.cmp(ia)))
            .map(|(i, _)| i);
        if let Some(km) = km {
            km.fit(self.segs.iter().map(|s| s.qty as f64), &mut self.labels);
        }
    }
}

/// A bar under the pointer.
struct Hit {
    truth: bool,
    side: Side,
    px: Px,
    seg: Seg,
}

pub struct BookView {
    pub levels: usize,
    pub kmeans: bool,
    pub view: View,
    /// `[truth, inferred][bid, ask]`, rebuilt every frame (buffers reused).
    queues: [[Queue; 2]; 2],
    km: KMeans,
}

impl BookView {
    pub fn new(levels: usize) -> Self {
        Self {
            levels: levels.clamp(1, MAX_LEVELS),
            kmeans: false,
            view: View::Both,
            queues: Default::default(),
            km: KMeans::default(),
        }
    }

    /// Effective view: inferred only when there is no truth.
    fn view(&self, books: &Books) -> View {
        if books.truth.is_some() {
            self.view
        } else {
            View::Inferred
        }
    }

    /// Collects the displayed levels; truth defines the price levels when shown.
    pub fn prepare(&mut self, books: &Books) {
        let view = self.view(books);
        for side in [Side::Bid, Side::Ask] {
            let [truth_q, inf_q] = &mut self.queues;
            let (tq, iq) = (&mut truth_q[side as usize], &mut inf_q[side as usize]);
            tq.clear();
            iq.clear();
            if let (Some(truth), View::Truth | View::Both) = (books.truth, view) {
                for (px, level) in truth.levels(side).take(self.levels) {
                    tq.push(px, level.total(), level.orders().iter().map(truth_seg));
                    if view == View::Both {
                        match books.model.queue(side, px) {
                            Some(q) => iq.push(px, q.total(), q.orders().iter().map(inf_seg)),
                            None => iq.push(px, 0, std::iter::empty()),
                        }
                    }
                }
            } else {
                for (px, q) in books.model.levels(side).take(self.levels) {
                    iq.push(px, q.total(), q.orders().iter().map(inf_seg));
                }
            }
            if self.kmeans {
                tq.finish(Some(&mut self.km));
                iq.finish(Some(&mut self.km));
            } else {
                tq.finish(None);
                iq.finish(None);
            }
        }
    }

    /// The queue that defines the displayed price levels.
    fn primary(&self, side: Side) -> &Queue {
        let t = &self.queues[0][side as usize];
        if t.levels.is_empty() {
            &self.queues[1][side as usize]
        } else {
            t
        }
    }

    /// Asks (top 20, far to near) then bids (top 20), price and level total.
    pub fn table(&self, ui: &mut Ui, books: &Books) {
        let mono = |s: String| RichText::new(s).monospace().size(11.5);
        egui::ScrollArea::vertical()
            .id_salt("level_table")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui::Grid::new("level_table")
                    .striped(true)
                    .num_columns(3)
                    .spacing(vec2(10.0, 1.0))
                    .show(ui, |ui| {
                        for side in [Side::Ask, Side::Bid] {
                            let (name, color) = match side {
                                Side::Ask => ("Asks", ASK),
                                Side::Bid => ("Bids", BID),
                            };
                            ui.label(RichText::new(name).small().color(color));
                            ui.label(RichText::new("Price").small());
                            ui.label(RichText::new("Quantity").small());
                            ui.end_row();
                            let rows = self.primary(side).levels.iter().take(TABLE_ROWS);
                            let rows: Vec<&Level> = match side {
                                Side::Ask => rows.rev().collect(),
                                Side::Bid => rows.collect(),
                            };
                            for level in rows {
                                ui.label("");
                                ui.label(mono(fmt::px(books.scale, level.px)).color(color));
                                ui.label(mono(fmt::qty(books.scale, level.total)));
                                ui.end_row();
                            }
                        }
                    });
            });
    }

    pub fn chart(&self, ui: &mut Ui, books: &Books) {
        let (rect, response) = ui.allocate_exact_size(ui.available_size(), Sense::hover());
        let painter = ui.painter_at(rect);
        let plot = Rect::from_min_max(
            pos2(rect.left() + AXIS_W, rect.top() + 4.0),
            pos2(rect.right() - 8.0, rect.bottom() - AXIS_H),
        );
        let view = self.view(books);
        let shown: &[usize] = match view {
            View::Truth => &[0],
            View::Inferred => &[1],
            View::Both => &[0, 1],
        };

        let max_total = shown
            .iter()
            .flat_map(|&b| self.queues[b].iter())
            .flat_map(|q| q.levels.iter().map(|l| l.total))
            .max()
            .unwrap_or(0);
        let max_y = books.scale.qty_f64(max_total).max(1e-9);
        let (step, top) = nice_axis(max_y, 8);
        let y_of = |q: f64| plot.bottom() - (q / top) as f32 * plot.height();
        let font = FontId::proportional(11.0);

        // Quantity gridlines.
        let mut v = 0.0;
        while v <= top + step * 0.5 {
            let y = y_of(v);
            painter.hline(plot.x_range(), y, Stroke::new(1.0_f32, GRID));
            painter.text(
                pos2(plot.left() - 6.0, y),
                Align2::RIGHT_CENTER,
                trim_float(v, step),
                font.clone(),
                AXIS,
            );
            v += step;
        }

        // Slots: the deeper side's level count on each side of a spread slot, so a
        // thin book (e.g. 20-level venue L2) still fills the width, like upstream's
        // auto-bounded plot.
        let per_side = [Side::Bid, Side::Ask]
            .map(|s| self.primary(s).levels.len())
            .into_iter()
            .max()
            .unwrap_or(0)
            .max(1);
        let slots = 2 * per_side + 1;
        let w = plot.width() / slots as f32;
        let mid_x = plot.center().x;
        let center = |side: Side, i: usize| match side {
            Side::Bid => mid_x - (i as f32 + 1.0) * w,
            Side::Ask => mid_x + (i as f32 + 1.0) * w,
        };

        // Price ticks along the x axis, with vertical gridlines.
        let every = ((LABEL_SPACING / w).ceil() as usize).max(1);
        for side in [Side::Bid, Side::Ask] {
            let levels = &self.primary(side).levels;
            for (i, level) in levels.iter().enumerate() {
                if i % every != every / 2 {
                    continue;
                }
                let x = center(side, i);
                painter.vline(x, plot.y_range(), Stroke::new(1.0_f32, GRID));
                painter.text(
                    pos2(x, plot.bottom() + 4.0),
                    Align2::CENTER_TOP,
                    fmt::px(books.scale, level.px),
                    font.clone(),
                    AXIS,
                );
            }
        }
        painter.hline(plot.x_range(), plot.bottom(), Stroke::new(1.0_f32, AXIS));

        // Bars.
        let hover = response.hover_pos().filter(|p| plot.contains(*p));
        let mut hit = None;
        let bar_w = if shown.len() == 2 { w * 0.45 } else { w * 0.9 };
        for side in [Side::Bid, Side::Ask] {
            for (k, &b) in shown.iter().enumerate() {
                let queue = &self.queues[b][side as usize];
                for (i, level) in queue.levels.iter().enumerate() {
                    let c = center(side, i);
                    let x0 = if shown.len() == 2 {
                        if k == 0 { c - bar_w } else { c }
                    } else {
                        c - bar_w / 2.0
                    };
                    let x1 = x0 + (bar_w - if bar_w > 3.0 { 1.0 } else { 0.0 }).max(0.75);
                    let ctx = BarCtx {
                        painter: &painter,
                        books,
                        side,
                        queue,
                        y_of: &y_of,
                        kmeans: self.kmeans,
                    };
                    if let Some(seg) = ctx.bar(level, x0, x1, hover) {
                        hit = Some(Hit {
                            truth: b == 0,
                            side,
                            px: level.px,
                            seg,
                        });
                    }
                }
            }
        }

        if let Some(hit) = hit {
            response.on_hover_ui_at_pointer(|ui| tooltip(ui, books, &hit));
        }
    }

    /// Legend for the current colouring.
    pub fn legend(&self, ui: &mut Ui, books: &Books) {
        if self.kmeans {
            ui.horizontal(|ui| {
                ui.label(RichText::new("size cluster").small());
                swatches(ui, &BID_COLORS);
                swatches(ui, &ASK_COLORS);
            });
        } else {
            ui.horizontal(|ui| {
                ui.label(RichText::new("age 0.1s").small());
                let (rect, _) = ui.allocate_exact_size(vec2(90.0, 10.0), Sense::hover());
                let half = rect.height() / 2.0;
                const STEPS: usize = 30;
                let step = rect.width() / STEPS as f32;
                for i in 0..STEPS {
                    let t = i as f32 / (STEPS - 1) as f32;
                    let x = rect.left() + i as f32 * step;
                    for (row, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
                        let y = rect.top() + row as f32 * half;
                        ui.painter().rect_filled(
                            Rect::from_min_max(pos2(x, y), pos2(x + step + 0.5, y + half)),
                            0.0,
                            gradient(side, t),
                        );
                    }
                }
                ui.label(RichText::new("10m").small());
            });
        }
        ui.horizontal(|ui| {
            let (rect, _) = ui.allocate_exact_size(vec2(10.0, 10.0), Sense::hover());
            ui.painter().rect_filled(rect, 0.0, Color32::GOLD);
            ui.label(RichText::new("largest order per side").small());
        });
        if let (Some(b), Some(a)) = (
            self.primary(Side::Bid).levels.first(),
            self.primary(Side::Ask).levels.first(),
        ) {
            let mid = (books.scale.px_f64(a.px) + books.scale.px_f64(b.px)) / 2.0;
            ui.label(
                RichText::new(format!(
                    "mid {mid:.*} · spread {}",
                    books.scale.px_decimals as usize + 1,
                    fmt::px(books.scale, a.px.saturating_sub(b.px))
                ))
                .small(),
            );
        }
        if self.view(books) == View::Both {
            ui.label(
                RichText::new(format!(
                    "each level: truth (left) | {} (right)",
                    books.model.name()
                ))
                .small(),
            );
        }
    }
}

struct BarCtx<'a, F> {
    painter: &'a Painter,
    books: &'a Books<'a>,
    side: Side,
    queue: &'a Queue,
    y_of: &'a F,
    kmeans: bool,
}

impl<F: Fn(f64) -> f32> BarCtx<'_, F> {
    /// Paints one level's queue, front at the bottom; returns the hovered order.
    fn bar(&self, level: &Level, x0: f32, x1: f32, hover: Option<Pos2>) -> Option<Seg> {
        let hover = hover.filter(|p| p.x >= x0 && p.x < x1.max(x0 + 1.0));
        let scale = self.books.scale;
        let mut cum: Qty = 0;
        let mut hit = None;
        for i in level.segs.clone() {
            let seg = self.queue.segs[i];
            let y0 = (self.y_of)(scale.qty_f64(cum));
            cum += seg.qty;
            let y1 = (self.y_of)(scale.qty_f64(cum));
            let hovered = hover.is_some_and(|p| p.y <= y0 && p.y > y1);
            let color = if hovered {
                Color32::WHITE
            } else if self.queue.largest == Some(i) {
                Color32::GOLD
            } else if self.kmeans {
                let palette = match self.side {
                    Side::Bid => &BID_COLORS,
                    Side::Ask => &ASK_COLORS,
                };
                palette[self.queue.labels.get(i).copied().unwrap_or(0) as usize % palette.len()]
            } else {
                age_color(self.side, self.books.now.saturating_sub(seg.born))
            };
            // A dark seam between orders when there is room for it.
            let top = if y0 - y1 > 3.0 { y1 + 1.0 } else { y1 };
            self.painter.rect_filled(
                Rect::from_min_max(pos2(x0, top), pos2(x1, y0.max(top + 0.5))),
                0.0,
                color,
            );
            if hovered {
                hit = Some(seg);
            }
        }
        hit
    }
}

fn truth_seg(o: &l3lab::truth::TruthOrder) -> Seg {
    Seg {
        id: o.id,
        qty: o.qty,
        born: o.born,
        born_batch: None,
        owner: o.owner,
    }
}

fn inf_seg(o: &l3lab::model::InfOrder) -> Seg {
    Seg {
        id: o.id,
        qty: o.qty,
        born: o.born,
        born_batch: Some(o.born_batch),
        owner: None,
    }
}

fn tooltip(ui: &mut Ui, books: &Books, hit: &Hit) {
    let title = if hit.truth {
        "truth order".to_owned()
    } else {
        format!("inferred order ({})", books.model.name())
    };
    ui.label(RichText::new(title).strong());
    let side = match hit.side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    };
    let seg = hit.seg;
    ui.label(format!("{side} @ {}", fmt::px(books.scale, hit.px)));
    ui.label(format!("qty: {}", fmt::qty(books.scale, seg.qty)));
    ui.label(format!(
        "age: {}",
        fmt::age(books.now.saturating_sub(seg.born))
    ));
    ui.label(format!("id: {}", seg.id));
    if let Some(owner) = seg.owner {
        ui.label(format!("owner: {owner}"));
    }
    match seg.born_batch {
        Some(0) => {
            ui.label("born: snapshot (unknown composition)");
        }
        Some(batch) => {
            ui.label(format!("born: batch {batch}"));
        }
        None => {}
    }
}

/// Young orders are bright, old ones dark; log scale over `AGE_MIN_S..AGE_MAX_S`.
fn age_color(side: Side, age_us: u64) -> Color32 {
    let secs = (age_us as f64 / 1e6).clamp(AGE_MIN_S, AGE_MAX_S);
    let t = ((secs / AGE_MIN_S).ln() / (AGE_MAX_S / AGE_MIN_S).ln()) as f32;
    gradient(side, t)
}

fn gradient(side: Side, t: f32) -> Color32 {
    let (young, old): ([u8; 3], [u8; 3]) = match side {
        Side::Bid => ([110, 255, 80], [0, 95, 10]),
        Side::Ask => ([255, 85, 60], [115, 5, 5]),
    };
    let mix = |a: u8, b: u8| (f32::from(a) + (f32::from(b) - f32::from(a)) * t).round() as u8;
    Color32::from_rgb(
        mix(young[0], old[0]),
        mix(young[1], old[1]),
        mix(young[2], old[2]),
    )
}

fn swatches(ui: &mut Ui, palette: &[Color32]) {
    let (rect, _) = ui.allocate_exact_size(vec2(6.0 * palette.len() as f32, 10.0), Sense::hover());
    for (i, &c) in palette.iter().enumerate() {
        let x = rect.left() + i as f32 * 6.0;
        ui.painter().rect_filled(
            Rect::from_min_size(pos2(x, rect.top()), vec2(6.0, rect.height())),
            0.0,
            c,
        );
    }
}

/// Gridline step (1/2/5 × 10^k) for about `ticks` lines, and the axis top.
fn nice_axis(max: f64, ticks: usize) -> (f64, f64) {
    let raw = max / ticks as f64;
    let mag = 10f64.powf(raw.log10().floor());
    let step = [1.0, 2.0, 5.0, 10.0]
        .into_iter()
        .map(|m| m * mag)
        .find(|s| *s >= raw)
        .unwrap_or(10.0 * mag);
    (step, (max / step).ceil().max(1.0) * step)
}

fn trim_float(v: f64, step: f64) -> String {
    let decimals = (-step.log10().floor()).max(0.0) as usize;
    format!("{v:.decimals$}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nice_axis_steps() {
        let close = |(s, t): (f64, f64), (es, et): (f64, f64)| {
            assert!((s - es).abs() < 1e-12 && (t - et).abs() < 1e-12, "{s} {t}");
        };
        close(nice_axis(13.0, 8), (2.0, 14.0));
        close(nice_axis(0.37, 8), (0.05, 0.4));
        close(nice_axis(1.0, 8), (0.2, 1.0));
    }
}
