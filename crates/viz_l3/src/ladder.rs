//! Order-book ladder: per level, the true queue above the inferred queue, one
//! segment per order (front of queue on the left), shared x-scale.

use egui::{Align2, Color32, FontId, Painter, Pos2, Rect, RichText, Sense, Stroke, Ui, pos2, vec2};
use l3lab::model::InferredBook;
use l3lab::truth::TruthBook;
use l3lab::types::{Px, Qty, Scale, Side, Ts};

use crate::fmt;

const BAR_H: f32 = 11.0;
const BAR_GAP: f32 = 2.0;
const ROW_PAD: f32 = 3.0;
const PX_W: f32 = 100.0;
const TOTAL_W: f32 = 110.0;
const TAG_W: f32 = 110.0;
const RIGHT_PAD: f32 = 8.0;
const SEG_GAP: f32 = 1.0;
const AGE_MIN_S: f64 = 0.1;
const AGE_MAX_S: f64 = 600.0;
const BID_TINT: Color32 = Color32::from_rgb(90, 200, 120);
const ASK_TINT: Color32 = Color32::from_rgb(230, 100, 90);

pub struct Ladder<'a> {
    pub truth: Option<&'a TruthBook>,
    pub model: &'a InferredBook,
    pub scale: Scale,
    /// Exchange time used to age orders.
    pub now: Ts,
    pub depth: usize,
}

#[derive(Clone, Copy)]
struct Seg {
    id: u64,
    qty: Qty,
    born: Ts,
    /// `Some(batch)` for inferred orders.
    born_batch: Option<u64>,
}

impl Ladder<'_> {
    pub fn show(&self, ui: &mut Ui) {
        let asks = self.prices(Side::Ask);
        let bids = self.prices(Side::Bid);
        let max_total = asks
            .iter()
            .map(|&px| self.max_total(Side::Ask, px))
            .chain(bids.iter().map(|&px| self.max_total(Side::Bid, px)))
            .max()
            .unwrap_or(0)
            .max(1);
        self.header(ui, max_total);
        ui.separator();
        egui::ScrollArea::vertical()
            .auto_shrink(false)
            .show(ui, |ui| {
                ui.spacing_mut().item_spacing.y = 1.0;
                for &px in asks.iter().rev() {
                    self.row(ui, Side::Ask, px, max_total);
                }
                self.spread(ui, asks.first().copied(), bids.first().copied());
                for &px in &bids {
                    self.row(ui, Side::Bid, px, max_total);
                }
            });
    }

    /// Displayed prices, best first; truth defines the levels when present.
    fn prices(&self, side: Side) -> Vec<Px> {
        match self.truth {
            Some(t) => t.levels(side).take(self.depth).map(|(px, _)| px).collect(),
            None => self
                .model
                .levels(side)
                .take(self.depth)
                .map(|(px, _)| px)
                .collect(),
        }
    }

    fn truth_total(&self, side: Side, px: Px) -> Option<Qty> {
        Some(self.truth?.level(side, px)?.total())
    }

    fn inferred_total(&self, side: Side, px: Px) -> Qty {
        self.model.queue(side, px).map_or(0, |q| q.total())
    }

    fn max_total(&self, side: Side, px: Px) -> Qty {
        self.truth_total(side, px)
            .unwrap_or(0)
            .max(self.inferred_total(side, px))
    }

    fn header(&self, ui: &mut Ui, max_total: Qty) {
        ui.horizontal(|ui| {
            ui.label(RichText::new(format!("{:>12}", "price")).monospace());
            ui.label(RichText::new(format!("{:>13}", "size")).monospace());
            ui.add_space(24.0);
            ui.label(format!(
                "queues: front → back, full width = {}",
                fmt::qty(self.scale, max_total)
            ));
            ui.add_space(24.0);
            ui.label("age:");
            age_legend(ui, Side::Bid);
            age_legend(ui, Side::Ask);
        });
    }

    fn spread(&self, ui: &mut Ui, best_ask: Option<Px>, best_bid: Option<Px>) {
        let (rect, _) = ui.allocate_exact_size(vec2(ui.available_width(), 18.0), Sense::hover());
        let painter = ui.painter_at(rect);
        let stroke = Stroke::new(1.0_f32, ui.visuals().weak_text_color());
        painter.hline(rect.x_range(), rect.center().y, stroke);
        let text = match (best_ask, best_bid) {
            (Some(a), Some(b)) => format!(" spread {} ", fmt::px(self.scale, a.saturating_sub(b))),
            _ => " one-sided book ".to_owned(),
        };
        let galley =
            painter.layout_no_wrap(text, FontId::proportional(11.0), ui.visuals().text_color());
        let pos = pos2(
            rect.left() + PX_W - galley.size().x,
            rect.center().y - galley.size().y / 2.0,
        );
        painter.rect_filled(
            Rect::from_min_size(pos, galley.size()),
            0.0,
            ui.visuals().panel_fill,
        );
        painter.galley(pos, galley, Color32::PLACEHOLDER);
    }

    fn row(&self, ui: &mut Ui, side: Side, px: Px, max_total: Qty) {
        let bars = if self.truth.is_some() { 2.0 } else { 1.0 };
        let height = bars * BAR_H + (bars - 1.0) * BAR_GAP + 2.0 * ROW_PAD;
        let (rect, response) =
            ui.allocate_exact_size(vec2(ui.available_width(), height), Sense::hover());
        if !ui.is_rect_visible(rect) {
            return;
        }
        let painter = ui.painter_at(rect);
        let visuals = ui.visuals();
        let tint = side_tint(side);
        let mono = FontId::monospace(12.0);
        let small = FontId::proportional(10.0);
        let mid_y = rect.center().y;

        let total = self
            .truth_total(side, px)
            .unwrap_or_else(|| self.inferred_total(side, px));
        painter.text(
            pos2(rect.left() + PX_W, mid_y),
            Align2::RIGHT_CENTER,
            fmt::px(self.scale, px),
            mono.clone(),
            tint,
        );
        painter.text(
            pos2(rect.left() + PX_W + TOTAL_W, mid_y),
            Align2::RIGHT_CENTER,
            fmt::qty(self.scale, total),
            mono,
            visuals.text_color(),
        );

        let left = rect.left() + PX_W + TOTAL_W + TAG_W;
        let width = (rect.right() - RIGHT_PAD - left).max(10.0);
        let sx = f64::from(width) / max_total as f64;
        let hover = response.hover_pos();
        let mut hit: Option<Seg> = None;
        let mut top = rect.top() + ROW_PAD;

        if let Some(truth) = self.truth {
            self.track(&painter, visuals, left, top, width, "truth", &small);
            if let Some(level) = truth.level(side, px) {
                let segs = level.orders().iter().map(|o| Seg {
                    id: o.id,
                    qty: o.qty,
                    born: o.born,
                    born_batch: None,
                });
                hit = hit.or(self.bar(&painter, side, left, top, sx, segs, hover));
            }
            top += BAR_H + BAR_GAP;
        }

        self.track(
            &painter,
            visuals,
            left,
            top,
            width,
            self.model.name(),
            &small,
        );
        if let Some(queue) = self.model.queue(side, px) {
            let segs = queue.orders().iter().map(|o| Seg {
                id: o.id,
                qty: o.qty,
                born: o.born,
                born_batch: Some(o.born_batch),
            });
            hit = hit.or(self.bar(&painter, side, left, top, sx, segs, hover));
        }

        if let Some(seg) = hit {
            response.on_hover_ui_at_pointer(|ui| self.tooltip(ui, side, px, seg));
        }
    }

    /// Bar background plus its label.
    #[allow(clippy::too_many_arguments)]
    fn track(
        &self,
        painter: &Painter,
        visuals: &egui::Visuals,
        left: f32,
        top: f32,
        width: f32,
        tag: &str,
        font: &FontId,
    ) {
        painter.text(
            pos2(left - 6.0, top + BAR_H / 2.0),
            Align2::RIGHT_CENTER,
            tag,
            font.clone(),
            visuals.weak_text_color(),
        );
        painter.rect_filled(
            Rect::from_min_size(pos2(left, top), vec2(width, BAR_H)),
            0.0,
            visuals.faint_bg_color,
        );
    }

    /// Paints one queue; returns the segment under the pointer.
    #[allow(clippy::too_many_arguments)]
    fn bar(
        &self,
        painter: &Painter,
        side: Side,
        left: f32,
        top: f32,
        sx: f64,
        segs: impl Iterator<Item = Seg>,
        hover: Option<Pos2>,
    ) -> Option<Seg> {
        let hover = hover.filter(|p| p.y >= top && p.y < top + BAR_H);
        let mut cum: Qty = 0;
        let mut hit = None;
        for seg in segs {
            let x0 = left + (cum as f64 * sx) as f32;
            cum += seg.qty;
            let x1 = left + (cum as f64 * sx) as f32;
            let gap = if x1 - x0 > 3.0 { SEG_GAP } else { 0.0 };
            let hovered = hover.is_some_and(|p| p.x >= x0 && p.x < x1);
            let color = if hovered {
                Color32::WHITE
            } else {
                age_color(side, self.now.saturating_sub(seg.born))
            };
            painter.rect_filled(
                Rect::from_min_max(pos2(x0, top), pos2((x1 - gap).max(x0 + 0.5), top + BAR_H)),
                0.0,
                color,
            );
            if hovered {
                hit = Some(seg);
            }
        }
        hit
    }

    fn tooltip(&self, ui: &mut Ui, side: Side, px: Px, seg: Seg) {
        let title = match seg.born_batch {
            None => "truth order".to_owned(),
            Some(_) => format!("inferred order ({})", self.model.name()),
        };
        ui.label(RichText::new(title).strong());
        let side_name = match side {
            Side::Bid => "bid",
            Side::Ask => "ask",
        };
        ui.label(format!("{side_name} @ {}", fmt::px(self.scale, px)));
        ui.label(format!("qty: {}", fmt::qty(self.scale, seg.qty)));
        ui.label(format!(
            "age: {}",
            fmt::age(self.now.saturating_sub(seg.born))
        ));
        ui.label(format!("id: {}", seg.id));
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
}

fn side_tint(side: Side) -> Color32 {
    match side {
        Side::Bid => BID_TINT,
        Side::Ask => ASK_TINT,
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
        Side::Bid => ([120, 245, 160], [20, 70, 65]),
        Side::Ask => ([255, 150, 115], [95, 28, 45]),
    };
    let mix = |a: u8, b: u8| (f32::from(a) + (f32::from(b) - f32::from(a)) * t).round() as u8;
    Color32::from_rgb(
        mix(young[0], old[0]),
        mix(young[1], old[1]),
        mix(young[2], old[2]),
    )
}

fn age_legend(ui: &mut Ui, side: Side) {
    ui.label(RichText::new("0.1s").small());
    let (rect, _) = ui.allocate_exact_size(vec2(80.0, 10.0), Sense::hover());
    let painter = ui.painter_at(rect);
    const STEPS: usize = 40;
    let step = rect.width() / STEPS as f32;
    for i in 0..STEPS {
        let x = rect.left() + i as f32 * step;
        painter.rect_filled(
            Rect::from_min_max(pos2(x, rect.top()), pos2(x + step + 0.5, rect.bottom())),
            0.0,
            gradient(side, i as f32 / (STEPS - 1) as f32),
        );
    }
    ui.label(RichText::new("10m").small());
}
