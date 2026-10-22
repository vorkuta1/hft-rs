//! Floating analytics windows after binance_l3_est: microstructure ratios, sweep
//! cost curves and the depth-time heatmap.

use egui::{Color32, Context, Id, Pos2, Rect, TextureHandle, TextureOptions, Ui, vec2};
use egui_plot::{AxisHints, Legend, Line, Plot, PlotPoint, PlotPoints};
use l3lab::micro::{HeatColumn, Microstructure, Ratio, RatioPoint, sweep_curve, sweep_impact};
use l3lab::model::InferredBook;
use l3lab::types::{Scale, Side, Ts};

/// Samples before the ratio plots and heatmap render (as upstream).
const WARMUP: usize = 30;
/// Levels per side fed to the sweep curves (as upstream).
const SWEEP_LEVELS: usize = 100;
const GAP: f32 = 8.0;

/// Default placement: the three windows side by side across the top of `area`.
fn slot(area: Rect, i: usize) -> (Pos2, f32) {
    let w = ((area.width() - 4.0 * GAP) / 3.0).max(260.0);
    (
        Pos2::new(area.left() + GAP + i as f32 * (w + GAP), area.top() + GAP),
        w,
    )
}

pub struct MetricsWindow {
    pub open: bool,
    /// Per ratio: (sample count, last t) the cached points were built from.
    keys: [(usize, f64); 4],
    /// `[ratio][bid, ask, both]`.
    points: [[Vec<PlotPoint>; 3]; 4],
}

const RATIOS: [(Ratio, &str); 4] = [
    (Ratio::CtrTop1, "Top-1 Ratio"),
    (Ratio::CtrTop20, "Top-20 Ratio"),
    (Ratio::OtrTop1, "Top-1 OTR"),
    (Ratio::OtrTop20, "Top-20 OTR"),
];

impl MetricsWindow {
    pub fn new() -> Self {
        Self {
            open: true,
            keys: [(usize::MAX, 0.0); 4],
            points: Default::default(),
        }
    }

    pub fn show(&mut self, ctx: &Context, area: Rect, micro: Option<&Microstructure>) {
        let (pos, w) = slot(area, 0);
        let mut open = self.open;
        egui::Window::new("Microstructure Metrics")
            .open(&mut open)
            .default_pos(pos)
            .default_width(w)
            .show(ctx, |ui| {
                ui.label("Cancellation-to-Trade Ratio (CTR)");
                ui.label("CTR > 1.0: Spoofing/Layering > Fills");
                let samples = micro.map_or(0, |m| m.series(Ratio::CtrTop1).len());
                let Some(micro) = micro.filter(|_| samples >= WARMUP) else {
                    warming_up(ui, "Warming up metrics...", samples);
                    return;
                };
                self.refresh(micro);
                let plot_h = ((area.height() - 170.0) / 4.0).clamp(85.0, 150.0);
                for (i, (_, label)) in RATIOS.iter().enumerate() {
                    if i == 2 {
                        ui.add_space(8.0);
                        ui.separator();
                        ui.label("Order-to-Trade Ratio (OTR)");
                        ui.label("OTR > 1.0: Liquidity Adding > Taking (Reloading)");
                    } else if i > 0 {
                        ui.add_space(4.0);
                    }
                    let [bid, ask, both] = &self.points[i];
                    Plot::new(("ratio_plot", i))
                        .height(plot_h)
                        .link_axis(Id::new("micro_ratio_x"), [true, false])
                        .link_cursor(Id::new("micro_ratio_x"), [true, false])
                        .show_axes([true, true])
                        .y_axis_label(*label)
                        .legend(Legend::default())
                        .show(ui, |plot| {
                            plot.line(
                                Line::new("Ask", PlotPoints::Borrowed(ask)).color(Color32::RED),
                            );
                            plot.line(
                                Line::new("Bid", PlotPoints::Borrowed(bid)).color(Color32::GREEN),
                            );
                            plot.line(
                                Line::new("Both", PlotPoints::Borrowed(both)).color(Color32::WHITE),
                            );
                        });
                }
            });
        self.open = open;
    }

    fn refresh(&mut self, micro: &Microstructure) {
        for (i, (ratio, _)) in RATIOS.iter().enumerate() {
            let series = micro.series(*ratio);
            let key = (series.len(), series.back().map_or(0.0, |p| p.t));
            if self.keys[i] == key {
                continue;
            }
            self.keys[i] = key;
            let fields: [fn(&RatioPoint) -> f64; 3] = [|p| p.bid, |p| p.ask, |p| p.both];
            for (points, field) in self.points[i].iter_mut().zip(fields) {
                points.clear();
                points.extend(series.iter().map(|p| PlotPoint::new(p.t, field(p))));
            }
        }
    }
}

pub struct SweepWindow {
    pub open: bool,
    pub execute_usd: f64,
    /// (batches, model name) the cached curves were built from.
    key: Option<(u64, &'static str)>,
    buy: Vec<PlotPoint>,
    sell: Vec<PlotPoint>,
    mid: Option<f64>,
}

impl SweepWindow {
    pub fn new() -> Self {
        Self {
            open: true,
            execute_usd: 100_000.0,
            key: None,
            buy: Vec::new(),
            sell: Vec::new(),
            mid: None,
        }
    }

    pub fn show(
        &mut self,
        ctx: &Context,
        area: Rect,
        book: &InferredBook,
        scale: Scale,
        batches: u64,
    ) {
        let (pos, w) = slot(area, 1);
        let mut open = self.open;
        egui::Window::new("Sweep / Liquidity-Cost")
            .open(&mut open)
            .default_pos(pos)
            .default_width(w)
            .show(ctx, |ui| {
                self.refresh(book, scale, batches);
                let height = (area.height() * 0.3).clamp(160.0, 300.0);
                Plot::new("liquidity_cost_plot")
                    .height(height)
                    .show_axes([true, true])
                    .custom_x_axes(vec![
                        AxisHints::new_x()
                            .label("Sweep Size (USD)")
                            .formatter(|mark, _| usd(mark.value))
                            .label_spacing(28.0..=40.0),
                    ])
                    .y_axis_label("Price Impact (bps)")
                    .boxed_zoom_pointer_button(egui::PointerButton::Secondary)
                    .auto_bounds([true, true])
                    .legend(Legend::default())
                    .show(ui, |plot| {
                        plot.line(
                            Line::new("Buy", PlotPoints::Borrowed(&self.buy)).color(Color32::BLUE),
                        );
                        plot.line(
                            Line::new("Sell", PlotPoints::Borrowed(&self.sell)).color(Color32::RED),
                        );
                    });
                let impact = |side: Side| {
                    let mid = self.mid?;
                    sweep_impact(levels(book, scale, side), mid, self.execute_usd)
                };
                let show = |v: Option<f64>| v.map_or("N/A".to_owned(), |b| format!("{b:.2} bps"));
                ui.label(format!(
                    "Buy impact = {} | Sell impact = {}",
                    show(impact(Side::Ask)),
                    show(impact(Side::Bid)),
                ));
                let max = self
                    .buy
                    .last()
                    .zip(self.sell.last())
                    .map_or(0.0, |(b, s)| b.x.max(s.x))
                    .max(100_000.0);
                ui.horizontal(|ui| {
                    ui.label("Execute USD X:");
                    ui.add(egui::Slider::new(&mut self.execute_usd, 0.0..=max).integer());
                });
                ui.label(
                    egui::RichText::new(format!(
                        "{} book, best {SWEEP_LEVELS} levels/side (totals = published L2)",
                        book.name()
                    ))
                    .small()
                    .weak(),
                );
            });
        self.open = open;
    }

    fn refresh(&mut self, book: &InferredBook, scale: Scale, batches: u64) {
        let key = Some((batches, book.name()));
        if self.key == key {
            return;
        }
        self.key = key;
        self.buy.clear();
        self.sell.clear();
        let best = |side| book.levels(side).next().map(|(px, _)| scale.px_f64(px));
        self.mid = best(Side::Bid)
            .zip(best(Side::Ask))
            .map(|(b, a)| (b + a) / 2.0);
        let Some(mid) = self.mid else {
            return;
        };
        for (side, out) in [(Side::Ask, &mut self.buy), (Side::Bid, &mut self.sell)] {
            out.extend(
                sweep_curve(levels(book, scale, side), mid)
                    .into_iter()
                    .map(|(x, y)| PlotPoint::new(x, y)),
            );
        }
    }
}

/// Compact USD tick label: 250k, 1.5M.
fn usd(v: f64) -> String {
    match v.abs() {
        a if a >= 1e6 => format!("{}M", trim(v / 1e6)),
        a if a >= 1e3 => format!("{}k", trim(v / 1e3)),
        _ => trim(v),
    }
}

fn trim(v: f64) -> String {
    let s = format!("{v:.2}");
    s.trim_end_matches('0').trim_end_matches('.').to_owned()
}

/// Best-first `(price, qty)` in real units.
fn levels(book: &InferredBook, scale: Scale, side: Side) -> impl Iterator<Item = (f64, f64)> + '_ {
    book.levels(side)
        .take(SWEEP_LEVELS)
        .map(move |(px, q)| (scale.px_f64(px), scale.qty_f64(q.total())))
}

pub struct HeatmapWindow {
    pub open: bool,
    pub contrast: f64,
    auto_scaled: bool,
    /// Columns before this time are ignored ("Reset Stats").
    cutoff: Ts,
    texture: Option<TextureHandle>,
    /// (columns, newest column time, contrast, cutoff) the texture was built from.
    key: Option<(usize, Ts, u64, Ts)>,
    /// Columns past warm-up in the current texture.
    drawn: usize,
}

impl HeatmapWindow {
    pub fn new() -> Self {
        Self {
            open: true,
            contrast: 4.0,
            auto_scaled: false,
            cutoff: 0,
            texture: None,
            key: None,
            drawn: 0,
        }
    }

    pub fn show(&mut self, ctx: &Context, area: Rect, micro: Option<&Microstructure>) {
        let (pos, w) = slot(area, 2);
        let mut open = self.open;
        egui::Window::new("Depth-Time Heatmap")
            .open(&mut open)
            .default_pos(pos)
            .default_width(w)
            .show(ctx, |ui| {
                let heat = micro.map(Microstructure::heat);
                ui.horizontal(|ui| {
                    ui.label("Contrast (Z-Range):");
                    ui.add(egui::Slider::new(&mut self.contrast, 1.0..=10.0));
                    if ui.button("Reset Stats").clicked() {
                        self.cutoff = heat.and_then(|h| h.back()).map_or(0, |c| c.t_us + 1);
                        self.auto_scaled = false;
                        self.contrast = 4.0;
                    }
                });
                ui.add_space(8.0);
                let Some(heat) = heat else {
                    warming_up(ui, "Warming up heatmap...", 0);
                    return;
                };
                // A replay restart/backward seek rebuilds the history: drop a stale cutoff.
                if heat.back().is_none_or(|c| c.t_us < self.cutoff) {
                    self.cutoff = 0;
                }
                let start = heat.partition_point(|c| c.t_us < self.cutoff);
                let fresh = heat.len() - start;
                if fresh <= WARMUP {
                    warming_up(ui, "Warming up heatmap...", fresh);
                    return;
                }
                let key = (heat.len(), heat.back().map_or(0, |c| c.t_us), self.cutoff);
                if self.key != Some((key.0, key.1, self.contrast.to_bits(), key.2))
                    || self.texture.is_none()
                {
                    self.rebuild(ui, heat.range(start..));
                    // Keyed after rebuilding: auto-contrast may have just set the contrast.
                    self.key = Some((key.0, key.1, self.contrast.to_bits(), key.2));
                }
                if let Some(texture) = &self.texture {
                    let width = ui.available_width().max(200.0);
                    ui.image((texture.id(), vec2(width, (width * 0.6).clamp(160.0, 360.0))));
                }
                if let (Some(first), Some(last)) =
                    (heat.range(start + WARMUP..).next(), heat.back())
                {
                    ui.label(
                        egui::RichText::new(format!(
                            "{} columns, last {:.0} s · asks above / bids below the mid",
                            self.drawn,
                            (last.t_us - first.t_us) as f64 / 1e6
                        ))
                        .small()
                        .weak(),
                    );
                }
            });
        self.open = open;
    }

    /// Replays upstream's online z-scoring over `cols` (oldest first): the first
    /// `WARMUP` columns only train the rolling stats (and set the auto contrast);
    /// the rest are drawn one texel per column, oldest left (stretched to the
    /// window, as upstream).
    fn rebuild<'a>(&mut self, ui: &Ui, cols: impl Iterator<Item = &'a HeatColumn>) {
        let mut stats = RollingStats::default();
        let mut drawn: Vec<(&HeatColumn, f64, f64)> = Vec::new();
        let mut levels = 0;
        let mut warm_z: Vec<f64> = Vec::new();
        for col in cols {
            stats.update(col);
            levels = levels.max(col.bids.len()).max(col.asks.len());
            let (mean, std) = (stats.mean, stats.std.max(1e-9));
            if stats.samples <= WARMUP {
                warm_z.extend(
                    col.bids
                        .iter()
                        .chain(&col.asks)
                        .filter(|&&q| q > 0)
                        .map(|&q| (q as f64 - mean) / std),
                );
            } else {
                drawn.push((col, mean, std));
            }
        }
        if !self.auto_scaled {
            // Upstream scales to the warm-up's max z; the 99th percentile keeps a
            // single outsized level from washing the map out to black.
            let z = if warm_z.is_empty() {
                1.0
            } else {
                let k = (warm_z.len() - 1) * 99 / 100;
                *warm_z.select_nth_unstable_by(k, f64::total_cmp).1
            };
            self.contrast = (z.max(1.0) + 1.0).clamp(1.0, 10.0);
            self.auto_scaled = true;
        }
        let height = (2 * levels).max(2);
        let width = drawn.len().max(1);
        let mut pixels = vec![Color32::BLACK; width * height];
        let contrast = self.contrast;
        for (x, (col, mean, std)) in drawn.iter().enumerate() {
            // Asks: best ask just above the mid row, deeper levels upwards.
            for (lvl, &q) in col.asks.iter().enumerate() {
                if q > 0 {
                    let z = (q as f64 - mean) / std;
                    pixels[(levels - 1 - lvl) * width + x] = heat_color(z, contrast, true);
                }
            }
            for (lvl, &q) in col.bids.iter().enumerate() {
                if q > 0 {
                    let z = (q as f64 - mean) / std;
                    pixels[(levels + lvl) * width + x] = heat_color(z, contrast, false);
                }
            }
        }
        self.drawn = drawn.len();
        let image = egui::ColorImage::new([width, height], pixels);
        match &mut self.texture {
            Some(t) => t.set(image, TextureOptions::NEAREST),
            None => {
                self.texture = Some(ui.ctx().load_texture(
                    "heatmap",
                    image,
                    TextureOptions::NEAREST,
                ));
            }
        }
    }
}

/// Upstream's EMA of the per-column mean/std of non-empty level sizes.
#[derive(Default)]
struct RollingStats {
    mean: f64,
    std: f64,
    samples: usize,
}

impl RollingStats {
    fn update(&mut self, col: &HeatColumn) {
        let qtys = || {
            col.bids
                .iter()
                .chain(&col.asks)
                .filter(|&&q| q > 0)
                .map(|&q| q as f64)
        };
        let n = qtys().count();
        if n == 0 {
            return;
        }
        let m = qtys().sum::<f64>() / n as f64;
        let s = (qtys().map(|q| (q - m).powi(2)).sum::<f64>() / n as f64)
            .sqrt()
            .max(1e-9);
        let alpha = if self.samples < WARMUP { 0.05 } else { 0.01 };
        if self.mean == 0.0 {
            (self.mean, self.std) = (m, s);
        } else {
            self.mean = self.mean * (1.0 - alpha) + m * alpha;
            self.std = self.std * (1.0 - alpha) + s * alpha;
        }
        self.samples += 1;
    }
}

/// Upstream's mapping: black below z = -1, then t² ramps red (asks) or blue
/// (bids) with a green boost towards yellow / cyan at the top of the range.
fn heat_color(z: f64, contrast: f64, ask: bool) -> Color32 {
    if z < -1.0 {
        return Color32::BLACK;
    }
    let t = ((z + 1.0) / contrast).clamp(0.0, 1.0);
    let t2 = t * t;
    let intensity = (t2 * 255.0) as u8;
    if ask {
        let g = ((t2 - 0.5).max(0.0) * 2.0 * 255.0) as u8;
        Color32::from_rgb(intensity, g, 0)
    } else {
        let g = ((t2 - 0.3).max(0.0) * 1.5 * 255.0).clamp(0.0, 255.0) as u8;
        Color32::from_rgb(0, g, intensity)
    }
}

fn warming_up(ui: &mut Ui, title: &str, samples: usize) {
    ui.vertical_centered(|ui| {
        ui.add_space(50.0);
        ui.heading(title);
        ui.label(format!(
            "Collecting data: {:.0}%",
            samples.min(WARMUP) as f64 / WARMUP as f64 * 100.0
        ));
        ui.add_space(50.0);
    });
}
