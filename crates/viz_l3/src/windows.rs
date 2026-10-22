//! Floating analytics windows after binance_l3_est: microstructure ratios, sweep
//! cost curves and the depth-time heatmap.

use egui::{Color32, Context, Id, Pos2, Rect, TextureHandle, TextureOptions, Ui, vec2};
use egui_plot::{
    AxisHints, Corner, HLine, Legend, Line, LineStyle, Plot, PlotPoint, PlotPoints,
    uniform_grid_spacer,
};
use l3lab::micro::{HeatColumn, Microstructure, Ratio, RatioPoint, sweep_curve, sweep_impact};
use l3lab::model::InferredBook;
use l3lab::types::{Scale, Side, Ts};

/// Samples before the heatmap renders (as upstream).
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
    /// Visible time span of the ratio plots, seconds.
    window_s: f64,
    /// Per ratio: (sample count, last t, window) the cached lines were built from.
    keys: [(usize, f64, f64); 4],
    /// `[ratio][bid, ask, both]`: contiguous runs of defined log10 values.
    lines: [[Vec<Vec<PlotPoint>>; 3]; 4],
    /// Exchange time (s) of the newest sample.
    now: f64,
}

const RATIOS: [(Ratio, &str); 4] = [
    (Ratio::CtrTop1, "Top-1 Ratio"),
    (Ratio::CtrTop20, "Top-20 Ratio"),
    (Ratio::OtrTop1, "Top-1 OTR"),
    (Ratio::OtrTop20, "Top-20 OTR"),
];

/// Selectable visible spans (seconds, label).
const WINDOWS: [(f64, &str); 3] = [(30.0, "30 s"), (120.0, "2 min"), (300.0, "5 min")];
/// Ratios are plotted as log10; zero (e.g. no cancels) sits at this floor.
const LOG_FLOOR: f64 = -2.0;

impl MetricsWindow {
    pub fn new() -> Self {
        Self {
            open: true,
            window_s: 120.0,
            keys: [(usize::MAX, 0.0, 0.0); 4],
            lines: Default::default(),
            now: 0.0,
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
                ui.horizontal(|ui| {
                    for (span, label) in WINDOWS {
                        ui.selectable_value(&mut self.window_s, span, label);
                    }
                    if let Some(m) = micro {
                        let half_life = m.config().half_life_us as f64 / 1e6;
                        ui.weak(format!("rolling, {half_life:.0} s half-life · log scale"));
                    }
                });
                ui.label("Cancellation-to-Trade Ratio (CTR)");
                ui.label("CTR > 1.0: Spoofing/Layering > Fills");
                let defined = micro
                    .is_some_and(|m| m.series(Ratio::CtrTop20).iter().any(|p| p.both.is_some()));
                let Some(micro) = micro.filter(|_| defined) else {
                    ui.vertical_centered(|ui| {
                        ui.add_space(50.0);
                        ui.heading("Waiting for fills...");
                        ui.label("A ratio needs a few recent fills before it means anything.");
                        ui.add_space(50.0);
                    });
                    return;
                };
                self.refresh(micro);
                let plot_h = ((area.height() - 190.0) / 4.0).clamp(85.0, 150.0);
                let x_range = (self.now - self.window_s)..=self.now;
                for (i, (_, label)) in RATIOS.iter().enumerate() {
                    if i == 2 {
                        ui.add_space(8.0);
                        ui.separator();
                        ui.label("Order-to-Trade Ratio (OTR)");
                        ui.label("OTR > 1.0: Liquidity Adding > Taking (Reloading)");
                    } else if i > 0 {
                        ui.add_space(4.0);
                    }
                    let [bid, ask, both] = &self.lines[i];
                    Plot::new(("ratio_plot", i))
                        .height(plot_h)
                        .link_cursor(Id::new("micro_ratio_x"), [true, false])
                        .allow_drag(false)
                        .allow_zoom(false)
                        .allow_scroll(false)
                        .show_axes([true, true])
                        .y_axis_label(*label)
                        .y_grid_spacer(uniform_grid_spacer(|_| [1.0, 10.0, 100.0]))
                        .y_axis_formatter(|mark, _| ratio_label(mark.value))
                        .label_formatter(|name, p| {
                            format!("{name}\nt = {:.1} s\nratio = {}", p.x, ratio_label(p.y))
                        })
                        .legend(Legend::default().position(Corner::LeftTop))
                        .show(ui, |plot| {
                            plot.set_plot_bounds_x(x_range.clone());
                            plot.set_auto_bounds([false, true]);
                            plot.hline(
                                HLine::new("", 0.0)
                                    .color(Color32::from_gray(90))
                                    .style(LineStyle::dashed_dense()),
                            );
                            for (runs, name, color) in [
                                (ask, "Ask", Color32::RED),
                                (bid, "Bid", Color32::GREEN),
                                (both, "Both", Color32::WHITE),
                            ] {
                                for run in runs {
                                    plot.line(
                                        Line::new(name, PlotPoints::Borrowed(run)).color(color),
                                    );
                                }
                            }
                        });
                }
            });
        self.open = open;
    }

    /// Rebuilds the visible line runs when new samples arrive or the span changes.
    fn refresh(&mut self, micro: &Microstructure) {
        for (i, (ratio, _)) in RATIOS.iter().enumerate() {
            let series = micro.series(*ratio);
            let last = series.back().map_or(0.0, |p| p.t);
            let key = (series.len(), last, self.window_s);
            self.now = last;
            if self.keys[i] == key {
                continue;
            }
            self.keys[i] = key;
            let from = last - self.window_s;
            let fields: [fn(&RatioPoint) -> Option<f64>; 3] = [|p| p.bid, |p| p.ask, |p| p.both];
            for (runs, field) in self.lines[i].iter_mut().zip(fields) {
                runs.clear();
                let mut run: Vec<PlotPoint> = Vec::new();
                for p in series.iter().filter(|p| p.t >= from) {
                    match field(p) {
                        Some(v) => run.push(PlotPoint::new(p.t, v.log10().max(LOG_FLOOR))),
                        None if run.is_empty() => {}
                        None => runs.push(std::mem::take(&mut run)),
                    }
                }
                if !run.is_empty() {
                    runs.push(run);
                }
            }
        }
    }
}

/// Formats a log10 value as the ratio it stands for (0.01, 1, 30, 2.5k …).
fn ratio_label(log: f64) -> String {
    if log <= LOG_FLOOR {
        return "0".into();
    }
    let v = 10f64.powf(log);
    let (scaled, unit, decimals) = match v {
        v if v >= 1e6 => (v / 1e6, "M", 1),
        v if v >= 1e3 => (v / 1e3, "k", 1),
        v if v >= 10.0 => (v, "", 0),
        v if v >= 1.0 => (v, "", 1),
        v => (v, "", 2),
    };
    let mut s = format!("{scaled:.decimals$}");
    if s.contains('.') {
        s = s.trim_end_matches('0').trim_end_matches('.').to_owned();
    }
    s + unit
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

#[cfg(test)]
mod tests {
    use super::ratio_label;

    #[test]
    fn ratio_labels_read_as_plain_numbers() {
        let label = |v: f64| ratio_label(v.log10());
        assert_eq!(label(30.0), "30");
        assert_eq!(label(1.0), "1");
        assert_eq!(label(2500.0), "2.5k");
        assert_eq!(label(0.05), "0.05");
        assert_eq!(ratio_label(-2.0), "0");
    }
}
