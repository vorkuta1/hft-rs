//! eframe app: owns the `Session`, drives the source, lays out panels.
//!
//! Layout after binance_l3_est: title, window toggles and the level table down
//! the left, the horizontal order book filling the centre, floating analytics
//! windows on top. Lab-specific pieces (source, model accuracy) live in a
//! toggleable bottom panel above the replay transport bar, so the book and the
//! windows keep the full width.

use std::time::Instant;

use egui::{Color32, Context, Rect, RichText, Ui};
use l3lab::score::ModelReport;
use l3lab::session::{L2Source, Session};
use l3lab::venue::Venue;

use crate::book::{BookView, Books, MAX_LEVELS, View};
use crate::fmt;
use crate::source::{LiveFeed, LiveStatus, Replay, Source};
use crate::windows::{HeatmapWindow, MetricsWindow, SweepWindow};

/// Longest frame gap credited to playback, so a stalled frame doesn't jump ahead.
const MAX_FRAME_S: f64 = 0.25;
const HIGHLIGHT: Color32 = Color32::from_rgb(255, 210, 90);
const LEFT_W: f32 = 240.0;

pub struct App {
    session: Session,
    source: Source,
    /// Index into `session.models()`.
    model: usize,
    playing: bool,
    /// Batches per second.
    speed: f64,
    /// Fractional batches owed to playback.
    owed: f64,
    last_frame: Instant,
    error: Option<String>,
    book: BookView,
    lab_panel: bool,
    metrics: MetricsWindow,
    sweep: SweepWindow,
    heatmap: HeatmapWindow,
    /// Where the order book chart was drawn last frame (anchors the windows).
    chart: Rect,
}

impl App {
    pub fn new(source: Source, session: Session, model: usize, levels: usize) -> Self {
        Self {
            session,
            source,
            model,
            playing: false,
            speed: 50.0,
            owed: 0.0,
            last_frame: Instant::now(),
            error: None,
            book: BookView::new(levels),
            lab_panel: true,
            metrics: MetricsWindow::new(),
            sweep: SweepWindow::new(),
            heatmap: HeatmapWindow::new(),
            chart: Rect::NOTHING,
        }
    }

    /// Dark theme close to the upstream screenshot.
    pub fn style(ctx: &Context) {
        let mut v = egui::Visuals::dark();
        v.panel_fill = Color32::from_gray(18);
        v.window_fill = Color32::from_gray(24);
        v.extreme_bg_color = Color32::from_gray(10);
        v.faint_bg_color = Color32::from_gray(28);
        ctx.set_visuals(v);
    }

    fn tick(&mut self) {
        let dt = self.last_frame.elapsed().as_secs_f64().min(MAX_FRAME_S);
        self.last_frame = Instant::now();
        match &mut self.source {
            Source::Live(live) => live.drain(&mut self.session),
            Source::Replay(replay) => {
                if !self.playing {
                    return;
                }
                self.owed += dt * self.speed;
                let steps = self.owed.floor();
                self.owed -= steps;
                if steps >= 1.0 {
                    let target = self.session.batches() + steps as u64;
                    if let Err(e) = replay.seek(&mut self.session, target) {
                        self.error = Some(format!("{e:#}"));
                        self.playing = false;
                    }
                }
                if replay.at_end(&self.session) {
                    self.playing = false;
                }
            }
        }
    }

    fn seek(&mut self, target: u64) {
        let Source::Replay(replay) = &mut self.source else {
            return;
        };
        if let Err(e) = replay.seek(&mut self.session, target) {
            self.error = Some(format!("{e:#}"));
            self.playing = false;
        }
    }

    fn restart(&mut self) {
        let Source::Replay(replay) = &mut self.source else {
            return;
        };
        match replay.restart() {
            Ok(session) => {
                self.session = session;
                self.error = None;
            }
            Err(e) => self.error = Some(format!("{e:#}")),
        }
    }

    /// Replay transport (play/step/restart/speed/seek) or the live status line.
    fn transport(&mut self, ui: &mut Ui) {
        let Source::Replay(replay) = &self.source else {
            if let Source::Live(live) = &self.source {
                ui.horizontal(|ui| {
                    let (text, color) = live_status(live.status);
                    ui.colored_label(color, text);
                    ui.separator();
                    ui.label(format!(
                        "{} events · {} batches",
                        live.events,
                        self.session.batches()
                    ));
                    ui.separator();
                    ui.label(exchange_time(&self.session));
                });
            }
            return;
        };
        let total = replay.total;
        let at_end = replay.at_end(&self.session);
        ui.horizontal(|ui| {
            let play = if self.playing {
                "⏸ Pause"
            } else {
                "▶ Play"
            };
            if ui
                .add_enabled(!at_end || self.playing, egui::Button::new(play))
                .clicked()
            {
                self.playing = !self.playing;
                self.owed = 0.0;
                self.last_frame = Instant::now();
            }
            if ui
                .add_enabled(!at_end, egui::Button::new("⏭ Step"))
                .clicked()
            {
                self.playing = false;
                self.seek(self.session.batches() + 1);
            }
            if ui.button("⏮ Restart").clicked() {
                self.playing = false;
                self.restart();
            }
            ui.separator();
            ui.add(
                egui::Slider::new(&mut self.speed, 1.0..=5000.0)
                    .logarithmic(true)
                    .integer()
                    .text("batches/s"),
            );
            ui.separator();
            ui.label(exchange_time(&self.session));
            ui.separator();
            ui.spacing_mut().slider_width = (ui.available_width() - 90.0).max(100.0);
            let mut pos = self.session.batches();
            if ui
                .add(egui::Slider::new(&mut pos, 0..=total).text("batch"))
                .changed()
            {
                self.seek(pos);
            }
        });
    }

    fn title(&self) -> String {
        match &self.source {
            Source::Replay(r) => format!(
                "{} {} Order Book",
                venue_name(r.venue),
                r.dir
                    .parent()
                    .and_then(|p| p.file_name())
                    .map_or(String::new(), |n| n.to_string_lossy().to_uppercase())
            ),
            Source::Live(l) => {
                let (venue, symbol) = match &l.feed {
                    LiveFeed::Bitstamp { pair } => ("Bitstamp", pair),
                    LiveFeed::HyperliquidPublic { coin } | LiveFeed::HyperliquidL4 { coin, .. } => {
                        ("Hyperliquid", coin)
                    }
                };
                format!("{venue} {} Order Book (live)", symbol.to_uppercase())
            }
        }
    }

    /// Source details and errors beside the accuracy table (bottom lab panel).
    fn lab_info(&mut self, ui: &mut Ui) {
        ui.horizontal_top(|ui| {
            ui.vertical(|ui| {
                ui.set_max_width(420.0);
                self.source_info(ui);
            });
            if self.session.truth().is_some() {
                ui.add_space(32.0);
                ui.vertical(|ui| {
                    ui.strong("Accuracy vs truth");
                    match self.session.report() {
                        Some(reports) if reports.iter().any(|r| r.level_samples > 0) => {
                            self.accuracy(ui, &reports);
                        }
                        _ => {
                            ui.label("warming up (first 30 s of exchange time are not scored)");
                        }
                    }
                });
            }
        });
    }

    fn source_info(&self, ui: &mut Ui) {
        ui.strong("Source");
        egui::Grid::new("source_info")
            .num_columns(2)
            .show(ui, |ui| {
                match &self.source {
                    Source::Replay(replay) => replay_info(ui, replay, &self.session),
                    Source::Live(live) => {
                        ui.label("venue");
                        ui.label(live.feed.describe());
                        ui.end_row();
                        ui.label("status");
                        let (text, color) = live_status(live.status);
                        ui.colored_label(color, text);
                        ui.end_row();
                        ui.label("events");
                        ui.label(live.events.to_string());
                        ui.end_row();
                        ui.label("batches");
                        ui.label(self.session.batches().to_string());
                        ui.end_row();
                    }
                }
                ui.label("models see");
                ui.label(match self.session.l2_source() {
                    L2Source::Derived {
                        conflate_us,
                        counts,
                    } => format!(
                        "derived L2, {} ms{}",
                        conflate_us / 1000,
                        if counts { " + counts" } else { "" }
                    ),
                    L2Source::Venue => "venue L2 feed".into(),
                });
                ui.end_row();
                ui.label("exchange time");
                ui.label(exchange_time(&self.session));
                ui.end_row();
            });

        if let Source::Live(live) = &self.source
            && !live.errors.is_empty()
        {
            ui.add_space(4.0);
            ui.colored_label(Color32::RED, "stream errors");
            egui::ScrollArea::vertical()
                .id_salt("live_errors")
                .max_height(120.0)
                .show(ui, |ui| {
                    for e in live.errors.iter().rev() {
                        ui.label(RichText::new(e).small().color(Color32::LIGHT_RED));
                    }
                });
        }
        if let Some(e) = &self.error {
            ui.add_space(4.0);
            ui.colored_label(Color32::RED, format!("error: {e}"));
        }
    }

    fn accuracy(&mut self, ui: &mut Ui, reports: &[ModelReport]) {
        egui::Grid::new("accuracy")
            .striped(true)
            .num_columns(7)
            .show(ui, |ui| {
                for h in [
                    "model",
                    "cnt MAE",
                    "cnt =%",
                    "size F1%",
                    "ahead err",
                    "p90",
                    "lost%",
                ] {
                    ui.label(RichText::new(h).strong());
                }
                ui.end_row();
                for (i, r) in reports.iter().enumerate() {
                    let selected = i == self.model;
                    if ui.selectable_label(selected, &r.model).clicked() {
                        self.model = i;
                    }
                    let cells = [
                        format!("{:.2}", r.count_mae),
                        format!("{:.1}", r.count_exact * 100.0),
                        format!("{:.1}", r.size_f1 * 100.0),
                        format!("{:.1}%", r.ahead_err_mean * 100.0),
                        format!("{:.1}%", r.ahead_err_p90 * 100.0),
                        format!("{:.1}", r.ahead_lost * 100.0),
                    ];
                    for cell in cells {
                        let text = RichText::new(cell).monospace();
                        ui.label(if selected {
                            text.color(HIGHLIGHT)
                        } else {
                            text
                        });
                    }
                    ui.end_row();
                }
            });
        ui.label(
            RichText::new(
                "top 10 levels/side · ahead err = |true − inferred qty ahead| / level total · \
                 lost = proxy order gone while the true order rests",
            )
            .small()
            .weak(),
        );
        let mismatches: u64 = reports.iter().map(|r| r.total_mismatches).sum();
        if mismatches > 0 {
            ui.colored_label(
                Color32::RED,
                format!("{mismatches} level-total mismatches vs truth (pipeline bug)"),
            );
        }
        if let Some(r) = reports.first() {
            ui.label(
                RichText::new(format!(
                    "{} level samples, {} queue-ahead samples",
                    r.level_samples, r.ahead_samples
                ))
                .small()
                .weak(),
            );
        }
    }
}

/// Title, window toggles, chart options, legend and the level table.
fn left_column(
    ui: &mut Ui,
    title: &str,
    toggles: [(&mut bool, &str); 4],
    book: &mut BookView,
    model: &mut usize,
    books: &Books,
    names: &[&'static str],
) {
    ui.heading(RichText::new(title).size(16.0));
    ui.add_space(2.0);
    ui.spacing_mut().button_padding = egui::vec2(4.0, 1.0);
    let kmeans = (&mut book.kmeans, "Toggle K-Means Mode");
    for (open, name) in std::iter::once(kmeans).chain(toggles) {
        if ui.button(RichText::new(name).small()).clicked() {
            *open = !*open;
        }
    }
    ui.add_space(4.0);
    egui::Grid::new("book_opts")
        .num_columns(2)
        .spacing(egui::vec2(6.0, 3.0))
        .show(ui, |ui| {
            ui.label(RichText::new("Model").small());
            egui::ComboBox::from_id_salt("model")
                .selected_text(names[*model])
                .show_ui(ui, |ui| {
                    for (i, name) in names.iter().enumerate() {
                        ui.selectable_value(model, i, *name);
                    }
                });
            ui.end_row();
            ui.label(RichText::new("Levels/side").small());
            ui.add(egui::Slider::new(&mut book.levels, 1..=MAX_LEVELS));
            ui.end_row();
            if books.truth.is_some() {
                ui.label(RichText::new("Show").small());
                ui.horizontal(|ui| {
                    for (v, name) in [
                        (View::Truth, "Truth"),
                        (View::Inferred, "Inferred"),
                        (View::Both, "Both"),
                    ] {
                        ui.selectable_value(&mut book.view, v, RichText::new(name).small());
                    }
                });
                ui.end_row();
            }
        });
    book.legend(ui, books);
    ui.add_space(4.0);
    book.table(ui, books);
}

fn live_status(status: LiveStatus) -> (&'static str, Color32) {
    match status {
        LiveStatus::Connecting => ("connecting…", Color32::YELLOW),
        LiveStatus::Streaming => ("streaming", Color32::GREEN),
        LiveStatus::Ended => ("disconnected", Color32::RED),
    }
}

fn exchange_time(session: &Session) -> String {
    match session.last_ts() {
        0 => "–".to_owned(),
        ts => fmt::utc(ts),
    }
}

fn venue_name(venue: Venue) -> &'static str {
    match venue {
        Venue::Bitstamp => "Bitstamp",
        Venue::Binance => "Binance",
        Venue::Hyperliquid => "Hyperliquid",
    }
}

fn replay_info(ui: &mut Ui, replay: &Replay, session: &Session) {
    ui.label("venue");
    ui.label(match (replay.venue, replay.has_truth) {
        (Venue::Bitstamp, _) => "Bitstamp capture (L3 truth)",
        (Venue::Binance, _) => "Binance capture (L2 only)",
        (Venue::Hyperliquid, true) => "Hyperliquid capture (L4 truth + public L2)",
        (Venue::Hyperliquid, false) => "Hyperliquid capture (public L2 only)",
    });
    ui.end_row();
    ui.label("path");
    ui.label(replay.dir.display().to_string());
    ui.end_row();
    ui.label("batch");
    ui.label(format!("{} / {}", session.batches(), replay.total));
    ui.end_row();
}

impl eframe::App for App {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        self.tick();

        egui::TopBottomPanel::bottom("transport").show(ctx, |ui| {
            ui.add_space(3.0);
            self.transport(ui);
            ui.add_space(3.0);
        });

        if self.lab_panel {
            egui::TopBottomPanel::bottom("lab")
                .resizable(false)
                .show(ctx, |ui| {
                    ui.add_space(4.0);
                    self.lab_info(ui);
                    ui.add_space(2.0);
                });
        }

        let title = self.title();
        let Self {
            session,
            source,
            model,
            book,
            lab_panel,
            metrics,
            sweep,
            heatmap,
            chart,
            ..
        } = self;
        let names: Vec<&'static str> = session.models().iter().map(|m| m.name()).collect();
        let books = Books {
            truth: session.truth(),
            model: &session.models()[*model],
            scale: source.scale(),
            now: session.last_ts(),
        };
        book.prepare(&books);

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal_top(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(LEFT_W, ui.available_height()),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        ui.set_width(LEFT_W);
                        left_column(
                            ui,
                            &title,
                            [
                                (&mut sweep.open, "Toggle Sweep / Liquidity-Cost"),
                                (&mut heatmap.open, "Toggle Depth-Time Heatmap"),
                                (&mut metrics.open, "Toggle Microstructure Metrics"),
                                (lab_panel, "Toggle Lab Panel"),
                            ],
                            book,
                            model,
                            &books,
                            &names,
                        );
                    },
                );
                *chart = ui.available_rect_before_wrap();
                book.chart(ui, &books);
            });
        });

        let micro = session.micro();
        metrics.show(ctx, *chart, micro);
        sweep.show(ctx, *chart, books.model, books.scale, session.batches());
        heatmap.show(ctx, *chart, micro);
        let live = matches!(self.source, Source::Live(_));
        if self.playing || live {
            ctx.request_repaint();
        }
    }
}
