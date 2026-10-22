//! eframe app: owns the `Session`, drives the source, lays out panels.

use std::time::Instant;

use egui::{Color32, Context, RichText, Ui};
use l3lab::score::ModelReport;
use l3lab::session::Session;
use l3lab::venue::Venue;

use crate::fmt;
use crate::ladder::Ladder;
use crate::source::{LiveStatus, Replay, Source};

/// Longest frame gap credited to playback, so a stalled frame doesn't jump ahead.
const MAX_FRAME_S: f64 = 0.25;
const HIGHLIGHT: Color32 = Color32::from_rgb(255, 210, 90);

pub struct App {
    session: Session,
    source: Source,
    /// Index into `session.models()`.
    model: usize,
    depth: usize,
    playing: bool,
    /// Batches per second.
    speed: f64,
    /// Fractional batches owed to playback.
    owed: f64,
    last_frame: Instant,
    error: Option<String>,
}

impl App {
    pub fn new(source: Source, session: Session, model: usize, depth: usize) -> Self {
        Self {
            session,
            source,
            model,
            depth,
            playing: false,
            speed: 50.0,
            owed: 0.0,
            last_frame: Instant::now(),
            error: None,
        }
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

    fn controls(&mut self, ui: &mut Ui) {
        let Source::Replay(replay) = &self.source else {
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
            ui.add(
                egui::DragValue::new(&mut self.depth)
                    .range(1..=200)
                    .prefix("depth: "),
            );
        });
        ui.horizontal(|ui| {
            ui.spacing_mut().slider_width = (ui.available_width() - 120.0).max(100.0);
            let mut pos = self.session.batches();
            if ui
                .add(egui::Slider::new(&mut pos, 0..=total).text("batch"))
                .changed()
            {
                self.seek(pos);
            }
        });
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

    fn side_panel(&mut self, ui: &mut Ui) {
        ui.heading("Source");
        egui::Grid::new("source_info")
            .num_columns(2)
            .show(ui, |ui| {
                match &self.source {
                    Source::Replay(replay) => replay_info(ui, replay, &self.session),
                    Source::Live(live) => {
                        ui.label("venue");
                        ui.label(format!("Bitstamp live ({})", live.pair));
                        ui.end_row();
                        ui.label("status");
                        let (text, color) = match live.status {
                            LiveStatus::Connecting => ("connecting…", Color32::YELLOW),
                            LiveStatus::Streaming => ("streaming", Color32::GREEN),
                            LiveStatus::Ended => ("disconnected", Color32::RED),
                        };
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
                ui.label("conflation");
                ui.label(format!("{} ms", self.session.conflate_us() / 1000));
                ui.end_row();
                ui.label("exchange time");
                let ts = self.session.last_ts();
                ui.label(if ts == 0 {
                    "–".to_owned()
                } else {
                    fmt::utc(ts)
                });
                ui.end_row();
            });

        if let Source::Live(live) = &self.source
            && !live.errors.is_empty()
        {
            ui.separator();
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
            ui.separator();
            ui.colored_label(Color32::RED, format!("error: {e}"));
        }

        ui.separator();
        ui.heading("Model");
        let names: Vec<&'static str> = self.session.models().iter().map(|m| m.name()).collect();
        egui::ComboBox::from_id_salt("model")
            .selected_text(names[self.model])
            .show_ui(ui, |ui| {
                for (i, name) in names.iter().enumerate() {
                    ui.selectable_value(&mut self.model, i, *name);
                }
            });

        if self.session.truth().is_some() {
            ui.separator();
            ui.heading("Accuracy vs truth");
            match self.session.report() {
                Some(reports) if reports.iter().any(|r| r.level_samples > 0) => {
                    self.metrics(ui, &reports);
                }
                _ => {
                    ui.label("warming up (first 30 s of exchange time are not scored)");
                }
            }
        }
    }

    fn metrics(&mut self, ui: &mut Ui, reports: &[ModelReport]) {
        egui::Grid::new("metrics")
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

fn replay_info(ui: &mut Ui, replay: &Replay, session: &Session) {
    ui.label("venue");
    ui.label(match replay.venue {
        Venue::Bitstamp => "Bitstamp capture (L3 truth)",
        Venue::Binance => "Binance capture (L2 only)",
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

        egui::TopBottomPanel::top("controls").show(ctx, |ui| {
            ui.add_space(4.0);
            match &self.source {
                Source::Replay(_) => self.controls(ui),
                Source::Live(_) => {
                    ui.horizontal(|ui| {
                        ui.label("live");
                        ui.separator();
                        ui.add(
                            egui::DragValue::new(&mut self.depth)
                                .range(1..=200)
                                .prefix("depth: "),
                        );
                    });
                }
            }
            ui.add_space(4.0);
        });

        egui::SidePanel::right("info")
            .min_width(360.0)
            .show(ctx, |ui| self.side_panel(ui));

        egui::CentralPanel::default().show(ctx, |ui| {
            let ladder = Ladder {
                truth: self.session.truth(),
                model: &self.session.models()[self.model],
                scale: self.source.scale(),
                now: self.session.last_ts(),
                depth: self.depth,
            };
            ladder.show(ui);
        });

        let live = matches!(self.source, Source::Live(_));
        if self.playing || live {
            ctx.request_repaint();
        }
    }
}
