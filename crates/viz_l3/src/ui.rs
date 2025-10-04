use crate::playback::{BookSnapshot, EventKind, InferredEvent, LevelRow, PlaybackBook};
use eframe::egui::{self, Align2, Color32, Context, ViewportCommand};
use egui_extras::{Column, TableBuilder};
use egui_plot::{Bar, BarChart, Plot, PlotPoint, Text};
use once_cell::sync::Lazy;
use std::time::{Duration, Instant};

const DEFAULT_DEPTH: usize = 100;

static BID_COLORS: Lazy<Vec<Color32>> = Lazy::new(|| {
    vec![
        Color32::from_rgb(222, 235, 247), // Light Blue
        Color32::from_rgb(204, 227, 245), // Lighter Blue
        Color32::from_rgb(158, 202, 225), // Blue
        Color32::from_rgb(129, 189, 231), // Light Medium Blue
        Color32::from_rgb(107, 174, 214), // Medium Blue
        Color32::from_rgb(78, 157, 202),  // Medium Deep Blue
        Color32::from_rgb(49, 130, 189),  // Deep Blue
        Color32::from_rgb(33, 113, 181),  // Darker Deep Blue
        Color32::from_rgb(16, 96, 168),   // Dark Blue
        Color32::from_rgb(8, 81, 156),    // Darkest Blue
    ]
});

static ASK_COLORS: Lazy<Vec<Color32>> = Lazy::new(|| {
    vec![
        Color32::from_rgb(254, 230, 206), // Light Orange
        Color32::from_rgb(253, 216, 186), // Lighter Orange
        Color32::from_rgb(253, 174, 107), // Orange
        Color32::from_rgb(253, 159, 88),  // Light Deep Orange
        Color32::from_rgb(253, 141, 60),  // Deep Orange
        Color32::from_rgb(245, 126, 47),  // Medium Red-Orange
        Color32::from_rgb(230, 85, 13),   // Red-Orange
        Color32::from_rgb(204, 75, 12),   // Darker Red-Orange
        Color32::from_rgb(179, 65, 10),   // Dark Red
        Color32::from_rgb(166, 54, 3),    // Darkest Red
    ]
});

pub struct VizApp {
    book: PlaybackBook,
    cursor: usize,
    depth: usize,
    auto_play: bool,
    playback_speed: f32,
    last_tick: Instant,
    price_scale: f64,
    show_plot: bool,
}

impl VizApp {
    pub fn new(_cc: &eframe::CreationContext<'_>, mut book: PlaybackBook) -> Self {
        let initial_depth = DEFAULT_DEPTH;
        book.seek(0);
        Self {
            book,
            cursor: 0,
            depth: initial_depth,
            auto_play: false,
            playback_speed: 1.0,
            last_tick: Instant::now(),
            price_scale: 1.0,
            show_plot: true,
        }
    }

    fn current_event(&self) -> Option<&InferredEvent> {
        self.book.event(self.cursor)
    }

    fn advance(&mut self) {
        if self.cursor + 1 < self.book.len() {
            self.cursor += 1;
            self.book.seek(self.cursor);
        } else {
            self.auto_play = false;
        }
    }

    fn rewind(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            self.book.seek(self.cursor);
        }
    }

    fn seek(&mut self, target: usize) {
        if target != self.cursor {
            self.cursor = target.min(self.book.len().saturating_sub(1));
            self.book.seek(self.cursor);
        }
    }

    fn top_levels(&self) -> BookSnapshot {
        self.book.snapshot(self.depth)
    }

    fn format_price(&self, ticks: u64) -> String {
        if self.price_scale <= f64::EPSILON {
            format!("{ticks}")
        } else {
            let price = ticks as f64 / self.price_scale;
            format!("{price:.6}")
        }
    }

    fn control_panel(&mut self, ctx: &Context, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            if ui
                .button(if self.auto_play { "⏸ Pause" } else { "▶ Play" })
                .clicked()
            {
                self.auto_play = !self.auto_play;
                self.last_tick = Instant::now();
            }
            if ui.button("⏮ Prev").clicked() {
                self.auto_play = false;
                self.rewind();
            }
            if ui.button("⏭ Next").clicked() {
                self.auto_play = false;
                self.advance();
            }
            if ui.button(if self.show_plot { "📊 Hide Plot" } else { "📊 Show Plot" }).clicked() {
                self.show_plot = !self.show_plot;
            }
            ui.add(egui::Slider::new(&mut self.playback_speed, 0.1..=10.0).text("speed"));
            let total = self.book.len();
            if total == 0 {
                ui.label("event 0/0");
            } else {
                ui.label(format!("event {}/{}", self.cursor + 1, total));
            }
        });
        ui.separator();
        ui.horizontal(|ui| {
            ui.add(
                egui::DragValue::new(&mut self.depth)
                    .range(1..=200)
                    .prefix("depth: "),
            );
            ui.add(
                egui::DragValue::new(&mut self.price_scale)
                    .range(1.0..=1_000_000.0)
                    .speed(1.0)
                    .prefix("price scale: "),
            );
            if ui.button("Reset scale").clicked() {
                self.price_scale = 1.0;
            }
        });
        ui.separator();
        if self.book.len() > 0 {
            let mut slider_value = self.cursor as f64;
            let max = self.book.len().saturating_sub(1) as f64;
            let slider = egui::Slider::new(&mut slider_value, 0.0..=max)
                .text("cursor")
                .integer();
            if ui.add(slider).changed() {
                self.seek(slider_value as usize);
            }
        } else {
            ui.label("no events loaded");
        }

        if self.auto_play {
            let elapsed = self.last_tick.elapsed();
            let target = Duration::from_millis((16.0 / self.playback_speed.max(0.1)) as u64);
            if elapsed >= target {
                self.advance();
                self.last_tick = Instant::now();
            }
            ctx.request_repaint_after(Duration::from_millis(16));
        }
    }

    fn render_event_details(&self, ui: &mut egui::Ui) {
        if let Some(event) = self.current_event() {
            ui.group(|ui| {
                ui.label(format!("sequence: {}", event.sequence));
                if let Some(ts) = event.timestamp_ms {
                    ui.label(format!("timestamp: {ts} ms"));
                }
                ui.label(format!("side: {:?}", event.side));
                ui.label(format!("price ticks: {}", event.price_ticks));
                if let Some(order_id) = event.order_id {
                    ui.label(format!("order id: {order_id}"));
                }
                ui.label(format!("event: {}", describe_kind(&event.kind)));
            });
        }
    }

    fn render_plot(&self, ui: &mut egui::Ui) {
        let snapshot = self.top_levels();
        let step = 1.0;
        let mut bars: Vec<Bar> = Vec::new();
        
        // Calculate max quantity for scaling
        let mut max_qty: f64 = 0.0;
        for level in &snapshot.bids {
            max_qty = max_qty.max(level.total_qty);
        }
        for level in &snapshot.asks {
            max_qty = max_qty.max(level.total_qty);
        }

        if max_qty <= 0.0 {
            ui.label("No order book data to display");
            return;
        }

        // Render asks (right side, positive x)
        for (i, level) in snapshot.asks.iter().enumerate() {
            let x = (i as f64 + 0.5) * step + 0.5;
            let queue_depth = level.queue_depth.min(10);
            let color = if queue_depth == 0 {
                ASK_COLORS[0]
            } else {
                ASK_COLORS
                    .get(queue_depth.saturating_sub(1))
                    .cloned()
                    .unwrap_or(Color32::DARK_RED)
            };
            
            let bar = Bar::new(x, level.total_qty)
                .fill(color)
                .width(step * 0.9);
            bars.push(bar);
        }

        // Render bids (left side, negative x)
        for (i, level) in snapshot.bids.iter().enumerate() {
            let x = -(i as f64 + 0.5) * step - 0.5;
            let queue_depth = level.queue_depth.min(10);
            let color = if queue_depth == 0 {
                BID_COLORS[0]
            } else {
                BID_COLORS
                    .get(queue_depth.saturating_sub(1))
                    .cloned()
                    .unwrap_or(Color32::DARK_BLUE)
            };
            
            let bar = Bar::new(x, level.total_qty)
                .fill(color)
                .width(step * 0.9);
            bars.push(bar);
        }

        let plot = Plot::new("orderbook_chart")
            .allow_drag(false)
            .allow_scroll(false)
            .allow_zoom(false)
            .allow_boxed_zoom(false)
            .show_axes([true, true])
            .show_x(true)
            .show_y(true)
            .legend(egui_plot::Legend::default())
            .height(ui.available_height());

        plot.show(ui, |plot_ui| {
            plot_ui.bar_chart(BarChart::new("orderbook", bars));

            // Add price labels for bids (every 20 levels)
            for (i, level) in snapshot.bids.iter().enumerate() {
                if i % 20 == 0 {
                    let x = -(i as f64 + 0.5) * step - 0.5;
                    let price_str = self.format_price(level.price_ticks);
                    plot_ui.text(
                        Text::new(
                            format!("bid_{}", i),
                            PlotPoint::new(x, -max_qty * 0.05),
                            price_str,
                        )
                        .anchor(Align2::CENTER_BOTTOM)
                        .color(Color32::LIGHT_BLUE),
                    );
                }
            }

            // Add price labels for asks (every 20 levels)
            for (i, level) in snapshot.asks.iter().enumerate() {
                if i % 20 == 0 && i > 0 {
                    let x = (i as f64 + 0.5) * step + 0.5;
                    let price_str = self.format_price(level.price_ticks);
                    plot_ui.text(
                        Text::new(
                            format!("ask_{}", i),
                            PlotPoint::new(x, -max_qty * 0.05),
                            price_str,
                        )
                        .anchor(Align2::CENTER_BOTTOM)
                        .color(Color32::LIGHT_RED),
                    );
                }
            }
        });
    }

    fn render_book_tables(&self, ui: &mut egui::Ui) {
        let snapshot = self.top_levels();
        ui.columns(2, |columns| {
            render_side_table(
                &mut columns[0],
                "Bids",
                Color32::from_rgb(20, 130, 40),
                &snapshot.bids,
                self,
            );
            render_side_table(
                &mut columns[1],
                "Asks",
                Color32::from_rgb(180, 50, 40),
                &snapshot.asks,
                self,
            );
        });
    }
}

impl eframe::App for VizApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        let mut should_close = false;
        egui::TopBottomPanel::top("controls").show(ctx, |ui| {
            ui.heading("L3 Inference Viewer");
            self.control_panel(ctx, ui);
            if ui.button("❌ Quit").clicked() {
                should_close = true;
            }
        });

        egui::SidePanel::right("event_info")
            .min_width(220.0)
            .show(ctx, |ui| {
                ui.heading("Event Details");
                self.render_event_details(ui);
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            if self.show_plot {
                ui.heading("Order Book Depth Chart");
                ui.separator();
                // Give the plot all available space
                self.render_plot(ui);
            } else {
                ui.heading("Order Book Table");
                self.render_book_tables(ui);
            }
        });

        if should_close {
            ctx.send_viewport_cmd(ViewportCommand::Close);
        }
    }
}

fn render_side_table(
    ui: &mut egui::Ui,
    title: &str,
    color: Color32,
    rows: &[LevelRow],
    app: &VizApp,
) {
    ui.heading(title);
    let table = TableBuilder::new(ui)
        .striped(true)
        .column(Column::auto())
        .column(Column::auto())
        .column(Column::auto());

    table
        .header(20.0, |mut header| {
            header.col(|ui| {
                ui.colored_label(color, "price");
            });
            header.col(|ui| {
                ui.colored_label(color, "qty");
            });
            header.col(|ui| {
                ui.colored_label(color, "queue");
            });
        })
        .body(|mut body| {
            for level in rows {
                body.row(18.0, |mut row| {
                    row.col(|ui| {
                        ui.label(app.format_price(level.price_ticks));
                    });
                    row.col(|ui| {
                        ui.label(format!("{:.6}", level.total_qty));
                    });
                    row.col(|ui| {
                        ui.label(format!("{}", level.queue_depth));
                    });
                });
            }
        });
}

fn describe_kind(kind: &EventKind) -> String {
    match kind {
        EventKind::Snapshot { qty } => format!("snapshot qty={qty:.6}"),
        EventKind::New { qty } => format!("new qty={qty:.6}"),
        EventKind::Fill { qty, remaining } => {
            format!("fill qty={qty:.6} remaining={remaining:.6}")
        }
        EventKind::Underflow { missing_qty } => {
            format!("underflow missing={missing_qty:.6}")
        }
    }
}
