mod app;
mod fmt;
mod ladder;
mod source;

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::builder::PossibleValuesParser;
use clap::{ArgGroup, Parser, ValueEnum};
use l3lab::model::MODEL_NAMES;

use app::App;
use source::{Live, Replay, Source};

#[derive(Parser, Debug)]
#[command(
    about = "L2→L3 order-queue inference viewer: ground-truth vs inferred queues",
    group(ArgGroup::new("source").required(true).args(["capture", "live"]))
)]
struct Args {
    /// Capture directory to replay (Bitstamp: truth + inferred; Binance: inferred only).
    #[arg(long)]
    capture: Option<PathBuf>,
    /// Stream a live venue.
    #[arg(long, value_enum)]
    live: Option<LiveVenue>,
    /// Pair for live mode.
    #[arg(long, default_value = "btcusd", requires = "live")]
    pair: String,
    /// Initially displayed model.
    #[arg(long, default_value = "hybrid", value_parser = PossibleValuesParser::new(MODEL_NAMES))]
    model: String,
    /// L2 conflation window in milliseconds.
    #[arg(long, default_value_t = 100)]
    conflate_ms: u64,
    /// Price levels shown per side.
    #[arg(long, default_value_t = 15)]
    depth: usize,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LiveVenue {
    Bitstamp,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let conflate_us = args.conflate_ms * 1000;
    let (source, session) = match (args.capture, args.live) {
        (Some(dir), None) => {
            let (replay, session) = Replay::open(dir, conflate_us)?;
            (Source::Replay(replay), session)
        }
        (None, Some(LiveVenue::Bitstamp)) => {
            let (live, session) = Live::start(args.pair, conflate_us)?;
            (Source::Live(live), session)
        }
        _ => unreachable!("clap enforces exactly one source"),
    };
    let model = session
        .models()
        .iter()
        .position(|m| m.name() == args.model)
        .ok_or_else(|| anyhow!("model {} not in session", args.model))?;
    let app = App::new(source, session, model, args.depth.max(1));

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1500.0, 950.0])
            .with_min_inner_size([900.0, 600.0]),
        ..Default::default()
    };
    eframe::run_native(
        "L3 queue inference viewer",
        options,
        Box::new(move |_cc| Ok(Box::new(app))),
    )
    .map_err(|e| anyhow!("eframe error: {e}"))
}
