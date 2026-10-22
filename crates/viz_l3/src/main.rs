mod app;
mod book;
mod fmt;
mod kmeans;
mod source;
mod windows;

use std::path::PathBuf;

use anyhow::{Result, anyhow, bail};
use clap::{ArgGroup, Parser, ValueEnum};
use l3lab::learn::Policy;
use l3lab::session::L2Source;

use app::App;
use source::{Live, LiveFeed, Replay, Setup, Source};

#[derive(Parser, Debug)]
#[command(
    about = "L2→L3 order-queue inference viewer: ground-truth vs inferred queues",
    group(ArgGroup::new("source").required(true).args(["capture", "live"]))
)]
struct Args {
    /// Capture directory to replay (Bitstamp / Hyperliquid with L4: truth + inferred;
    /// Binance / Hyperliquid public-only: inferred only).
    #[arg(long)]
    capture: Option<PathBuf>,
    /// Stream a live venue.
    #[arg(long, value_enum)]
    live: Option<LiveVenue>,
    /// Bitstamp pair or Hyperliquid coin for live mode.
    #[arg(long, requires = "live")]
    symbol: Option<String>,
    /// Hyperliquid `order_book_server` URL for live L4 truth (e.g. ws://host:8000/ws);
    /// without it, live Hyperliquid shows the public L2 feed only.
    #[arg(long, requires = "live")]
    l4_server: Option<String>,
    /// Initially displayed model.
    #[arg(long, default_value = "hybrid")]
    model: String,
    /// L2 conflation window in milliseconds (L2 derived from truth).
    #[arg(long, default_value_t = 100)]
    conflate_ms: u64,
    /// Derived L2 also publishes per-level order counts (Hyperliquid-style).
    #[arg(long)]
    counts: bool,
    /// Drive models with the capture's own recorded L2 feed instead of deriving it.
    #[arg(long)]
    venue_l2: bool,
    /// Also run a trained model (from `l3lab train`).
    #[arg(long)]
    learned: Option<PathBuf>,
    /// Price levels shown per side (up to 200).
    #[arg(long, default_value_t = 50)]
    levels: usize,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LiveVenue {
    Bitstamp,
    Hyperliquid,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let setup = Setup {
        l2: if args.venue_l2 {
            L2Source::Venue
        } else {
            L2Source::Derived {
                conflate_us: args.conflate_ms * 1000,
                counts: args.counts,
            }
        },
        learned: args.learned.as_deref().map(Policy::load).transpose()?,
    };
    let (source, session) = match (args.capture, args.live) {
        (Some(dir), None) => {
            let (replay, session) = Replay::open(dir, setup)?;
            (Source::Replay(replay), session)
        }
        (None, Some(venue)) => {
            let feed = match (venue, args.l4_server) {
                (LiveVenue::Bitstamp, None) => LiveFeed::Bitstamp {
                    pair: args.symbol.unwrap_or_else(|| "btcusd".into()),
                },
                (LiveVenue::Bitstamp, Some(_)) => bail!("--l4-server only applies to Hyperliquid"),
                (LiveVenue::Hyperliquid, server) => {
                    let coin = args.symbol.unwrap_or_else(|| "BTC".into());
                    match server {
                        Some(server) => LiveFeed::HyperliquidL4 { coin, server },
                        None => LiveFeed::HyperliquidPublic { coin },
                    }
                }
            };
            let (live, session) = Live::start(feed, setup)?;
            (Source::Live(live), session)
        }
        _ => unreachable!("clap enforces exactly one source"),
    };
    let model = session
        .models()
        .iter()
        .position(|m| m.name() == args.model)
        .ok_or_else(|| {
            let names: Vec<_> = session.models().iter().map(|m| m.name()).collect();
            anyhow!(
                "unknown model {:?}; available: {}",
                args.model,
                names.join(", ")
            )
        })?;
    let app = App::new(source, session, model, args.levels);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1600.0, 950.0])
            .with_min_inner_size([900.0, 600.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Order Book Visualizer",
        options,
        Box::new(move |cc| {
            App::style(&cc.egui_ctx);
            Ok(Box::new(app))
        }),
    )
    .map_err(|e| anyhow!("eframe error: {e}"))
}
