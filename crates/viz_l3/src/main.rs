mod playback;
mod ui;

use anyhow::{Result, anyhow};
use clap::Parser;
use playback::{EventLog, PlaybackBook};
use std::path::PathBuf;
use ui::VizApp;

#[derive(Parser, Debug)]
#[command(author, version, about = "L3 viewer for captured inference events", long_about = None)]
struct Args {
    /// Path to JSONL or bincode inferred events file
    #[arg(short, long)]
    input: PathBuf,
    /// Maximum number of events to preload (0 = all)
    #[arg(long, default_value_t = 0)]
    limit: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let log = EventLog::load(&args.input, args.limit)?;
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1400.0, 900.0])
            .with_min_inner_size([800.0, 600.0]),
        ..Default::default()
    };
    let runner_log = log.clone();
    let result = eframe::run_native(
        "L3 Inference Viewer - Order Book Visualization",
        native_options,
        Box::new(move |cc| {
            let book = PlaybackBook::new(runner_log.clone());
            Ok(Box::new(VizApp::new(cc, book)))
        }),
    );
    if let Err(err) = result {
        return Err(anyhow!("eframe error: {err}"));
    }
    Ok(())
}
