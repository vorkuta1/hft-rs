use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use serde::Serialize;

use l3lab::model::{MODEL_NAMES, QueueModel, all_models, model_by_name};
use l3lab::score::{ModelReport, ScoreConfig};
use l3lab::session::Session;
use l3lab::types::Scale;
use l3lab::venue::{Capture, Venue, bitstamp};

#[derive(Parser)]
#[command(about = "L2→L3 order-queue inference lab")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Record a Bitstamp L3 ground-truth capture.
    Record {
        #[arg(long, default_value = "btcusd")]
        pair: String,
        /// Capture root; the session lands in <out>/bitstamp/<pair>/<session>/.
        #[arg(long, default_value = "data")]
        out: PathBuf,
        /// Session label (default: start time in epoch ms).
        #[arg(long)]
        session: Option<String>,
        /// Recording length in seconds.
        #[arg(long, default_value_t = 600)]
        secs: u64,
        #[arg(long, default_value_t = bitstamp::DEFAULT_SCALE.px_decimals)]
        px_decimals: u32,
        #[arg(long, default_value_t = bitstamp::DEFAULT_SCALE.qty_decimals)]
        qty_decimals: u32,
    },
    /// Score every model on a ground-truth capture.
    Score {
        capture: PathBuf,
        /// Conflation windows to evaluate (0 = every L3 event visible).
        #[arg(long, value_delimiter = ',', default_value = "0,100")]
        conflate_ms: Vec<u64>,
        /// Levels per side to sample.
        #[arg(long, default_value_t = 10)]
        top: usize,
        /// Seconds to skip while snapshot-seeded levels turn over.
        #[arg(long, default_value_t = 30)]
        warmup_secs: u64,
        /// Comma-separated subset of models (default: all).
        #[arg(long, value_delimiter = ',')]
        models: Vec<String>,
        /// Also write the results as JSON.
        #[arg(long)]
        json: Option<PathBuf>,
    },
}

#[derive(Serialize)]
struct Run {
    conflate_ms: u64,
    batches: u64,
    elapsed_ms: u128,
    models: Vec<ModelReport>,
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Record {
            pair,
            out,
            session,
            secs,
            px_decimals,
            qty_decimals,
        } => {
            let dir = bitstamp::record(&bitstamp::RecordOptions {
                pair,
                root: out,
                session,
                duration: Duration::from_secs(secs),
                scale: Scale {
                    px_decimals,
                    qty_decimals,
                },
            })?;
            println!("{}", dir.display());
            Ok(())
        }
        Cmd::Score {
            capture,
            conflate_ms,
            top,
            warmup_secs,
            models,
            json,
        } => {
            let cfg = ScoreConfig {
                top_k: top,
                warmup_us: warmup_secs * 1_000_000,
            };
            let mut runs = Vec::new();
            for ms in conflate_ms {
                let run = score(&capture, ms, cfg, &models)?;
                print_run(&run);
                runs.push(run);
            }
            if let Some(path) = json {
                std::fs::write(&path, serde_json::to_string_pretty(&runs)?)
                    .with_context(|| format!("write {}", path.display()))?;
            }
            Ok(())
        }
    }
}

fn pick_models(names: &[String]) -> Result<Vec<Box<dyn QueueModel>>> {
    if names.is_empty() {
        return Ok(all_models());
    }
    names
        .iter()
        .map(|n| {
            model_by_name(n)
                .ok_or_else(|| anyhow!("unknown model {n:?}; known: {}", MODEL_NAMES.join(", ")))
        })
        .collect()
}

fn score(capture: &Path, conflate_ms: u64, cfg: ScoreConfig, models: &[String]) -> Result<Run> {
    let cap = Capture::open(capture, None)?;
    if cap.venue != Venue::Bitstamp {
        bail!(
            "{} has no L3 ground truth; only Bitstamp captures can be scored",
            capture.display()
        );
    }
    let mut session = Session::new(pick_models(models)?, conflate_ms * 1000).with_scorer(cfg);
    let t0 = Instant::now();
    for ev in cap.events {
        session.ingest(ev?)?;
    }
    session.finish();
    if let Some(truth) = session.truth() {
        let stats = truth.stats();
        eprintln!(
            "[score] conflate={conflate_ms}ms batches={} unknown_refs={} duplicate_adds={} pending_at_end={}",
            session.batches(),
            stats.unknown_refs,
            stats.duplicate_adds,
            truth.pending_len()
        );
    }
    Ok(Run {
        conflate_ms,
        batches: session.batches(),
        elapsed_ms: t0.elapsed().as_millis(),
        models: session.report().unwrap_or_default(),
    })
}

fn print_run(run: &Run) {
    println!(
        "\n### conflation {} ms — {} batches ({} ms)\n",
        run.conflate_ms, run.batches, run.elapsed_ms
    );
    println!(
        "| model | count MAE | count exact | size P | size R | size F1 | ahead err mean | p50 | p90 | ahead lost |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for m in &run.models {
        println!(
            "| {} | {:.2} | {:.1}% | {:.1}% | {:.1}% | {:.1}% | {:.3} | {:.3} | {:.3} | {:.1}% |",
            m.model,
            m.count_mae,
            m.count_exact * 100.0,
            m.size_precision * 100.0,
            m.size_recall * 100.0,
            m.size_f1 * 100.0,
            m.ahead_err_mean,
            m.ahead_err_p50,
            m.ahead_err_p90,
            m.ahead_lost * 100.0
        );
        if m.total_mismatches > 0 {
            eprintln!(
                "[score] BUG: {} level totals disagreed with truth for {}",
                m.total_mismatches, m.model
            );
        }
    }
    if let Some(m) = run.models.first() {
        println!(
            "\n{} level samples, {} queue-ahead samples per model",
            m.level_samples, m.ahead_samples
        );
    }
}
