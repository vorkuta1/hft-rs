use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use serde::Serialize;

use l3lab::model::{MODEL_NAMES, QueueModel, all_models, model_by_name};
use l3lab::score::{ModelReport, OwnerReport, ScoreConfig};
use l3lab::session::{L2Source, Session};
use l3lab::types::Scale;
use l3lab::venue::{Capture, bitstamp};

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
        /// Conflation windows to evaluate for L2 derived from truth (0 = every L3
        /// event visible).
        #[arg(long, value_delimiter = ',', default_value = "0,100")]
        conflate_ms: Vec<u64>,
        /// Derived L2 also publishes per-level order counts (Hyperliquid-style).
        #[arg(long)]
        counts: bool,
        /// Drive models with the capture's own recorded L2 feed instead of deriving
        /// L2 from truth (ignores --conflate-ms / --counts).
        #[arg(long)]
        venue_l2: bool,
        /// Levels per side to sample.
        #[arg(long, default_value_t = 10)]
        top: usize,
        /// Seconds to skip while snapshot-seeded levels turn over.
        #[arg(long, default_value_t = 30)]
        warmup_secs: u64,
        /// Comma-separated subset of models (default: all).
        #[arg(long, value_delimiter = ',')]
        models: Vec<String>,
        /// Also print the N busiest order owners per model (L4 captures).
        #[arg(long, default_value_t = 0)]
        by_user: usize,
        /// Also write the results as JSON.
        #[arg(long)]
        json: Option<PathBuf>,
    },
}

#[derive(Serialize)]
struct Run {
    l2: String,
    batches: u64,
    elapsed_ms: u128,
    models: Vec<ModelReport>,
    owners: Vec<(String, Vec<OwnerReport>)>,
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
            counts,
            venue_l2,
            top,
            warmup_secs,
            models,
            by_user,
            json,
        } => {
            let cfg = ScoreConfig {
                top_k: top,
                warmup_us: warmup_secs * 1_000_000,
            };
            let sources: Vec<L2Source> = if venue_l2 {
                vec![L2Source::Venue]
            } else {
                conflate_ms
                    .iter()
                    .map(|ms| L2Source::Derived {
                        conflate_us: ms * 1000,
                        counts,
                    })
                    .collect()
            };
            let mut runs = Vec::new();
            for source in sources {
                let run = score(&capture, source, cfg, &models, by_user)?;
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

fn describe(source: L2Source) -> String {
    match source {
        L2Source::Derived {
            conflate_us,
            counts,
        } => format!(
            "derived, conflation {} ms{}",
            conflate_us / 1000,
            if counts { ", with order counts" } else { "" }
        ),
        L2Source::Venue => "venue L2 feed".into(),
    }
}

fn score(
    capture: &Path,
    source: L2Source,
    cfg: ScoreConfig,
    models: &[String],
    by_user: usize,
) -> Result<Run> {
    let cap = Capture::open(capture, None)?;
    if !cap.has_truth() {
        bail!(
            "{} has no L3/L4 ground truth; it can be viewed but not scored",
            capture.display()
        );
    }
    let mut session = Session::new(pick_models(models)?, source).with_scorer(cfg);
    let t0 = Instant::now();
    for ev in cap.events {
        session.ingest(ev?)?;
    }
    session.finish();
    let label = describe(source);
    if let Some(truth) = session.truth() {
        let stats = truth.stats();
        eprintln!(
            "[score] {label}: batches={} unknown_refs={} duplicate_adds={} missing_anchors={} pending_at_end={}",
            session.batches(),
            stats.unknown_refs,
            stats.duplicate_adds,
            stats.missing_anchors,
            truth.pending_len()
        );
    }
    let reports = session.report().unwrap_or_default();
    let owners = if by_user == 0 {
        Vec::new()
    } else {
        reports
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let rows = session.owner_report(i, by_user).unwrap_or_default();
                (r.model.clone(), rows)
            })
            .filter(|(_, rows)| !rows.is_empty())
            .collect()
    };
    Ok(Run {
        l2: label,
        batches: session.batches(),
        elapsed_ms: t0.elapsed().as_millis(),
        models: reports,
        owners,
    })
}

fn print_run(run: &Run) {
    println!(
        "\n### {} — {} batches ({} ms)\n",
        run.l2, run.batches, run.elapsed_ms
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
    }
    if let Some(m) = run.models.first() {
        println!(
            "\n{} level samples, {} queue-ahead samples per model",
            m.level_samples, m.ahead_samples
        );
        if m.total_mismatches > 0 {
            let pct = m.total_mismatches as f64 / m.level_samples.max(1) as f64 * 100.0;
            eprintln!(
                "[score] {pct:.2}% of level samples had inferred totals that disagree with truth{}",
                if run.l2.starts_with("derived") {
                    " (BUG: derived L2 must match exactly)"
                } else {
                    " (venue feed vs truth misalignment)"
                }
            );
        }
    }
    for (model, rows) in &run.owners {
        println!("\n#### {model}: busiest owners\n");
        println!("| owner | orders sampled | size recall | ahead samples | ahead err mean | ahead lost |");
        println!("|---|---:|---:|---:|---:|---:|");
        for o in rows {
            println!(
                "| {} | {} | {:.1}% | {} | {:.3} | {:.1}% |",
                o.owner,
                o.order_samples,
                o.size_recall * 100.0,
                o.ahead_samples,
                o.ahead_err_mean,
                o.ahead_lost * 100.0
            );
        }
    }
}
