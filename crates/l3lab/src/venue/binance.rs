//! Binance spot depth captures written by `poc_binance --capture-dir`:
//! `snapshot.json` (REST depth), `diffs.jsonl` (`@depth@100ms` frames), `metadata.json`.
//! L2 only: there is no ground truth, so these can be viewed but not scored.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Lines};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::l2::{L2Batch, LevelUpdate};
use crate::session::FeedEvent;
use crate::types::{Scale, Side, Ts};

pub const DEFAULT_SCALE: Scale = Scale {
    px_decimals: 2,
    qty_decimals: 8,
};

#[derive(Deserialize)]
struct Snapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct Diff {
    #[serde(rename = "U")]
    first_id: u64,
    #[serde(rename = "u")]
    final_id: u64,
    #[serde(rename = "E")]
    event_ms: u64,
    #[serde(rename = "b", default)]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a", default)]
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct Metadata {
    started_at_ms: u64,
}

pub struct CaptureReader {
    scale: Scale,
    snapshot: Option<FeedEvent>,
    last_update_id: u64,
    lines: Lines<BufReader<File>>,
    line_no: usize,
}

impl CaptureReader {
    pub fn open(dir: &Path, scale: Scale) -> Result<Self> {
        let raw = fs::read_to_string(dir.join("snapshot.json"))
            .with_context(|| format!("read {}/snapshot.json", dir.display()))?;
        let snap: Snapshot = serde_json::from_str(&raw).context("parse binance snapshot")?;
        let meta: Metadata = serde_json::from_str(
            &fs::read_to_string(dir.join("metadata.json"))
                .with_context(|| format!("read {}/metadata.json", dir.display()))?,
        )?;
        let mut levels = Vec::with_capacity(snap.bids.len() + snap.asks.len());
        push_levels(&mut levels, Side::Bid, &snap.bids, scale)?;
        push_levels(&mut levels, Side::Ask, &snap.asks, scale)?;
        levels.retain(|l| l.total > 0);
        let file = File::open(dir.join("diffs.jsonl"))
            .with_context(|| format!("open {}/diffs.jsonl", dir.display()))?;
        Ok(Self {
            scale,
            snapshot: Some(FeedEvent::L2Snapshot {
                ts: meta.started_at_ms * 1000,
                levels,
            }),
            last_update_id: snap.last_update_id,
            lines: BufReader::new(file).lines(),
            line_no: 0,
        })
    }

    fn batch(&mut self, line: &str) -> Result<Option<L2Batch>> {
        let diff: Diff = serde_json::from_str(line)?;
        if diff.final_id <= self.last_update_id {
            return Ok(None);
        }
        if diff.first_id > self.last_update_id + 1 {
            eprintln!(
                "[binance] sequence gap: last={} next first={}; book may drift",
                self.last_update_id, diff.first_id
            );
        }
        self.last_update_id = diff.final_id;
        let ts: Ts = diff.event_ms * 1000;
        let mut levels = Vec::with_capacity(diff.bids.len() + diff.asks.len());
        push_levels(&mut levels, Side::Bid, &diff.bids, self.scale)?;
        push_levels(&mut levels, Side::Ask, &diff.asks, self.scale)?;
        Ok(Some(L2Batch {
            seq: 0,
            start: ts,
            end: ts,
            levels,
            trades: Vec::new(),
        }))
    }
}

fn push_levels(
    out: &mut Vec<LevelUpdate>,
    side: Side,
    rows: &[[String; 2]],
    scale: Scale,
) -> Result<()> {
    for [px, qty] in rows {
        out.push(LevelUpdate {
            side,
            px: scale.px(px)?,
            total: scale.qty(qty)?,
        });
    }
    Ok(())
}

impl Iterator for CaptureReader {
    type Item = Result<FeedEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(snap) = self.snapshot.take() {
            return Some(Ok(snap));
        }
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(e) => return Some(Err(e.into())),
            };
            self.line_no += 1;
            match self.batch(&line) {
                Ok(Some(batch)) => return Some(Ok(FeedEvent::L2(batch))),
                Ok(None) => continue,
                Err(_) if self.lines.next().is_none() => return None,
                Err(e) => {
                    return Some(Err(e.context(format!("diffs.jsonl line {}", self.line_no))));
                }
            }
        }
    }
}
