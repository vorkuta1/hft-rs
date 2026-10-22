pub mod binance;
pub mod bitstamp;
pub mod hyperliquid;

use std::path::Path;

use anyhow::{Result, bail};

use crate::session::FeedEvent;
use crate::types::Scale;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Venue {
    /// L3 ground truth.
    Bitstamp,
    /// L2 only.
    Binance,
    /// Public L2 (`l2Book` + `trades`), plus L4 ground truth when recorded from a node.
    Hyperliquid,
}

/// A capture directory opened for replay.
pub struct Capture {
    pub venue: Venue,
    pub scale: Scale,
    pub events: Box<dyn Iterator<Item = Result<FeedEvent>> + Send>,
    truth: bool,
}

impl Capture {
    /// Whether the capture carries order-level ground truth (and can be scored).
    pub fn has_truth(&self) -> bool {
        self.truth
    }

    /// Detects the venue from the files present. `scale` overrides the stored or
    /// default fixed-point scale.
    pub fn open(dir: &Path, scale: Option<Scale>) -> Result<Self> {
        if dir.join("orders.jsonl").exists() {
            let scale = match scale {
                Some(s) => s,
                None => bitstamp::CaptureReader::read_metadata(dir)?.scale,
            };
            Ok(Self {
                venue: Venue::Bitstamp,
                scale,
                events: Box::new(bitstamp::CaptureReader::open(dir, scale)?),
                truth: true,
            })
        } else if dir.join("diffs.jsonl").exists() {
            let scale = scale.unwrap_or(binance::DEFAULT_SCALE);
            Ok(Self {
                venue: Venue::Binance,
                scale,
                events: Box::new(binance::CaptureReader::open(dir, scale)?),
                truth: false,
            })
        } else if dir.join("public.jsonl").exists() {
            let scale = match scale {
                Some(s) => s,
                None => hyperliquid::CaptureReader::read_metadata(dir)?.scale,
            };
            Ok(Self {
                venue: Venue::Hyperliquid,
                scale,
                events: Box::new(hyperliquid::CaptureReader::open(dir, scale)?),
                truth: hyperliquid::has_l4(dir),
            })
        } else {
            bail!(
                "{} is not a capture (expected orders.jsonl, diffs.jsonl or public.jsonl)",
                dir.display()
            )
        }
    }
}
