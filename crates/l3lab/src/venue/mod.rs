pub mod binance;
pub mod bitstamp;

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
}

/// A capture directory opened for replay.
pub struct Capture {
    pub venue: Venue,
    pub scale: Scale,
    pub events: Box<dyn Iterator<Item = Result<FeedEvent>> + Send>,
}

impl Capture {
    /// Whether the capture carries order-level ground truth (and can be scored).
    pub fn has_truth(&self) -> bool {
        match self.venue {
            Venue::Bitstamp => true,
            Venue::Binance => false,
        }
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
            })
        } else if dir.join("diffs.jsonl").exists() {
            let scale = scale.unwrap_or(binance::DEFAULT_SCALE);
            Ok(Self {
                venue: Venue::Binance,
                scale,
                events: Box::new(binance::CaptureReader::open(dir, scale)?),
            })
        } else {
            bail!(
                "{} is not a capture (expected orders.jsonl or diffs.jsonl)",
                dir.display()
            )
        }
    }
}
