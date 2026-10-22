//! Feed sources driving the UI's `Session`: a seekable capture replay or a live socket.

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;

use anyhow::{Context, Result};
use l3lab::model::{all_models, model_by_name};
use l3lab::score::ScoreConfig;
use l3lab::session::{FeedEvent, Session};
use l3lab::types::Scale;
use l3lab::venue::{Capture, Venue, bitstamp};

const SCORE: ScoreConfig = ScoreConfig {
    top_k: 10,
    warmup_us: 30_000_000,
};
const MAX_ERRORS: usize = 20;

type Events = Box<dyn Iterator<Item = Result<FeedEvent>> + Send>;

pub enum Source {
    Replay(Replay),
    Live(Live),
}

impl Source {
    pub fn scale(&self) -> Scale {
        match self {
            Source::Replay(r) => r.scale,
            Source::Live(l) => l.scale,
        }
    }
}

/// Every model, scored when the source carries ground truth.
fn new_session(conflate_us: u64, has_truth: bool) -> Session {
    let session = Session::new(all_models(), conflate_us);
    if has_truth {
        session.with_scorer(SCORE)
    } else {
        session
    }
}

pub struct Replay {
    pub dir: PathBuf,
    pub venue: Venue,
    pub scale: Scale,
    /// Batches in the whole capture.
    pub total: u64,
    conflate_us: u64,
    events: Events,
    exhausted: bool,
}

impl Replay {
    /// Counts the capture's batches, then opens it positioned at batch 0 (snapshot applied).
    pub fn open(dir: PathBuf, conflate_us: u64) -> Result<(Self, Session)> {
        let total = count_batches(&dir, conflate_us)?;
        let capture = Capture::open(&dir, None)?;
        let mut replay = Self {
            dir,
            venue: capture.venue,
            scale: capture.scale,
            total,
            conflate_us,
            events: capture.events,
            exhausted: false,
        };
        let mut session = new_session(conflate_us, replay.venue == Venue::Bitstamp);
        replay.prime(&mut session)?;
        Ok((replay, session))
    }

    /// Reopens the capture from the start.
    pub fn restart(&mut self) -> Result<Session> {
        let capture = Capture::open(&self.dir, Some(self.scale))?;
        self.events = capture.events;
        self.exhausted = false;
        let mut session = new_session(self.conflate_us, self.venue == Venue::Bitstamp);
        self.prime(&mut session)?;
        Ok(session)
    }

    /// Moves `session` to batch `target`; backwards seeks replay from the start.
    pub fn seek(&mut self, session: &mut Session, target: u64) -> Result<()> {
        if target < session.batches() {
            *session = self.restart()?;
        }
        while session.batches() < target {
            match self.events.next() {
                Some(ev) => {
                    session.ingest(ev?)?;
                }
                None => {
                    if !self.exhausted {
                        session.finish();
                        self.exhausted = true;
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn at_end(&self, session: &Session) -> bool {
        self.exhausted || session.batches() >= self.total
    }

    /// Applies the leading snapshot so batch 0 shows a book.
    fn prime(&mut self, session: &mut Session) -> Result<()> {
        if let Some(ev) = self.events.next() {
            session.ingest(ev?)?;
        }
        Ok(())
    }
}

fn count_batches(dir: &std::path::Path, conflate_us: u64) -> Result<u64> {
    let capture = Capture::open(dir, None)?;
    let model = model_by_name("fifo").expect("fifo is registered");
    let mut session = Session::new(vec![model], conflate_us);
    for ev in capture.events {
        session.ingest(ev?)?;
    }
    session.finish();
    Ok(session.batches())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LiveStatus {
    Connecting,
    Streaming,
    Ended,
}

pub struct Live {
    pub pair: String,
    pub scale: Scale,
    pub status: LiveStatus,
    pub events: u64,
    /// Most recent stream/ingest errors, oldest first.
    pub errors: Vec<String>,
    rx: Receiver<Result<FeedEvent>>,
}

impl Live {
    pub fn start(pair: String, conflate_us: u64) -> Result<(Self, Session)> {
        let scale = bitstamp::DEFAULT_SCALE;
        let (tx, rx) = mpsc::channel();
        let feed_pair = pair.clone();
        thread::Builder::new()
            .name("bitstamp-feed".into())
            .spawn(move || {
                let err_tx = tx.clone();
                if let Err(e) = bitstamp::live_feed(&feed_pair, scale, tx) {
                    let _ = err_tx.send(Err(e));
                }
            })
            .context("spawn live feed thread")?;
        let live = Self {
            pair,
            scale,
            status: LiveStatus::Connecting,
            events: 0,
            errors: Vec::new(),
            rx,
        };
        Ok((live, new_session(conflate_us, true)))
    }

    /// Ingests everything the feed thread has sent since the last call.
    pub fn drain(&mut self, session: &mut Session) {
        loop {
            match self.rx.try_recv() {
                Ok(Ok(ev)) => {
                    self.events += 1;
                    if self.status == LiveStatus::Connecting {
                        self.status = LiveStatus::Streaming;
                    }
                    if let Err(e) = session.ingest(ev) {
                        self.push_error(format!("ingest: {e:#}"));
                    }
                }
                Ok(Err(e)) => self.push_error(format!("{e:#}")),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.status = LiveStatus::Ended;
                    break;
                }
            }
        }
    }

    fn push_error(&mut self, msg: String) {
        if self.errors.len() == MAX_ERRORS {
            self.errors.remove(0);
        }
        self.errors.push(msg);
    }
}
