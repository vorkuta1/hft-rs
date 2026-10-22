//! Feed sources driving the UI's `Session`: a seekable capture replay or a live socket.

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;

use anyhow::{Context, Result};
use l3lab::learn::{LearnedModel, Policy};
use l3lab::model::{QueueModel, all_models, model_by_name};
use l3lab::score::ScoreConfig;
use l3lab::session::{FeedEvent, L2Source, Session};
use l3lab::types::Scale;
use l3lab::venue::{Capture, Venue, bitstamp, hyperliquid};

const SCORE: ScoreConfig = ScoreConfig {
    top_k: 10,
    warmup_us: 30_000_000,
};
const MAX_ERRORS: usize = 20;

type Events = std::iter::Peekable<Box<dyn Iterator<Item = Result<FeedEvent>> + Send>>;

/// How sessions are built; kept so replays can rebuild them on restart and seek.
#[derive(Clone)]
pub struct Setup {
    pub l2: L2Source,
    pub learned: Option<Policy>,
}

impl Setup {
    /// Every rule model, plus the trained one if given; scored when there is truth.
    fn session(&self, has_truth: bool) -> Session {
        let mut models: Vec<Box<dyn QueueModel>> = all_models();
        if let Some(policy) = &self.learned {
            models.push(Box::new(LearnedModel::new(policy.clone())));
        }
        let session = Session::new(models, self.l2);
        if has_truth {
            session.with_scorer(SCORE)
        } else {
            session
        }
    }
}

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

pub struct Replay {
    pub dir: PathBuf,
    pub venue: Venue,
    pub has_truth: bool,
    pub scale: Scale,
    /// Batches in the whole capture.
    pub total: u64,
    setup: Setup,
    events: Events,
    exhausted: bool,
}

impl Replay {
    /// Counts the capture's batches, then opens it positioned at batch 0 (snapshot applied).
    pub fn open(dir: PathBuf, setup: Setup) -> Result<(Self, Session)> {
        let total = count_batches(&dir, setup.l2)?;
        let capture = Capture::open(&dir, None)?;
        let mut replay = Self {
            dir,
            venue: capture.venue,
            has_truth: capture.has_truth(),
            scale: capture.scale,
            total,
            setup,
            events: capture.events.peekable(),
            exhausted: false,
        };
        let mut session = replay.setup.session(replay.has_truth);
        replay.prime(&mut session)?;
        Ok((replay, session))
    }

    /// Reopens the capture from the start.
    pub fn restart(&mut self) -> Result<Session> {
        let capture = Capture::open(&self.dir, Some(self.scale))?;
        self.events = capture.events.peekable();
        self.exhausted = false;
        let mut session = self.setup.session(self.has_truth);
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

    /// Ingests the capture's leading snapshots (truth and/or L2) so batch 0 shows a book.
    fn prime(&mut self, session: &mut Session) -> Result<()> {
        while let Some(Ok(FeedEvent::TruthSnapshot { .. } | FeedEvent::L2Snapshot { .. })) =
            self.events.peek()
        {
            let ev = self.events.next().expect("peeked")?;
            session.ingest(ev)?;
        }
        Ok(())
    }
}

fn count_batches(dir: &std::path::Path, l2: L2Source) -> Result<u64> {
    let capture = Capture::open(dir, None)?;
    let model = model_by_name("fifo").expect("fifo is registered");
    let mut session = Session::new(vec![model], l2);
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

/// Which live stream to follow.
pub enum LiveFeed {
    /// Bitstamp L3 truth; models see derived L2.
    Bitstamp { pair: String },
    /// Hyperliquid public `l2Book` + trades; models only (no truth).
    HyperliquidPublic { coin: String },
    /// Hyperliquid L4 truth from an `order_book_server`; models see derived L2.
    HyperliquidL4 { coin: String, server: String },
}

impl LiveFeed {
    pub fn describe(&self) -> String {
        match self {
            LiveFeed::Bitstamp { pair } => format!("Bitstamp live L3 ({pair})"),
            LiveFeed::HyperliquidPublic { coin } => format!("Hyperliquid live public L2 ({coin})"),
            LiveFeed::HyperliquidL4 { coin, server } => {
                format!("Hyperliquid live L4 ({coin} via {server})")
            }
        }
    }

    fn has_truth(&self) -> bool {
        !matches!(self, LiveFeed::HyperliquidPublic { .. })
    }
}

pub struct Live {
    pub feed: LiveFeed,
    pub scale: Scale,
    pub status: LiveStatus,
    pub events: u64,
    /// Most recent stream/ingest errors, oldest first.
    pub errors: Vec<String>,
    rx: Receiver<Result<FeedEvent>>,
}

impl Live {
    /// Starts the feed thread. The public Hyperliquid feed always drives the models
    /// directly (`setup.l2` is overridden to `Venue`); the truth feeds use `setup.l2`.
    pub fn start(feed: LiveFeed, mut setup: Setup) -> Result<(Self, Session)> {
        let scale = match &feed {
            LiveFeed::Bitstamp { .. } => bitstamp::DEFAULT_SCALE,
            LiveFeed::HyperliquidPublic { coin } | LiveFeed::HyperliquidL4 { coin, .. } => {
                hyperliquid::fetch_scale(coin)?
            }
        };
        if let LiveFeed::HyperliquidPublic { .. } = feed {
            setup.l2 = L2Source::Venue;
        }
        let (tx, rx) = mpsc::channel();
        let run: Box<dyn FnOnce(Sender<Result<FeedEvent>>) -> Result<()> + Send> = match &feed {
            LiveFeed::Bitstamp { pair } => {
                let pair = pair.clone();
                Box::new(move |tx| bitstamp::live_feed(&pair, scale, tx))
            }
            LiveFeed::HyperliquidPublic { coin } => {
                let coin = coin.clone();
                Box::new(move |tx| hyperliquid::live_public(&coin, scale, tx))
            }
            LiveFeed::HyperliquidL4 { coin, server } => {
                let (coin, server) = (coin.clone(), server.clone());
                Box::new(move |tx| hyperliquid::live_l4(&server, &coin, scale, tx))
            }
        };
        thread::Builder::new()
            .name("live-feed".into())
            .spawn(move || {
                let err_tx = tx.clone();
                if let Err(e) = run(tx) {
                    let _ = err_tx.send(Err(e));
                }
            })
            .context("spawn live feed thread")?;
        let session = setup.session(feed.has_truth());
        let live = Self {
            feed,
            scale,
            status: LiveStatus::Connecting,
            events: 0,
            errors: Vec::new(),
            rx,
        };
        Ok((live, session))
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
