//! A trainable queue model: a lookup policy from discretised level-change
//! features to one of a fixed set of queue actions, fitted by oracle labelling
//! against ground truth.
//!
//! **Training.** A capture is replayed through a [`Session`]; after every L2 batch
//! the truth book sits at the batch boundary. The trainer keeps its own inferred
//! queues. For each level update it applies every applicable [`Action`] to a copy
//! of its queue, scores each result against the true level with [`similarity`],
//! credits a *win* to every action that reaches the best score (ties all win),
//! then advances its own queue with the highest-priority winner. Samples where
//! every candidate scores the same carry no information and are skipped.
//!
//! **Fit.** Wins are tallied per feature bucket at several backoff granularities
//! ([`BACKOFF`], finest first). Prediction takes the finest bucket with at least
//! `min_support` samples and returns its most-winning applicable action (ties go
//! to the earlier action in [`ACTIONS`]); the last level is a global default, and
//! a bucket-less change falls back to the first applicable action.
//!
//! **Similarity** (higher is better, in `[0, 4]`), between a candidate queue `I`
//! and the true level `T` at the batch boundary:
//! * `J` – multiset Jaccard of order sizes: `m / (|T| + |I| − m)` with `m` the
//!   exact-size overlap (1 when both are empty);
//! * `A` – order agreement: equal-size orders are paired in queue order, and
//!   `A = 1 − mean(|ahead_T − ahead_I| / total)` over the `m` pairs (0 if `m = 0`);
//! * `N` – arrival placement, mirroring the scorer's queue-ahead metric: the n-th
//!   true order that arrived in this batch is paired with the n-th order the
//!   candidate created in it (or its last one); `N = 1 − mean(err)` with
//!   `err = |ahead_T − ahead_I| / total`, or 1 when the candidate created none
//!   (`N = 1` when nothing arrived);
//! * `C` – count agreement: `1 − ||T| − |I|| / max(|T|, |I|)`;
//!
//! and `sim = J + A + N + C`.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::l2::{L2Batch, levels_of};
use crate::model::{Births, Book, Count, Hybrid, LevelChange, Queue, QueueModel};
use crate::session::{FeedEvent, L2Source, Session};
use crate::truth::{TruthBook, TruthLevel};
use crate::types::{Qty, Ts};

pub const SCHEMA_VERSION: u32 = 1;

/// A way to turn the inferred queue into one with the published total.
/// Declaration order is the tie-break priority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Action {
    /// Net zero, no trades: leave the queue alone.
    Keep,
    /// Increase joins the back as one order.
    AddBack,
    /// Traded volume fills from the front; the remaining increase joins the back.
    FillAddBack,
    /// Decrease cancels the newest order of exactly that size, else from the back.
    CutExactNewest,
    /// Fill from the front, then [`CutExactNewest`](Self::CutExactNewest) on the rest.
    FillCutExactNewest,
    /// The `count` model with trades ignored.
    CountAware,
    /// The `count` model (fills from the front first).
    FillCountAware,
    /// Increase joins the back split into `count − orders` equal orders.
    AddSplit,
    /// Fill from the front, then [`AddSplit`](Self::AddSplit) on the rest.
    FillAddSplit,
    /// Decrease taken from the newest quantity.
    CutBack,
    /// Fill from the front, then the rest of the decrease from the back.
    FillCutBack,
    /// Decrease taken from the front (a fill).
    CutFront,
    /// Decrease cancels the oldest order of exactly that size, else from the front.
    CutExactOldest,
    /// Fill from the front, then [`CutExactOldest`](Self::CutExactOldest) on the rest.
    FillCutExactOldest,
    /// Increase is a priority insert at the front.
    AddFront,
    /// Fill from the front, then the remaining increase at the front.
    FillAddFront,
}

pub const N_ACTIONS: usize = 16;

/// Every action, in priority order.
pub const ACTIONS: [Action; N_ACTIONS] = [
    Action::Keep,
    Action::AddBack,
    Action::FillAddBack,
    Action::CutExactNewest,
    Action::FillCutExactNewest,
    Action::CountAware,
    Action::FillCountAware,
    Action::AddSplit,
    Action::FillAddSplit,
    Action::CutBack,
    Action::FillCutBack,
    Action::CutFront,
    Action::CutExactOldest,
    Action::FillCutExactOldest,
    Action::AddFront,
    Action::FillAddFront,
];

impl Action {
    fn index(self) -> usize {
        self as usize
    }

    fn fills(self) -> bool {
        use Action::*;
        matches!(
            self,
            FillAddBack
                | FillCutExactNewest
                | FillAddSplit
                | FillCutBack
                | FillCutExactOldest
                | FillAddFront
        )
    }

    fn name(self) -> String {
        serde_json::to_value(self)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default()
    }
}

/// Actions that make a distinct, sensible move for `change`, in priority order.
pub fn candidates(queue: &Queue, change: LevelChange, out: &mut Vec<Action>) {
    use Action::*;
    out.clear();
    let (old, new) = (queue.total(), change.new_total);
    let counts = change.count.is_some();
    if new > old {
        out.extend([AddBack, AddFront]);
        if counts {
            out.push(AddSplit);
        }
    } else if new < old {
        out.extend([CutExactNewest, CutBack, CutFront, CutExactOldest]);
    } else {
        out.push(Keep);
    }
    if counts {
        out.push(CountAware);
    }
    if change.traded > 0 && old > 0 {
        let rest = old - change.traded.min(old);
        if new > rest {
            out.extend([FillAddBack, FillAddFront]);
            if counts {
                out.push(FillAddSplit);
            }
        } else if new < rest {
            out.extend([FillCutExactNewest, FillCutBack, FillCutExactOldest]);
        }
        // new == rest: the fill explains the drop exactly, which is `CutFront`.
        if counts {
            out.push(FillCountAware);
        }
    }
    out.sort_unstable();
}

/// Applies `action`; always leaves `queue.total() == change.new_total`.
pub fn apply_action(action: Action, queue: &mut Queue, change: LevelChange, births: &mut Births) {
    use Action::*;
    match action {
        CountAware => {
            return Count.apply(
                queue,
                LevelChange {
                    traded: 0,
                    ..change
                },
                births,
            );
        }
        FillCountAware => return Count.apply(queue, change, births),
        _ => {}
    }
    if action.fills() {
        queue.take_front(change.traded);
    }
    let (rest, new) = (queue.total(), change.new_total);
    if new > rest {
        let add = new - rest;
        match action {
            AddFront | FillAddFront => queue.push_front(add, births.next()),
            AddSplit | FillAddSplit => {
                let k = change
                    .count
                    .map_or(1, |c| (i64::from(c) - queue.len() as i64).max(1) as u64);
                queue.push_split(add, k, births);
            }
            _ => queue.push_back(add, births.next()),
        }
    } else if new < rest {
        let cut = rest - new;
        match action {
            CutFront => {
                queue.take_front(cut);
            }
            CutExactNewest | FillCutExactNewest => {
                if !queue.remove_exact(cut) {
                    queue.take_back(cut);
                }
            }
            CutExactOldest | FillCutExactOldest => {
                if !queue.remove_exact_oldest(cut) {
                    queue.take_front(cut);
                }
            }
            _ => {
                queue.take_back(cut);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Features

pub const N_FEATURES: usize = 7;
/// One bucket index per feature; [`ANY`] marks a feature dropped by backoff.
pub type Key = [u8; N_FEATURES];
const ANY: u8 = u8::MAX;

pub struct FeatureSpec {
    pub name: &'static str,
    pub buckets: &'static [&'static str],
}

/// Feature order matches [`Key`] positions.
pub const FEATURES: [FeatureSpec; N_FEATURES] = [
    // Sign of new total − inferred total.
    FeatureSpec {
        name: "dir",
        buckets: &["up", "down", "flat"],
    },
    // |Δ| relative to the inferred total ("new" = the level was empty).
    FeatureSpec {
        name: "mag",
        buckets: &[
            "new", "0", "<=2%", "<=10%", "<=30%", "<=60%", "<100%", ">=100%",
        ],
    },
    // Traded maker volume vs. the drop (">drop" includes any trade on a rise).
    FeatureSpec {
        name: "traded",
        buckets: &["none", "<drop", "=drop", ">drop"],
    },
    // k = published count − inferred orders, or n/a without counts.
    FeatureSpec {
        name: "count",
        buckets: &["n/a", "<=-2", "-1", "0", "+1", ">=+2"],
    },
    // Levels better than this one (0 = touch).
    FeatureSpec {
        name: "rank",
        buckets: &["0", "1", "2-4", "5+"],
    },
    // On a drop: does an inferred order (the newest = "last", any = "single") or a
    // pair among the newest PAIR_WINDOW orders have exactly the dropped size?
    FeatureSpec {
        name: "exact",
        buckets: &["n/a", "none", "last", "single", "pair"],
    },
    // Inferred orders at the level.
    FeatureSpec {
        name: "qlen",
        buckets: &["0", "1", "2-3", "4-7", "8+"],
    },
];

/// Backoff granularities (feature indices kept), finest first; the last is global.
pub const BACKOFF: [&[usize]; 6] = [
    &[0, 1, 2, 3, 4, 5, 6],
    &[0, 1, 2, 3, 5],
    &[0, 2, 3, 5],
    &[0, 2, 3],
    &[0],
    &[],
];

const PAIR_WINDOW: usize = 64;

/// Discretises one level change against the inferred queue it will act on.
pub fn features(queue: &Queue, change: LevelChange) -> Key {
    let (old, new) = (queue.total(), change.new_total);
    let delta = old.abs_diff(new);
    let dir = match new.cmp(&old) {
        std::cmp::Ordering::Greater => 0,
        std::cmp::Ordering::Less => 1,
        std::cmp::Ordering::Equal => 2,
    };
    let mag = if old == 0 {
        0
    } else if delta == 0 {
        1
    } else {
        let pct = u128::from(delta) * 100;
        let old = u128::from(old);
        match () {
            _ if pct <= 2 * old => 2,
            _ if pct <= 10 * old => 3,
            _ if pct <= 30 * old => 4,
            _ if pct <= 60 * old => 5,
            _ if pct < 100 * old => 6,
            _ => 7,
        }
    };
    let drop = old.saturating_sub(new);
    let traded = match change.traded {
        0 => 0,
        t if t < drop => 1,
        t if t == drop => 2,
        _ => 3,
    };
    let count = match change.count {
        None => 0,
        Some(c) => match i64::from(c) - queue.len() as i64 {
            ..=-2 => 1,
            -1 => 2,
            0 => 3,
            1 => 4,
            _ => 5,
        },
    };
    let rank = match change.rank {
        0 => 0,
        1 => 1,
        2..=4 => 2,
        _ => 3,
    };
    let exact = if drop == 0 {
        0
    } else if queue.orders().back().is_some_and(|o| o.qty == drop) {
        2
    } else if queue.orders().iter().any(|o| o.qty == drop) {
        3
    } else if has_pair(queue, drop) {
        4
    } else {
        1
    };
    let qlen = match queue.len() {
        0 => 0,
        1 => 1,
        2..=3 => 2,
        4..=7 => 3,
        _ => 4,
    };
    [dir, mag, traded, count, rank, exact, qlen]
}

fn has_pair(queue: &Queue, sum: Qty) -> bool {
    let orders = queue.orders();
    let start = orders.len().saturating_sub(PAIR_WINDOW);
    let newest = orders.range(start..);
    newest
        .clone()
        .enumerate()
        .any(|(i, a)| a.qty < sum && newest.clone().skip(i + 1).any(|b| a.qty + b.qty == sum))
}

fn mask(key: &Key, keep: &[usize]) -> Key {
    let mut out = [ANY; N_FEATURES];
    for &i in keep {
        out[i] = key[i];
    }
    out
}

fn key_string(key: &Key) -> String {
    let mut s = String::new();
    for (spec, &b) in FEATURES.iter().zip(key) {
        if b != ANY {
            if !s.is_empty() {
                s.push(' ');
            }
            let _ = write!(s, "{}={}", spec.name, spec.buckets[usize::from(b)]);
        }
    }
    if s.is_empty() { "*".into() } else { s }
}

fn parse_key(s: &str) -> Result<Key> {
    let mut key = [ANY; N_FEATURES];
    if s == "*" {
        return Ok(key);
    }
    for part in s.split(' ') {
        let (name, bucket) = part
            .split_once('=')
            .with_context(|| format!("bad bucket key {s:?}"))?;
        let f = FEATURES
            .iter()
            .position(|f| f.name == name)
            .with_context(|| format!("unknown feature {name:?} in {s:?}"))?;
        let b = FEATURES[f]
            .buckets
            .iter()
            .position(|&x| x == bucket)
            .with_context(|| format!("unknown bucket {bucket:?} in {s:?}"))?;
        key[f] = b as u8;
    }
    Ok(key)
}

// ---------------------------------------------------------------------------
// Policy

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Bucket {
    /// Informative samples that fell in this bucket.
    n: u64,
    /// Per action: samples in which it reached the best similarity.
    wins: [u64; N_ACTIONS],
}

/// Where the training samples came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum SourceSpec {
    Derived { conflate_ms: u64, counts: bool },
    Venue,
}

impl From<L2Source> for SourceSpec {
    fn from(s: L2Source) -> Self {
        match s {
            L2Source::Derived {
                conflate_us,
                counts,
            } => Self::Derived {
                conflate_ms: conflate_us / 1000,
                counts,
            },
            L2Source::Venue => Self::Venue,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainStats {
    pub batches: u64,
    pub level_updates: u64,
    /// Recorded (informative, post-warm-up) samples.
    pub samples: u64,
    /// Post-warm-up updates where every candidate scored the same.
    pub uninformative: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingMeta {
    pub captures: Vec<String>,
    pub l2_source: SourceSpec,
    pub warmup_secs: u64,
    pub stats: TrainStats,
}

/// The fitted lookup policy.
#[derive(Clone, Debug, PartialEq)]
pub struct Policy {
    pub min_support: u64,
    pub meta: TrainingMeta,
    /// One table per [`BACKOFF`] level.
    tables: Vec<HashMap<Key, Bucket>>,
}

impl Policy {
    fn empty(min_support: u64, meta: TrainingMeta) -> Self {
        Self {
            min_support,
            meta,
            tables: vec![HashMap::new(); BACKOFF.len()],
        }
    }

    /// Credits one informative sample to every backoff level.
    fn record(&mut self, key: &Key, winners: &[Action]) {
        for (table, keep) in self.tables.iter_mut().zip(BACKOFF) {
            let bucket = table.entry(mask(key, keep)).or_default();
            bucket.n += 1;
            for &a in winners {
                bucket.wins[a.index()] += 1;
            }
        }
    }

    /// The action for `key` among `applicable` (non-empty, priority order), and the
    /// backoff level that decided it (`BACKOFF.len()` = fixed fallback).
    pub fn choose(&self, key: &Key, applicable: &[Action]) -> (Action, usize) {
        let last = self.tables.len().saturating_sub(1);
        for (level, (table, keep)) in self.tables.iter().zip(BACKOFF).enumerate() {
            let Some(bucket) = table.get(&mask(key, keep)) else {
                continue;
            };
            if bucket.n < self.min_support && level != last {
                continue;
            }
            let mut best: Option<(Action, u64)> = None;
            for &a in applicable {
                let w = bucket.wins[a.index()];
                if w > 0 && best.is_none_or(|(_, bw)| w > bw) {
                    best = Some((a, w));
                }
            }
            if let Some((a, _)) = best {
                return (a, level);
            }
        }
        (applicable[0], BACKOFF.len())
    }

    pub fn to_json(&self) -> Result<String> {
        let tables = self
            .tables
            .iter()
            .map(|table| {
                table
                    .iter()
                    .map(|(key, b)| {
                        let wins = ACTIONS
                            .iter()
                            .filter(|a| b.wins[a.index()] > 0)
                            .map(|&a| (a, b.wins[a.index()]))
                            .collect();
                        (key_string(key), BucketFile { n: b.n, wins })
                    })
                    .collect()
            })
            .collect();
        let file = ModelFile {
            schema: SCHEMA_VERSION,
            model: "learned".into(),
            similarity: SIMILARITY.into(),
            features: FEATURES
                .iter()
                .map(|f| FeatureFile {
                    name: f.name.into(),
                    buckets: f.buckets.iter().map(|&b| b.into()).collect(),
                })
                .collect(),
            actions: ACTIONS.to_vec(),
            backoff: backoff_names(),
            min_support: self.min_support,
            training: self.meta.clone(),
            tables,
        };
        Ok(serde_json::to_string_pretty(&file)?)
    }

    pub fn from_json(raw: &str) -> Result<Self> {
        let file: ModelFile = serde_json::from_str(raw)?;
        if file.schema != SCHEMA_VERSION {
            bail!(
                "model schema {} (this build reads {SCHEMA_VERSION})",
                file.schema
            );
        }
        let same_features =
            file.features.len() == N_FEATURES
                && file.features.iter().zip(&FEATURES).all(|(f, spec)| {
                    f.name == spec.name && f.buckets.iter().eq(spec.buckets.iter())
                });
        if !same_features || file.actions != ACTIONS || file.backoff != backoff_names() {
            bail!("model feature/action/backoff spec differs from this build");
        }
        let mut tables = Vec::with_capacity(file.tables.len());
        for table in file.tables {
            let mut out = HashMap::with_capacity(table.len());
            for (key, b) in table {
                let mut wins = [0; N_ACTIONS];
                for (a, w) in b.wins {
                    wins[a.index()] = w;
                }
                out.insert(parse_key(&key)?, Bucket { n: b.n, wins });
            }
            tables.push(out);
        }
        if tables.len() != BACKOFF.len() {
            bail!(
                "model has {} tables, expected {}",
                tables.len(),
                BACKOFF.len()
            );
        }
        Ok(Self {
            min_support: file.min_support,
            meta: file.training,
            tables,
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        std::fs::write(path, self.to_json()?).with_context(|| format!("write {}", path.display()))
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        Self::from_json(&raw).with_context(|| format!("load model {}", path.display()))
    }

    /// Human-readable training summary: action wins and the busiest finest buckets.
    pub fn summary(&self, top: usize) -> String {
        let st = &self.meta.stats;
        let mut s = String::new();
        let _ = writeln!(
            s,
            "{} samples ({} uninformative skipped) from {} batches / {} level updates; min support {}",
            st.samples, st.uninformative, st.batches, st.level_updates, self.min_support
        );
        let Some(global) = self.tables.last().and_then(|t| t.get(&[ANY; N_FEATURES])) else {
            return s;
        };
        let _ = writeln!(s, "\naction | optimal in (ties count for each)");
        let mut order: Vec<Action> = ACTIONS.to_vec();
        order.sort_by_key(|a| std::cmp::Reverse(global.wins[a.index()]));
        for a in order.iter().filter(|a| global.wins[a.index()] > 0) {
            let _ = writeln!(
                s,
                "  {:<22} {:>6.1}%",
                a.name(),
                pct(global.wins[a.index()], global.n)
            );
        }
        let mut buckets: Vec<(&Key, &Bucket)> = self.tables[0].iter().collect();
        buckets.sort_by(|a, b| b.1.n.cmp(&a.1.n).then(a.0.cmp(b.0)));
        let _ = writeln!(
            s,
            "\ntop buckets (finest level): samples | best action (wins) | bucket"
        );
        for (key, b) in buckets.into_iter().take(top) {
            let best = ACTIONS
                .iter()
                .copied()
                .max_by_key(|a| (b.wins[a.index()], std::cmp::Reverse(*a)))
                .expect("non-empty");
            let _ = writeln!(
                s,
                "  {:>7} | {:<22} {:>5.1}% | {}",
                b.n,
                best.name(),
                pct(b.wins[best.index()], b.n),
                key_string(key)
            );
        }
        s
    }
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 * 100.0 / d as f64
    }
}

fn backoff_names() -> Vec<Vec<String>> {
    BACKOFF
        .iter()
        .map(|keep| keep.iter().map(|&i| FEATURES[i].name.to_string()).collect())
        .collect()
}

const SIMILARITY: &str = "J + A + N + C: J = exact-size multiset Jaccard; A = 1 - mean \
    |ahead_true - ahead_inferred| / total over equal-size orders paired in queue order (0 if none); \
    N = 1 - mean queue-ahead error of this batch's true arrivals vs. the candidate's n-th new order \
    (1 per arrival if none created; N = 1 if nothing arrived); C = 1 - |count_true - \
    count_inferred| / max(counts)";

#[derive(Serialize, Deserialize)]
struct FeatureFile {
    name: String,
    buckets: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct BucketFile {
    n: u64,
    wins: BTreeMap<Action, u64>,
}

#[derive(Serialize, Deserialize)]
struct ModelFile {
    schema: u32,
    model: String,
    similarity: String,
    features: Vec<FeatureFile>,
    actions: Vec<Action>,
    backoff: Vec<Vec<String>>,
    min_support: u64,
    training: TrainingMeta,
    tables: Vec<BTreeMap<String, BucketFile>>,
}

/// The fitted policy as a [`QueueModel`].
pub struct LearnedModel {
    policy: Policy,
    cands: Vec<Action>,
}

impl LearnedModel {
    pub fn new(policy: Policy) -> Self {
        Self {
            policy,
            cands: Vec::with_capacity(N_ACTIONS),
        }
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }
}

impl QueueModel for LearnedModel {
    fn name(&self) -> &'static str {
        "learned"
    }

    fn apply(&mut self, queue: &mut Queue, change: LevelChange, births: &mut Births) {
        candidates(queue, change, &mut self.cands);
        let (action, _) = self.policy.choose(&features(queue, change), &self.cands);
        apply_action(action, queue, change, births);
    }
}

// ---------------------------------------------------------------------------
// Training

/// The true level, prepared for [`similarity`].
#[derive(Default)]
struct Target {
    total: Qty,
    /// `(size, qty ahead)` of every true order, sorted.
    orders: Vec<(Qty, Qty)>,
    /// Qty ahead of each order that arrived in this batch, in queue order.
    arrivals: Vec<Qty>,
}

impl Target {
    fn load(&mut self, level: Option<&TruthLevel>, arrived_after: Ts) {
        self.orders.clear();
        self.arrivals.clear();
        self.total = 0;
        let Some(level) = level else { return };
        self.total = level.total();
        let mut ahead = 0;
        for o in level.orders() {
            self.orders.push((o.qty, ahead));
            if o.born > arrived_after {
                self.arrivals.push(ahead);
            }
            ahead += o.qty;
        }
        self.orders.sort_unstable();
    }
}

#[derive(Default)]
struct Scratch {
    target: Target,
    inferred: Vec<(Qty, Qty)>,
    created: Vec<Qty>,
    sims: Vec<f64>,
    cands: Vec<Action>,
    winners: Vec<Action>,
}

/// See the module docs; `batch` is the seq whose new orders count as arrivals.
fn similarity(target: &Target, queue: &Queue, batch: u64, scratch: &mut Scratch) -> f64 {
    let total = target.total.max(queue.total()).max(1) as f64;
    scratch.inferred.clear();
    scratch.created.clear();
    let mut ahead = 0;
    for o in queue.orders() {
        scratch.inferred.push((o.qty, ahead));
        if o.born_batch == batch {
            scratch.created.push(ahead);
        }
        ahead += o.qty;
    }
    scratch.inferred.sort_unstable();

    let (t, inf) = (&target.orders, &scratch.inferred);
    let (mut i, mut j, mut m, mut err) = (0, 0, 0usize, 0.0);
    while i < t.len() && j < inf.len() {
        match t[i].0.cmp(&inf[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                err += t[i].1.abs_diff(inf[j].1) as f64 / total;
                m += 1;
                i += 1;
                j += 1;
            }
        }
    }
    let union = t.len() + inf.len() - m;
    let jaccard = if union == 0 {
        1.0
    } else {
        m as f64 / union as f64
    };
    let agreement = if union == 0 {
        1.0
    } else if m == 0 {
        0.0
    } else {
        1.0 - err / m as f64
    };
    let arrivals = if target.arrivals.is_empty() {
        1.0
    } else {
        let errs: f64 = target
            .arrivals
            .iter()
            .enumerate()
            .map(
                |(n, &true_ahead)| match scratch.created.get(n).or(scratch.created.last()) {
                    Some(&inf_ahead) => (true_ahead.abs_diff(inf_ahead) as f64 / total).min(1.0),
                    None => 1.0,
                },
            )
            .sum();
        1.0 - errs / target.arrivals.len() as f64
    };
    let counts = 1.0 - t.len().abs_diff(inf.len()) as f64 / t.len().max(inf.len()).max(1) as f64;
    jaccard + agreement + arrivals + counts
}

/// Oracle labeller: replays captures and accumulates per-bucket action wins.
pub struct Trainer {
    policy: Policy,
    book: Book,
    prev_end: Option<Ts>,
    scratch: Scratch,
}

impl Trainer {
    pub fn new(source: L2Source, warmup_secs: u64, min_support: u64) -> Self {
        let meta = TrainingMeta {
            captures: Vec::new(),
            l2_source: source.into(),
            warmup_secs,
            stats: TrainStats::default(),
        };
        Self {
            policy: Policy::empty(min_support, meta),
            book: Book::default(),
            prev_end: None,
            scratch: Scratch::default(),
        }
    }

    /// Replays one ground-truth capture (`name` is recorded in the metadata).
    pub fn train_capture(
        &mut self,
        name: &str,
        events: impl Iterator<Item = Result<FeedEvent>>,
    ) -> Result<()> {
        let source = match self.policy.meta.l2_source {
            SourceSpec::Derived {
                conflate_ms,
                counts,
            } => L2Source::Derived {
                conflate_us: conflate_ms * 1000,
                counts,
            },
            SourceSpec::Venue => L2Source::Venue,
        };
        let derived = matches!(source, L2Source::Derived { .. });
        let counts = matches!(source, L2Source::Derived { counts: true, .. });
        let warmup_us = self.policy.meta.warmup_secs * 1_000_000;
        let mut session = Session::new(Vec::new(), source);
        self.book = Book::default();
        self.prev_end = None;
        let mut start: Option<Ts> = None;
        let mut on_batch = |this: &mut Self, session: &Session| {
            if let Some(batch) = session.last_batch() {
                let start = *start.get_or_insert(batch.start);
                this.observe(batch, session.truth(), batch.end >= start + warmup_us);
            }
        };
        for ev in events {
            let ev = ev?;
            // Mirror the session's model seeding on this trainer's own book.
            let seed = match &ev {
                FeedEvent::TruthSnapshot { ts, .. } if derived => Some((*ts, None)),
                FeedEvent::L2Snapshot { ts, levels } if !(derived && session.truth().is_some()) => {
                    Some((*ts, Some(levels.clone())))
                }
                _ => None,
            };
            let flushed = session.ingest(ev)?;
            if let Some((ts, levels)) = seed {
                let levels = match levels {
                    Some(l) => l,
                    None => levels_of(session.truth().expect("truth just seeded"), counts),
                };
                self.book.seed(ts, &levels);
            }
            if flushed > 0 {
                on_batch(self, &session);
            }
        }
        if session.finish() > 0 {
            on_batch(self, &session);
        }
        self.policy.meta.captures.push(name.to_string());
        Ok(())
    }

    pub fn finish(self) -> Policy {
        self.policy
    }

    /// Labels and applies one batch; venue L2 that precedes the first truth snapshot
    /// only advances the queues (with the `hybrid` rule).
    fn observe(&mut self, batch: &L2Batch, truth: Option<&TruthBook>, warm: bool) {
        let Self {
            policy,
            book,
            prev_end,
            scratch,
        } = self;
        let prev = prev_end.replace(batch.end);
        let record = warm && prev.is_some();
        let arrived_after = prev.unwrap_or(Ts::MAX);
        policy.meta.stats.batches += 1;
        book.apply_with(batch, "learn-oracle", |lu, queue, change, births| {
            policy.meta.stats.level_updates += 1;
            if change.new_total == 0 {
                queue.take_front(queue.total());
                return;
            }
            let Some(truth) = truth else {
                return Hybrid.apply(queue, change, births);
            };
            let mut cands = std::mem::take(&mut scratch.cands);
            candidates(queue, change, &mut cands);
            let mut target = std::mem::take(&mut scratch.target);
            target.load(truth.level(lu.side, lu.px), arrived_after);
            scratch.sims.clear();
            for &a in &cands {
                let mut q = queue.clone();
                let mut ids = 0;
                apply_action(
                    a,
                    &mut q,
                    change,
                    &mut Births::new(&mut ids, batch.end, batch.seq),
                );
                let sim = similarity(&target, &q, batch.seq, scratch);
                scratch.sims.push(sim);
            }
            let best = scratch.sims.iter().copied().fold(f64::MIN, f64::max);
            scratch.winners.clear();
            scratch.winners.extend(
                cands
                    .iter()
                    .zip(&scratch.sims)
                    .filter(|&(_, &s)| s >= best - 1e-12)
                    .map(|(&a, _)| a),
            );
            if record {
                if scratch.winners.len() < cands.len() {
                    policy.record(&features(queue, change), &scratch.winners);
                    policy.meta.stats.samples += 1;
                } else {
                    policy.meta.stats.uninformative += 1;
                }
            }
            apply_action(scratch.winners[0], queue, change, births);
            scratch.cands = cands;
            scratch.target = target;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Birth;

    /// Deterministic xorshift for reproducible random cases.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn queue_of(sizes: &[Qty]) -> Queue {
        let mut q = Queue::default();
        for (i, &s) in sizes.iter().enumerate() {
            q.push_back(
                s,
                Birth {
                    id: i as u64 + 1,
                    ts: 0,
                    batch: 0,
                },
            );
        }
        q
    }

    fn random_case(rng: &mut Rng) -> (Queue, LevelChange) {
        let n = rng.below(6) as usize;
        let sizes: Vec<Qty> = (0..n).map(|_| 1 + rng.below(5)).collect();
        let q = queue_of(&sizes);
        let new_total = rng.below(q.total() + 8);
        let change = LevelChange {
            new_total,
            traded: if rng.below(3) == 0 {
                rng.below(q.total() + 1)
            } else {
                0
            },
            count: (rng.below(2) == 0).then(|| rng.below(new_total.min(8) + 1) as u32),
            rank: rng.below(7) as u8,
        };
        (q, change)
    }

    fn meta() -> TrainingMeta {
        TrainingMeta {
            captures: vec!["synthetic".into()],
            l2_source: SourceSpec::Derived {
                conflate_ms: 100,
                counts: true,
            },
            warmup_secs: 0,
            stats: TrainStats::default(),
        }
    }

    #[test]
    fn every_action_keeps_the_level_total() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..2000 {
            let (q, change) = random_case(&mut rng);
            for a in ACTIONS {
                let mut q = q.clone();
                let mut ids = 1000;
                apply_action(a, &mut q, change, &mut Births::new(&mut ids, 1, 1));
                assert_eq!(q.total(), change.new_total, "{a:?} on {change:?}");
            }
        }
    }

    #[test]
    fn saved_model_predicts_like_the_trained_one() {
        let mut rng = Rng(42);
        let mut policy = Policy::empty(3, meta());
        let mut cands = Vec::new();
        for _ in 0..3000 {
            let (q, change) = random_case(&mut rng);
            candidates(&q, change, &mut cands);
            let winners: Vec<Action> = cands
                .iter()
                .copied()
                .filter(|_| rng.below(3) == 0)
                .collect();
            if !winners.is_empty() {
                policy.record(&features(&q, change), &winners);
            }
        }
        let loaded = Policy::from_json(&policy.to_json().unwrap()).unwrap();
        assert_eq!(loaded, policy);

        let mut trained = LearnedModel::new(policy);
        let mut reloaded = LearnedModel::new(loaded);
        for _ in 0..3000 {
            let (q, change) = random_case(&mut rng);
            let (mut a, mut b) = (q.clone(), q);
            let (mut ia, mut ib) = (100, 100);
            trained.apply(&mut a, change, &mut Births::new(&mut ia, 1, 1));
            reloaded.apply(&mut b, change, &mut Births::new(&mut ib, 1, 1));
            assert_eq!(a.orders(), b.orders());
        }
    }

    #[test]
    fn unseen_buckets_back_off_to_coarser_levels() {
        use Action::*;
        let down = |qlen: u8, mag: u8| -> Key { [1, mag, 0, 0, 0, 1, qlen] };
        let mut policy = Policy::empty(5, meta());
        for _ in 0..10 {
            policy.record(&down(1, 3), &[CutBack]);
        }
        for _ in 0..20 {
            policy.record(&down(4, 3), &[CutFront]);
        }
        let cuts = [CutExactNewest, CutBack, CutFront, CutExactOldest];
        // Seen at the finest level.
        assert_eq!(policy.choose(&down(1, 3), &cuts), (CutBack, 0));
        // Unseen queue length: level 1 (no qlen/rank) pools both, front wins 20:10.
        assert_eq!(policy.choose(&down(3, 3), &cuts), (CutFront, 1));
        // Unseen magnitude too: falls to level 2 (dir, traded, count, exact).
        assert_eq!(policy.choose(&down(3, 6), &cuts), (CutFront, 2));
        // Too little support at the finest level: backs off despite a hit.
        let mut sparse = Policy::empty(5, meta());
        for _ in 0..3 {
            sparse.record(&down(1, 3), &[CutBack]);
        }
        for _ in 0..6 {
            sparse.record(&down(2, 3), &[CutFront]);
        }
        assert_eq!(sparse.choose(&down(1, 3), &cuts), (CutFront, 1));
        // Nothing ever won among these actions: fixed fallback (first applicable).
        let adds = [AddBack, AddFront];
        assert_eq!(
            policy.choose(&[0, 3, 0, 0, 0, 0, 1], &adds),
            (AddBack, BACKOFF.len())
        );
    }
}
