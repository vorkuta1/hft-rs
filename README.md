# hft-rs Proof of Concept

`hft-rs` is a collection of small Rust crates that explore a low-latency market-data pipeline. The current proof of concept focuses on:

- Reusable engine scaffolding that pins a core and drives venue-specific handlers.
- A lock-free single-producer/single-consumer (SPSC) queue used to offload logging onto a dedicated thread.
- Instrumented WebSocket handlers for Binance and Hyperliquid test feeds.

The repository is intentionally simple: no async runtime, minimal dynamic allocation on hot paths, and explicit control over logging and metrics.

## Repository Layout

| Crate | Description |
| --- | --- |
| `engine_core` | Core engine loop, structured logger, shared handler trait |
| `spsc` | Generic bounded SPSC ring buffer used by the logging pipeline |
| `ws_binance` | Binance depth-stream handler that parses diffs and snapshots |
| `ws_hyperliquid` | Hyperliquid order-book handler (testnet) |
| `bins` | Binary examples (`poc_binance`, `poc_hyperliquid`) wiring handlers into the engine |
| `viz_l3` | Egui/eframe viewer for captured L3 inference datasets |
| `metrics`, `exec_hyperliquid` | Stubs/placeholders for future work |

Each crate is independent and can be built in isolation (`cargo check -p <crate>`).

## Data & Logging Flow

```
Main thread (Engine)
  ├─ Venue handler (Biz logic)
  │    └─ Emit structured events -> Logger producer (SPSC)
  └─ Periodic metrics -> Logger producer (SPSC)

Logging thread
  └─ SPSC consumer → formats events → stderr (or future sinks)
```

Key traits:

- `EngineHandler::poll_once(&Logger)` returns an optional `Duration` so the engine can histogram handler latency.
- `HandlerEvent` describes venue-specific milestones (connected, snapshot applied, synchronized, metrics, warnings/errors).
- The logger thread prints typed events immediately and exposes queue depth so backlog is visible (`EngineLogQueue`).

## Getting Started

### Build

```bash
cargo build
```

To build a specific handler or library:

```bash
cargo check -p ws_binance
cargo check -p ws_hyperliquid
```

### Run the Proof-of-Concept Binaries

> **Note:** The handlers connect to public WebSocket endpoints. Ensure you have outbound network access before running.

```bash
# Binance diff-depth stream (prod endpoints)
cargo run -p bin --bin poc_binance

# Hyperliquid testnet order-book stream
cargo run -p bin --bin poc_hyperliquid
```

Both binaries pin the current thread to the configured CPU core and stream log events to the dedicated logger thread. Example output:

```
[engine] binance:BTCUSDT started (+47us)
[binance:BTCUSDT] connected (+231us)
[binance:BTCUSDT] snapshot applied bids=1000 asks=1000 (+219us)
[latency] apply 87us (+118us)
[metrics] flushed (+5000413us)
[log-queue] depth=0 capacity=1024 (+5000420us)
```

## Logging & SPSC Details

### SPSC Queue (`crates/spsc`)

- `Ring<T, const N: usize>` maintains an `UnsafeCell<Option<T>>` buffer and atomic head/tail pointers.
- `channel()` returns `Producer` / `Consumer` handles (cloneable) so the engine can keep a handle while the logging thread owns the consumer.
- The queue exposes `len()` and `capacity()` to feed queue-depth metrics back to the engine.

### Structured Logger (`crates/engine_core`)

- `Logger` wraps the SPSC producer and provides helper methods:
  - `emit` for any `LogEventKind`.
  - `handler_event` for venue-specific `HandlerEvent` variants.
  - `shutdown` to drain graceful shutdown messages; the logger consumes a `Shutdown` event and exits.
- The logging thread catches up on events in order and prints concise lines.
- The engine run-loop is guarded with `catch_unwind` so handler panics still trigger logger shutdown.

## Handler Highlights

### Binance (`crates/ws_binance`)

- Connects to `wss://stream.binance.com:9443/ws/<symbol>@depth@100ms` with a manual redirect-follow helper.
- REST snapshot hits `https://api.binance.com/api/v3/depth?limit=1000` and applies the book into a small `AHashMap`-backed structure.
- Emits structured events: connection lifecycle, snapshot size, synchronization progress, per-diff stats, and latency percentiles.
- Supports an optional on-disk capture mode (see below) that writes the raw snapshot, diff stream, and metadata in a layout reusable by the replay tool.

### Hyperliquid (`crates/ws_hyperliquid`)

- Connects to the public testnet stream and subscribes to `l2Book` updates.
- Parses the nested JSON shape (with optional `data` envelope) and promotes snapshot vs. incremental updates.
- Emits connection, subscription, snapshot, and warning events to the shared logger.

## Development Tips

- `cargo fmt` and `cargo clippy` keep the code tidy.
- `cargo run -p xtask -- <task>` provides shortcuts (`fmt`, `fmt-check`, `clippy`, `check`, `lint`).
- Use `RUST_LOG` or external tooling if you want to redirect the structured logs to a file; currently everything goes to `stderr`.
- The SPSC queue is intentionally minimal—extend it with statistics or blocking behaviour if you need multi-producer semantics in the future.

## Capturing Binance Depth Streams

The `poc_binance` binary accepts two optional flags to persist raw market data in a layout consumed by `replay_binance`:

```bash
cargo run -p bin --bin poc_binance -- \
  --capture-dir data/binance \
  --capture-session 2025-10-04-btcusdt
```

- `--capture-dir` points at the root directory where sessions should be stored. The handler creates `<capture-dir>/<symbol>/<session>/`.
- `--capture-session` is an optional label; when omitted the handler uses the current epoch milliseconds.

Each session folder contains:

- `snapshot.json` – the REST snapshot payload used for seeding the replay.
- `diffs.jsonl` – raw WebSocket diff messages (one per line).
- `metadata.json` – capture metadata tracking timestamps and the latest applied sequence.

The capture mode updates `metadata.json` incrementally so long-running sessions can be inspected while still recording.

## Replaying Captures & Exporting Comparison Sets

The `replay_binance` binary consumes the captured files and reconstructs an L3 book. It can emit inferred events to stdout (default), JSONL, or bincode files:

```bash
cargo run -p replay_binance -- \
  --snapshot data/binance/btcusdt/2024-05-07-btcusdt/snapshot.json \
  --diffs data/binance/btcusdt/2024-05-07-btcusdt/diffs.jsonl \
  --out data/binance/btcusdt/2024-05-07-btcusdt/inferred.jsonl \
  --out-format jsonl
```

Use `--out-format bincode` to produce a compact binary stream suitable for offline comparison pipelines. All emitted events mirror the `InferredEvent` schema (see `crates/replay_binance/src/book.rs`).

## Visualising Captured Books

The `viz_l3` crate provides an interactive desktop viewer powered by `egui`/`eframe`. It plays back inferred event logs (JSONL or bincode) and renders bid/ask ladders alongside per-event metadata. Example:

```bash
cargo run -p viz_l3 -- --input \
  data/binance/btcusdt/2025-10-04-btcusdt/inferred.jsonl
```

- `--limit <n>` can be used to load only the first `n` events for quick inspection.
- Use the in-app slider or playback controls to scrub through events; adjust the displayed depth and price scale directly from the control bar.

The viewer mirrors the reconstruction logic in `replay_binance` so it can visualise either live captures (from `poc_binance`/`poc_hyperliquid`) or offline datasets when iterating on inference heuristics.
