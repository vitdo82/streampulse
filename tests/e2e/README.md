# StreamPulse TUI — VHS End-to-End Tests

Terminal E2E tests for the TUI using [charmbracelet/vhs](https://github.com/charmbracelet/vhs):
each tape runs the real `bin/streampulse` binary in a PTY, drives it with scripted
keystrokes, and records the session as an animated GIF for review.

## Prerequisites

```bash
brew install vhs          # vhs 0.11.0+
docker compose up -d      # Kafka at localhost:9093 (producer writes fixtures)
make build                # bin/streampulse (requires Go toolchain)
```

## Running

```bash
make e2e                  # preflight checks + run ALL tapes → tests/e2e/screenshots/*.gif
make e2e-watch TAPE=02-topics-search.tape   # replay one tape live in your terminal
```

Individual tape (headless):

```bash
vhs tests/e2e/vhs/02-topics-search.tape
```

## Verification model

- **Artifacts:** one animated GIF per tape, committed for human review. The GIF shows
  the entire session — boot, keystrokes, and the resulting screens.
- **Text checks (`make e2e-verify`, requires `ffmpeg` + `tesseract`):** samples one
  frame per second of each GIF, OCRs the frames, and reports the stable labels that
  appear (e.g. `BROKERS`, `TOPICS`, `payments.dlq`). Per-tape expectations:

  ```bash
  make e2e-verify
  # 01-overview.gif        → BROKERS, TOPICS
  # 02-topics-search.gif   → StreamPulse, TOPICS, orders (filtered list)
  # 04-dlq.gif             → DEAD LETTER QUEUES, payments.dlq
  # 06-overlay-q-quit.gif  → Q_QUIT_OK (shell prompt returned after q)
  # 07-scrollable-content.gif → TOPICS, ANALYTICS, REBALANCES, PATTERNS
  ```
- **VHS v0.11 limitations:** no built-in assertions, and `Output "x.png"` writes a
  *directory of frames* rather than a single image — GIFs are the primary artifact.
  Deterministic text assertions live in the unit tests (`internal/tui/model_test.go`).

## Tape index

| Tape | Scenario | Key assertions (OCR on final frames) |
|------|----------|--------------------------------------|
| `01-overview.tape` | Boot TUI in Kafka mode | `StreamPulse`, `BROKERS`, `TOPICS`, `ACTIVITY LOG` |
| `02-topics-search.tape` | Topics tab + `/` search "orders" | `TOPICS`, `orders`, filtered list |
| `03-topic-tail.tape` | Enter on a topic → tail view, `p` pause | `TAIL orders`, `following`/`paused`, `[p 0\|o` message lines |
| `04-dlq.tape` | DLQ tab + Enter inspect | `payments.dlq`, `DEAD LETTER QUEUES`, inspect payloads |
| `05-analytics.tape` | Analytics tab + `a` analyze CLI view | `ANALYTICS`, `analyze --window`, `No growth detected` |
| `06-overlay-q-quit.tape` | `q` in the tail overlay quits the app | `Q_QUIT_OK` (sentinel printed after quit) |
| `07-scrollable-content.tape` | Small (80x24) terminal; Topics + Analytics scroll | `TOPICS`, `ANALYTICS`, `REBALANCES`, `PATTERNS` |
| `08-pagination.tape` | Large table + `pgup`/`pgdn` | `Showing`, `PAGINATION_OK` |
| `09-search-no-results.tape` | Topics search with no matches | `No match`, `NO_RESULTS_OK` |
| `10-help-modal.tape` | `?` opens the keybinding legend modal, `esc` closes | `KEYBINDINGS` |

## Status notes

- `03-topic-tail.tape` and `05-analytics.tape` (the `a`-view part) require a binary
  rebuilt after the tail + analyze features (`git log` ≥ `a6e23b2`); their GIFs are
  committed only after a fresh `make e2e`.
- Tapes assert stable labels only (never volatile counts); `Sleep`s cover the 2s
  refresh tick and 1s tail poll.
- The TUI panicked on narrow terminals (`strings.Repeat` in `renderHeader`) — fixed
  by clamping the pad width (commit pending Go rebuild); the e2e tapes run at
  1200px width where the layout fits.
- VHS runs the tape from the repo root; `Output` paths are relative to it.

## Make targets

| Target | Purpose |
|--------|---------|
| `make e2e` | preflight (vhs, Kafka, build) + run all tapes |
| `make e2e-watch TAPE=02-topics-search.tape` | live replay |
| `make e2e-verify` | OCR last frame of each GIF and check expected labels |
