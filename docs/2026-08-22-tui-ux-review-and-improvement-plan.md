# StreamPulse TUI — UX Review & Improvement Plan

> Date: 2026-08-22 · Author: UX review (e2e harness) · Scope: `internal/tui`
>
> Method: ran all 5 VHS e2e tapes against a live Kafka cluster, extracted one
> frame per second from each GIF, OCR'd the frames, and cross-checked against the
> render source (`model.go`, `views_dlq.go`, `views_tail.go`). Screenshots are in
> `tests/e2e/screenshots/review/`.

---

## 1. Screenshots

| # | Scenario | Final-state screenshot | What it shows |
|---|----------|------------------------|---------------|
| 1 | Overview | `review/01-overview-last.png` | Header, 3 summary cards (BROKERS/TOPICS/ALERTS), broker table, consumer-group table, activity log |
| 2 | Topics + search | `review/02-topics-search-last.png` | Topics table filtered to `orders`, search bar in footer |
| 3 | Topic tail | `review/03-topic-tail-last.png` | Tail overlay for `audit`, status `paused`, empty buffer |
| 4 | DLQ inspect | `review/04-dlq-last.png` | DLQ table, then inspect overlay for `orders.dlq` |
| 5 | Analytics | `review/05-analytics-last.png` | Analytics panes + `analyze --window 24h` CLI overlay |

Each is paired with its animated GIF in `tests/e2e/screenshots/*.gif` for full
interaction context.

---

## 2. Current UX model (baseline)

The TUI is a single fixed layout composed top-to-bottom of four regions
(`model.go:1100`):

1. **Header** — `⚡ StreamPulse v0.1.0` (left) + `Brokers: N │ Updated: HH:MM:SS │ Auto-refresh: 2s` (right, gray). (`renderHeader`, `model.go:1129`)
2. **Tab bar** — 6 tabs; active tab is white-on-purple, inactive tabs are gray on near-black. (`renderTabs`, `model.go:1163`)
3. **Content** — one of 6 views (plus overlays for tail / DLQ / analyze).
4. **Help bar** — one long global key legend. (`renderHelp`, `model.go:1528`)

Data refreshes every 2s via `tea.Tick`. All lists are `bubbles/table` with a
fixed height of `len(rows)+1` (`model.go:1006`), i.e. **no scrolling and no
pagination** for large tables. Analytics is a stack of 6 fixed panes with no
viewport.

---

## 3. Findings (by severity)

### Critical

**C1 — Analytics view has no scrolling and overflows the screen.**
`renderAnalyticsView` (`model.go:1293`) always emits 6 panes vertically with no
viewport. On a typical 24–40 row terminal the lower panes (REBALANCES, PATTERNS)
fall below the fold and are unreachable. The e2e GIF already shows the help bar
overlapping the last pane. **Impact:** a core v0.1.5 feature is partially
unusable on common terminal sizes.

**C2 — Table content overflows with no pagination/scroll.**
`buildTable` sets `table.WithHeight(len(rows)+1)` (`model.go:1006`). A cluster
with 200+ topics or groups pushes rows off-screen; there is no scrollbar,
filter, or paging. **Impact:** the primary navigation surface (Topics/Consumers)
does not scale to real clusters.

**C3 — "q" has two meanings depending on context.**
`q` is global "quit" (`model.go:752`) but inside tail/DLQ/analyze overlays it
means "close the overlay" (`views_tail.go:185`, `views_dlq.go` via
`handleDLQViewKey`, `renderAnalyzeView` help). A user habitually reaching for
`q` risks quitting the app instead of closing a view — or vice versa.

### High

**H1 — Auto-refresh status is duplicated.**
"Auto-refresh: 2s" appears in the header (`model.go:1140`) *and* in the help bar
(`model.go:1529`). Redundant; one belongs in the footer only.

**H2 — Help bar is context-blind and too long.**
The global legend advertises `/`, `a`, `1-6` on every tab, but `a` only works on
tab 6 (`model.go:791`) and `/` only makes sense on table tabs. It wraps/truncates
on narrow terminals (already required a width-clamp fix in the header). Each
overlay duplicates a subset of keys (`esc/q`, `j/k`) inconsistently.

**H3 — "ERROR PATTERN" column always renders "-".**
`fetchDLQ` sets `ErrorPattern: "-"` unconditionally (`views_dlq.go:50`), with a
comment admitting it is not populated. A permanently empty column wastes
horizontal space and signals "broken" to the user. Same for `GROWTH` when no rate
exists.

**H4 — Alerts card is always red, even at 0.**
The overview ALERTS card renders its value in `#EF4444` (red) regardless of
count (`model.go:1215`). With "0 firing" the user sees an alarm color for a
healthy state. Color should encode severity only when non-zero.

**H5 — Search has no match count or "no results" state.**
The footer shows `/ search: orders │ esc: close` (`model.go:1531`) but never how
many rows matched, and a zero-match result falls through to the generic "No data"
row — indistinguishable from "not yet loaded". Case-sensitivity is also not
communicated.

### Medium

**M1 — Overview is redundant and mislabeled.**
The Overview shows a "TOPICS" card (`6 topics`) yet no topics table; a "BROKERS"
card *and* a "BROKERS" section header *and* the broker table. Two "BROKERS"
labels stack vertically (`model.go:1202` + `:1233`). The cards duplicate
information already present in the tables below.

**M2 — Inconsistent empty-state phrasing.**
Analytics uses `no data`, `no anomaly data`, `no rebalance data`, and
`all topics within retention` (`model.go:1340,1443,1478,1427`). Pick one
grammar ("No anomalies detected", "No rebalances in window", …) and use it
everywhere.

**M3 — Tail overlay is too terse.**
`TAIL audit — following` + `no messages` (`views_tail.go:227`) gives no
partition/offset context, no message count, no last-message timestamp. The user
cannot tell if follow is actually consuming.

**M4 — Inactive tab contrast is borderline.**
Inactive tabs use `#6B7280` on `#181425` (~4.3:1); muted text `#6B7280` on the
dark background is used broadly (`helpStyle`, card labels, table cell text).
Below the 4.5:1 AA threshold for small text in places.

**M5 — No modal help (`?`) and no state indicator for refresh.**
Keys are only discoverable via the cramped footer. There is no visible "last
scrape failed" banner beyond the activity log; the `DataUpdated.Failed` flag has
no dedicated UI treatment.

### Low / Polish

**L1 — Case inconsistency in key legends:** `R: replay` on the DLQ table
(`model.go:1289`) vs `r: replay` in the inspect overlay (`model.go:1282`).

**L2 — Emoji as icons.** `⚡`, `📨`, `📈` are used as UI glyphs (header, tail,
growth pane). On many terminal fonts these render as boxes or are inconsistent
with the otherwise text-only design language.

**L3 — No loading skeleton.** On boot the cards show `0 monitored / 0 topics / 0
firing` briefly, then jump. A `loading…` placeholder would avoid the "is it
empty or still connecting?" ambiguity.

---

## 4. Improvement plan

Ordered by impact ÷ effort. Each item lists the target change and the files
touched.

### Phase 1 — Correctness & trust (do first)

1. **Unify "q" semantics (C3).**
   Make `esc` the *only* close for overlays (tail/DLQ/analyze); reserve `q` for
   quit, or introduce a two-state confirm ("q to quit, press again"). Update
   `handleTailViewKey`, `handleDLQViewKey`, `handleAnalyzeViewKey`, and the
   overlay help strings. *Files: `views_tail.go`, `views_dlq.go`, `model.go`.*

2. **Drop or populate "ERROR PATTERN" (H3).**
   Remove the column and the `ErrorPattern` field, or read it from message
   headers during discovery so it is never a constant `-`. *Files:
   `views_dlq.go`, `model.go` (DLQRow + dlqTable).*

3. **Conditional alert color (H4).**
   Render the ALERTS card value in the success color when `len(alerts)==0`,
   red only when firing. *File: `model.go` (`renderOverview`).*

4. **De-duplicate refresh status (H1).**
   Keep "Auto-refresh: 2s" only in the footer; remove it from the header status
   string. *File: `model.go` (`renderHeader`, `renderHelp`).*

### Phase 2 — Scalability of lists & panes

5. **Scrollable content region (C1, C2).**
   Wrap the content area in a `bubbles/viewport` (or give each table a bounded
   height with the viewport scrolling) so Topics/Consumers/Analytics never
   overflow. Reuse the pattern already present in the tail/DLQ overlays.
   *Files: `model.go`, `views_*.go`.*

6. **Pagination / "N of M" for large tables (C2).**
   Add a `Showing N of M` footer for filtered/large tables and `PgUp/PgDn`
   or `j/k` scroll when rows exceed the viewport. *Files: `model.go`
   (`buildTable`, `renderTopicsView`).*

7. **Search match count + explicit no-results (H5).**
   Surface `filteredTopics` length in the footer (`"orders — 2 of 6"`) and a
   distinct "No topics match /orders/" empty state; note case-sensitivity.
   *Files: `model.go` (`renderTopicsView`, `renderHelp`).*

### Phase 3 — Clarity & consistency

8. **Context-aware help bar (H2).**
   Split the footer into (a) global keys (`1-6`, `q`, `r`) and (b) tab-specific
   keys (`/` on tables, `a`/`w` on Analytics, `p` on tail). Add a `?` modal with
   the full legend. *Files: `model.go` (`renderHelp`), new `renderHelpModal`.*

9. **Standardize empty states (M2).**
   Introduce one phrasing pattern and apply across analytics panes and tables.
   *Files: `model.go` (`render*Pane`).*

10. **Rework Overview to remove duplication (M1).**
    Replace the three cards with the broker + consumer tables as the primary
    content (cards already duplicate them), or make the cards clickable
    navigation shortcuts. *File: `model.go` (`renderOverview`).*

11. **Enrich tail overlay (M3).**
    Add partition/offset summary, buffered-message count, and last-message
    timestamp to the `TAIL <topic>` header. *File: `views_tail.go`.*

12. **Improve contrast & glyph consistency (M4, L2).**
    Raise muted-text gray to at least `#9CA3AF`, raise inactive-tab foreground,
    and replace emoji glyphs (`⚡📨📈`) with text or box-drawing markers.
    *Files: `model.go` (styles block), `views_tail.go`.*

### Phase 4 — Polish

13. **Loading skeleton (L3).** Add a `loading…` placeholder to summary cards
    until the first successful scrape. *File: `model.go`.*

14. **Refresh-failure banner.** Use the existing `DataUpdated.Failed` flag to
    show a persistent red "last scrape failed" banner instead of burying it in
    the activity log. *File: `model.go`.*

15. **Unify replay key casing (L1).** Make it `r` everywhere. *Files:
    `model.go`, `views_dlq.go`.*

---

## 5. Acceptance criteria

- [ ] `q` never closes an overlay; `esc` always does (C3).
- [ ] Topics/Consumers/Analytics remain fully reachable at 80×24 (C1/C2).
- [ ] No column renders a constant `-` (H3).
- [ ] 0-firing alerts render in a non-alarm color (H4).
- [ ] Footer shows only one "Auto-refresh" indicator (H1).
- [ ] Search reports match count and a distinct no-results state (H5).
- [ ] Empty-state phrasing is uniform (M2).
- [ ] e2e tapes still pass; add one tape for the `?` help modal and one for a
  large-topic-list scroll (Phase 2/3 verifies the fixes visually).

---

## 6. User stories (backlog)

Persona for all stories: **an SRE / platform engineer operating a Kafka cluster
through the StreamPulse TUI.**

Priorities: `P0` critical · `P1` high · `P2` medium · `P3` low/polish.
Sizes: `XS` ≤1h · `S` 1–2h · `M` 3–4h · `L` 5–8h · `XL` >8h.

| Story | Title | Priority | Size | Effort |
|-------|-------|----------|------|--------|
| SP-01 | Unify overlay "q" key semantics | P0 | S | 1–2h |
| SP-02 | Scrollable content region | P0 | L–XL | 5–9h |
| SP-03 | Conditional alert-card color | P1 | XS | 0.5h |
| SP-04 | Remove dead "ERROR PATTERN" column | P1 | XS | 1h |
| SP-05 | Pagination "N of M" for large tables | P1 | M | 2–3h |
| SP-06 | Search match count + no-results state | P1 | S | 1–2h |
| SP-07 | Context-aware help bar + `?` modal | P2 | M | 3–4h |
| SP-08 | Rework Overview to remove duplication | P2 | M | 2–3h |
| SP-09 | De-duplicate auto-refresh indicator | P2 | XS | 0.5h |
| SP-10 | Enrich tail overlay header | P2 | S | 1–2h |
| SP-11 | Refresh-failure banner | P2 | S | 1–2h |
| SP-12 | Contrast & glyph consistency | P2 | XS | 0.5–1h |
| SP-13 | Loading skeleton on boot | P3 | S | 1h |
| SP-14 | Standardize empty-state phrasing | P3 | XS | 0.5h |
| SP-15 | Unify replay key casing | P3 | XS | 0.25h |

---

### SP-01 — Unify overlay "q" key semantics
- **User story:** As an operator, I want `q` to always quit and `esc` to always
  close, so I never accidentally quit the app while closing a tail/DLQ/analyze
  view.
- **Description:** Today `q` is global quit (`model.go:752`) but inside the tail,
  DLQ, and analyze overlays it also means "close" (`views_tail.go:185` etc.).
  Make `esc` the only close key for overlays and keep `q` reserved for quit.
  Update `handleTailViewKey`, `handleDLQViewKey`, `handleAnalyzeViewKey`, and the
  overlay help strings.
- **Acceptance criteria:**
  - [ ] `q` never closes an overlay; it always quits the app.
  - [ ] `esc` closes tail, DLQ-inspect, and analyze overlays and returns to the parent tab.
  - [ ] Overlay help strings reflect the new keys.
  - [ ] Unit tests cover each overlay key handler.
- **Estimation:** 1–2h (S)
- **Priority:** P0
- **Labels:** `tui` `ux` `correctness`

### SP-02 — Scrollable content region (bounded lists & panes)
- **User story:** As an operator on a small terminal, I want the Topics,
  Consumers, and Analytics views to scroll instead of overflowing, so I can reach
  every row and pane.
- **Description:** Tables use `table.WithHeight(len(rows)+1)` (`model.go:1006`)
  and Analytics emits 6 fixed panes with no viewport, so content falls below the
  fold. Bound each table height to the available space and wrap the content
  region (esp. Analytics) in a `bubbles/viewport`, reusing the tail/DLQ overlay
  pattern. Resolve key routing so the table's `j/k/up/down` and the viewport
  scroll don't fight.
- **Acceptance criteria:**
  - [ ] Topics, Consumers, and Analytics are fully reachable at 80×24.
  - [ ] `j/k` still move row selection on tables; scroll reaches the last row.
  - [ ] Analytics bottom panes (REBALANCES, PATTERNS) are reachable via scroll.
  - [ ] No content overlaps the footer help bar.
- **Estimation:** 5–9h (L–XL)
- **Priority:** P0
- **Labels:** `tui` `scalability` `ux`

### SP-03 — Conditional alert-card color
- **User story:** As an operator, I want the ALERTS summary card to be red only
  when alerts are actually firing, so a healthy cluster doesn't look alarming.
- **Description:** `renderOverview` renders the ALERTS card value in `#EF4444`
  regardless of count (`model.go:1215`). Use the success color when
  `len(alerts)==0`.
- **Acceptance criteria:**
  - [ ] "0 firing" renders in the success color.
  - [ ] Non-zero firing still renders red.
- **Estimation:** 0.5h (XS)
- **Priority:** P1
- **Labels:** `tui` `ux` `a11y`

### SP-04 — Remove dead "ERROR PATTERN" column
- **User story:** As an operator, I don't want a table column that is always
  `-`, so the DLQ table shows only meaningful data.
- **Description:** `fetchDLQ` sets `ErrorPattern: "-"` unconditionally
  (`views_dlq.go:50`). Remove the column and the `DLQRow.ErrorPattern` field, and
  widen remaining columns. (Populating it from message headers is a separate,
  larger story — tracked as a follow-up.)
- **Acceptance criteria:**
  - [ ] No column renders a constant `-`.
  - [ ] DLQ table renders DLQ TOPIC / MESSAGES / GROWTH only.
  - [ ] `views_dlq_test.go` updated.
- **Estimation:** 1h (XS)
- **Priority:** P1
- **Labels:** `tui` `tech-debt` `ux`

### SP-05 — Pagination "N of M" for large tables
- **User story:** As an operator with hundreds of topics, I want to see how many
  rows exist and page through them, so I can navigate large clusters.
- **Description:** Add a `Showing N of M` indicator and `PgUp/PgDn` paging when
  rows exceed the visible height. Depends on SP-02 (bounded heights).
- **Acceptance criteria:**
  - [ ] Footer shows visible/total row count for Topics and Consumers.
  - [ ] `PgUp`/`PgDn` page through large lists.
  - [ ] Count updates after search filtering.
- **Estimation:** 2–3h (M)
- **Priority:** P1
- **Labels:** `tui` `scalability`

### SP-06 — Search match count + explicit no-results state
- **User story:** As an operator, I want search to tell me how many topics match
  and to clearly indicate "no matches", so I can tell an empty filter from an
  empty cluster.
- **Description:** Surface `filteredTopics` length in the footer
  (`"orders — 2 of 6"`) and render a distinct "No topics match /query/" state
  instead of the generic "No data" row. Document case-sensitivity.
- **Acceptance criteria:**
  - [ ] Footer shows match count while searching.
  - [ ] Zero matches shows a distinct message, not "No data".
  - [ ] Case-sensitivity is communicated in the help text.
- **Estimation:** 1–2h (S)
- **Priority:** P1
- **Labels:** `tui` `ux` `discoverability`

### SP-07 — Context-aware help bar + `?` modal
- **User story:** As an operator, I want the footer to show only keys relevant to
  the current view and a `?` modal with the full legend, so I can discover
  commands without scanning a wall of text.
- **Description:** Split the footer into global keys (`1-6`, `q`, `r`) plus
  tab-specific keys (`/` on tables, `a`/`w` on Analytics, `p` on tail). Add a `?`
  key that opens a modal listing all keybindings.
- **Acceptance criteria:**
  - [ ] Footer varies by tab/overlay; no stale keys shown.
  - [ ] `?` opens and `esc` closes the help modal.
  - [ ] `a` is no longer advertised on non-Analytics tabs.
- **Estimation:** 3–4h (M)
- **Priority:** P2
- **Labels:** `tui` `ux` `discoverability`

### SP-08 — Rework Overview to remove duplication
- **User story:** As an operator, I want the Overview to show unique, scannable
  information, so I don't see the same metrics twice.
- **Description:** The Overview repeats "BROKERS" as a card label and a section
  header (`model.go:1202,1233`) and shows a TOPICS card with no topics table.
  Make the broker/consumer tables the primary content and turn the cards into
  navigation shortcuts, or drop them.
- **Acceptance criteria:**
  - [ ] No label is duplicated on the Overview screen.
  - [ ] Cards are actionable navigation or removed.
  - [ ] Overview still surfaces broker, group, and alert counts.
- **Estimation:** 2–3h (M)
- **Priority:** P2
- **Labels:** `tui` `ux`

### SP-09 — De-duplicate auto-refresh indicator
- **User story:** As an operator, I want a single source of truth for refresh
  cadence, so the header and footer stop repeating each other.
- **Description:** "Auto-refresh: 2s" appears in the header (`model.go:1140`) and
  the footer (`model.go:1529`). Keep it only in the footer.
- **Acceptance criteria:**
  - [ ] Exactly one "Auto-refresh" indicator is visible.
- **Estimation:** 0.5h (XS)
- **Priority:** P2
- **Labels:** `tui` `polish`

### SP-10 — Enrich tail overlay header
- **User story:** As an operator tailing a topic, I want partition/offset context,
  message count, and last-message time in the header, so I can confirm the follow
  is actually consuming.
- **Description:** Extend `renderTailView` (`views_tail.go:219`) to show buffered
  message count, per-partition offsets, and last-message timestamp from
  `m.tailMessages`/`m.tailOffsets`.
- **Acceptance criteria:**
  - [ ] Header shows message count and last-message timestamp.
  - [ ] Offset/partition summary is visible.
  - [ ] "following"/"paused" status remains.
- **Estimation:** 1–2h (S)
- **Priority:** P2
- **Labels:** `tui` `ux`

### SP-11 — Refresh-failure banner
- **User story:** As an operator, I want a persistent, visible warning when the
  last scrape failed, so I don't have to dig through the activity log to notice
  stale data.
- **Description:** Use the existing `DataUpdated.Failed` flag to render a red
  "last scrape failed" banner at the top of the content region, instead of
  burying the failure in the activity log.
- **Acceptance criteria:**
  - [ ] A banner appears when the last scrape failed and clears on success.
  - [ ] Banner is visible on every tab.
- **Estimation:** 1–2h (S)
- **Priority:** P2
- **Labels:** `tui` `ux` `observability`

### SP-12 — Contrast & glyph consistency
- **User story:** As an operator, I want readable muted text and consistent
  glyphs, so the UI is legible across terminal fonts and color schemes.
- **Description:** Raise muted-text gray from `#6B7280` to at least `#9CA3AF`,
  raise inactive-tab foreground contrast, and replace emoji glyphs (`⚡📨📈`)
  with text or box-drawing markers.
- **Acceptance criteria:**
  - [ ] Muted text meets 4.5:1 contrast on the dark background.
  - [ ] No emoji used as UI icons.
- **Estimation:** 0.5–1h (XS)
- **Priority:** P2
- **Labels:** `tui` `a11y` `polish`

### SP-13 — Loading skeleton on boot
- **User story:** As an operator, I want a loading placeholder while the first
  scrape runs, so I can tell "empty" from "still connecting".
- **Description:** Show a `loading…` placeholder in the summary cards until the
  first successful scrape populates them.
- **Acceptance criteria:**
  - [ ] Cards show a loading state before the first data update.
  - [ ] Loading state resolves to real counts on first scrape.
- **Estimation:** 1h (S)
- **Priority:** P3
- **Labels:** `tui` `polish`

### SP-14 — Standardize empty-state phrasing
- **User story:** As an operator, I want consistent empty-state wording, so the
  analytics panes read predictably.
- **Description:** Unify `no data`, `no anomaly data`, `no rebalance data`, and
  `all topics within retention` into one phrasing pattern (e.g. "No anomalies
  detected", "No rebalances in window").
- **Acceptance criteria:**
  - [ ] Empty-state phrasing is uniform across panes.
  - [ ] Unit tests assert the wording.
- **Estimation:** 0.5h (XS)
- **Priority:** P3
- **Labels:** `tui` `polish`

### SP-15 — Unify replay key casing
- **User story:** As an operator, I want one canonical replay key, so the DLQ
  table and inspect overlay don't disagree (`R` vs `r`).
- **Description:** Make `r` the replay key everywhere (`model.go:1289` table hint
  vs `:1282` overlay hint).
- **Acceptance criteria:**
  - [ ] Both DLQ table and overlay show `r` for replay.
- **Estimation:** 0.25h (XS)
- **Priority:** P3
- **Labels:** `tui` `polish`
