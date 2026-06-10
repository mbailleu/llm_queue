# CLAUDE.md

Guidance for working in this repo.

## What this is

`anthropic_proxy` is an async **queueing reverse proxy for LLM APIs**.
It sits between a client (Claude Code, opencode, any Anthropic/OpenAI client) and
an upstream LLM endpoint, forwarding every path/header/body verbatim while adding:

- **A shared concurrency queue** with two auto-detected tiers (LOW / HIGH).
- **Retry + backoff** on upstream rate limits (429/503/529), so callers wait
  instead of failing.
- **Token / cost metrics** that understand Anthropic Messages, OpenAI Chat
  Completions, and OpenAI Responses usage shapes.
- **A live web dashboard** and **statusline** for observability.

The same upstream can serve both `/v1/messages` (Anthropic) and
`/v1/chat/completions` + `/v1/responses` (OpenAI-compatible) — all traffic flows
through one queue.

There is **no preemptive rate pacing**: requests are admitted as fast as the
concurrency cap allows; the upstream's own 429s + the retry/backoff loop are what
actually pace the client.

## Files

| File | Purpose |
|------|---------|
| `anthropic_proxy/` | The application package — module map below. |
| `proxy.py` | Thin shim (keeps the PEP 723 `# /// script` header) re-exporting `app`/`main` from `anthropic_proxy.server`, so `uv run proxy.py` and `uvicorn proxy:app` still work. |
| `config.yaml` | Runtime config. **Hot-reloaded** (polled every `config_poll_seconds`). Heavily commented — read it for the meaning of every knob. |
| `stats.json` | Disk-persisted long-horizon stats (gitignored). Written by `PersistentStats`. |
| `window.json` | Disk-persisted current rolling-window state — request count + start time (gitignored). Restored on boot unless already elapsed. |
| `statusline.sh` | Polls `/_proxy/statusline` for a compact tmux / Claude Code / zellij / wezterm status bar line. |
| `pyproject.toml` | Package metadata, deps, `anthropic-proxy` console script, pytest config. |
| `tests/` | Unit tests for the pure modules (`.venv/bin/python -m pytest`). |
| `README.md` | User-facing setup + usage docs. |

## How the package is organized (where things live)

Symbol names below are greppable — exact line numbers are deliberately omitted
since they drift on every edit.

### `state.py` — AppState
One dataclass holding everything the running proxy needs: `config`,
`config_path`/`config_mtime`, `limiter`, `metrics`, `pstats`, `pacer`, the
shared httpx `client`, and `bg_tasks`. The FastAPI app stores it at
`app.state.proxy`; routes and the proxy handler read it from there. There are
**no module globals** — this is what makes every other module importable and
testable on its own.

### `config.py` — defaults, loading, hot-reload
- `DEFAULT_CONFIG`: every config key with its default and an inline explanation.
  Source of truth for what's configurable.
- `load_config_file()` (YAML merged over defaults), `make_tier()`,
  `parse_window_weights()`, `parse_switch_time()` (unix seconds / `HH:MM` /
  ISO-8601 → absolute timestamp).
- `build_state()`: the bootstrap — builds the limiter/metrics/pstats/pacer,
  arms the daily switches, restores `window.json`, returns the `AppState`.
- `apply_config_change(state, new_cfg)`: applies a reloaded config live (incl.
  `limiter.set_auto_params()` + `pacer.configure()` + the shared client's
  `upstream_timeout`). Only `upstream_base_url` / host / the two ports need a
  restart; everything else hot-reloads.
- `config_watch_loop(state)`: background task; also polls
  `limiter.apply_scheduled_switch()` so scheduled tier switches land within
  `config_poll_seconds` of their target time.

### `limiter.py` — tiers, queue, lanes, quota window
- `Tier`: name + `max_concurrent` + its **own rolling request-quota window**
  (`window_seconds` / `window_limit`). LOW defaults to 600 / 5h, HIGH to
  99999 / 1h.
- `local_tod_seconds()` / `next_time_of_day()`: local time-of-day helpers for
  the daily switch recurrence.
- `Limiter`: the heart of the queue. An `asyncio.Condition`
  guards `in_flight` / `waiters`. Read-only `active` / `forced` properties
  expose the current tier. Key methods:
  - `acquire(lane)`: admits a request for the `"human"` or `"auto"` lane, or
    **probes HIGH** when LOW is saturated and the promotion cooldown has elapsed.
    Returns `was_probe`. **Human priority**: auto is never admitted while a human
    waits, and auto in-flight is capped at `max_concurrent - auto_concurrency_reserve`;
    only humans probe.
  - `release_success` / `release_rate_limited` / `release_other_error` (each
    takes `lane`): release a slot via `_release_slot()` and drive **auto-tier
    switching** — a successful probe promotes LOW→HIGH; any rate-limit demotes
    HIGH→LOW.
  - `boost_high()`: manual temporary jump to HIGH (auto-demotes on the next
    rate-limit). Distinct from `force_tier` which pins a tier.
  - `set_daily_switch()` / `set_oneshot_switch()` (LOW→HIGH) +
    `set_daily_low_switch()` / `set_oneshot_low_switch()` (HIGH→LOW) /
    `scheduled_switch_at()` / `scheduled_low_switch_at()` /
    `apply_scheduled_switch()` / `schedule_snapshot()`: scheduled tier switches.
    **Each direction** has two independent slots — a **recurring daily** one from
    config (`scheduled_high_at` / `scheduled_low_at`, fires at a local
    time-of-day every day, re-arming itself) and a **one-shot** one from the API
    (`POST /_proxy/schedule_high` / `POST /_proxy/schedule_low`, fires once then
    clears). `scheduled_switch_at()` / `scheduled_low_switch_at()` each return the
    earlier of their two slots. The background `config_watch_loop` polls
    `apply_scheduled_switch()` which, once any slot is due (and not blocked by
    `force_tier`), switches the tier + restarts the window (re-arming daily /
    clearing one-shot); if both directions fire in the same tick the later-scheduled
    one wins. Before a LOW→HIGH switch fires the pacer reads `scheduled_switch_at`
    to end the current LOW window early (the HIGH→LOW slot does not feed the pacer).
  - `_restart_window()`: called on **every tier change** (promotion, demotion,
    boost, or a forced config switch) — resets the count + timer so the window
    re-anchors under the new active tier's limit/duration.
  - `note_request()` / `note_done()` / `discount_request()` /
    `_window_snapshot()` / `window_snapshot()`: track the rolling request-quota
    **window** for the *active* tier (dashboard "X / N this window" indicator —
    tracked, NOT enforced), including per-model `model_window_weights`.
    `note_request(model, lane)` returns `(weight, window_token)` and also folds
    the weight into a per-lane split (`_window_human_count` /
    `_window_auto_count`, invariant: they sum to `_window_count`); the snapshot
    exposes `count_human` / `count_auto` plus a `projected_human` end-of-window
    estimate (`count_human + human_rate·effective_remaining`). A pending LOW→HIGH
    switch ends the window early, so the snapshot also exposes
    `effective_remaining_seconds` (= `remaining_seconds` capped at the switch
    time, only while LOW; `remaining_seconds` stays the true window remaining) +
    `switch_at`, and projects humans over that shorter horizon. This is the single
    source of truth the pacer and dashboard both read, so the countdown, the
    human/auto projections, and the drain rate all shorten together.
    `discount_request(weight, token, lane)` reverses both the total and the lane
    count when a request ultimately fails (so the window counts only quota that
    was actually consumed), no-op if the window has since rolled. `note_request`
    also adds the weight to an **in-flight accumulator** (`_inflight_weight` +
    lane split); `note_done(weight, lane)` — called once per request by the
    handler's `finalize` on any outcome — removes it. `_restart_window`
    **re-seeds** the fresh window from this in-flight tally, so a window restart
    mid-flight (e.g. a probe-driven LOW→HIGH promotion) keeps requests already
    running counted under the new tier instead of zeroing them out of the
    indicator. The split is persisted in `window.json` and restored on boot
    (the human share is re-derived as `count - auto` so the invariant holds even
    for a hand-edited file); the in-flight tally is ephemeral (no request
    survives a restart).
  - `_note_human()` / `human_rate()`: track human arrivals (a `deque` over
    `human_demand_horizon`) and report a horizon-averaged rate for the pacer.
  - `set_window_count()` / `set_window_start()`: manual overrides behind the
    `/_proxy/window/*` endpoints.
  - `window_state()` / `load_window_state()`: serialize / restore the window for
    `window.json` persistence; a restored window is discarded if already elapsed.
  - `enter_rl_wait()` / `leave_rl_wait()`: bump a **rate-limit retry-backoff
    gauge** (`_rl_waiting` + per-lane), bracketed by the proxy handler around the
    429/503/529 backoff sleep. A request waiting out upstream pushback holds no
    slot and isn't a `waiter`, so without this gauge it's invisible on the
    dashboard while the client is actually waiting. `release_rate_limited` also
    stamps `_last_rate_limited_at` (wall-clock) for an "upstream limiting us"
    indicator.
  - `snapshot()`: the JSON state (incl. per-lane in-flight/queued + human rate +
    `rate_limited_waiting` / `last_rate_limited_at`) used by dashboard/statusline.

### `pacer.py` — automation-lane pacing
- `AutoPacer`: paces the `"auto"` lane so it spends only the *leftover* quota.
  `gate()` blocks an auto request until it may proceed; `_usable_and_rate()`
  computes `usable = window_limit − used − safety·human_rate·min(time_left,
  lookahead) − floor` and a target rate `usable/time_left`, capped by
  `max_concurrent/avg_request_time`. The `min(…, lookahead)`
  (`human_demand_lookahead_seconds`) stops a long (e.g. 5h LOW) window from
  reserving nearly all quota off a small human rate. A pending scheduled LOW→HIGH
  switch shortens the horizon — the capping lives in the window snapshot
  (`effective_remaining_seconds`), which the pacer reads directly; this keeps
  the drain rate, the human/background projections, and the dashboard countdown
  all shortened in lockstep. So the LOW leftover drains over the shorter
  horizon. Near window end the predicted-human term vanishes so auto can drain
  ~100%; if humans already spent the window, `usable ≤ 0` and auto parks.
  `gate()` clamps its internal `_next` schedule to at most one *current*
  interval ahead, so a rate jump (window/tier change) releases parked requests
  immediately instead of stranding them behind a stale slow schedule. Timing
  uses `time.monotonic()` (not the event-loop clock), so `status()` works
  outside a running loop (e.g. in tests). `_parked` counts requests held in
  `gate()`; `status()` exposes `{parked, usable, rate_per_min, next_seconds,
  reason, count_auto, projected_auto}` for the dashboard "Auto Pacing" card +
  statusline (`projected_auto = count_auto + remaining usable budget`).
  `configure()` is re-called on config reload. The human lane never touches the
  pacer.

### `usage.py` — provider usage parsing (pure, no FastAPI)
- `normalize_usage()`: maps the three provider usage wire formats to one
  canonical 4-field shape (input / output / cache_creation / cache_read).
- `SSEUsageExtractor`: scrapes usage out of streaming SSE bodies (Anthropic
  `message_start`/`message_delta`, OpenAI `response.completed` / final-chunk
  `usage`).
- `JSONUsageExtractor`: buffers a non-streaming JSON body and pulls top-level
  `usage`. `make_extractor()` picks one by content-type.
- `extract_model()`: reads `model` from the request body.

### `metrics.py` — rolling metrics + pricing
- `compute_cost()`: applies `model_pricing` (per 1M tokens).
- `METRIC_WINDOWS`: the 1m/10m/1h/5h/24h dashboard windows.
- `Metrics`: in-memory rolling window (default 24h) of completions; produces the
  `overall` + `per_model` windowed summaries. `avg_duration()` exposes a cheap
  O(1) EWMA latency that `AutoPacer` reads on every gate.

### `persistence.py` — stats.json + window.json
- `PersistentStats`: folds completions into **hourly per-model buckets** +
  lifetime totals, flushes to `stats.json` on an interval.
  - `record()`: called from `Metrics.request_finished`.
  - `summary()`: 24h / 7d / 30d / lifetime totals.
  - `series()`: bucketed time series for the dashboard graphs.
  - Cost is computed at read time from current pricing (re-pricing is
    retroactive); percentiles aren't kept long-term (only `duration_sum` → avg).
- `load_window_file(path)` / `save_window_file(limiter, path)`: window.json
  read/write helpers (atomic temp-file writes; the file is cleared when no
  window is active).

### `server.py` — HTTP app + proxy handler
- `RATE_LIMIT_STATUSES = {429, 503, 529}`, `HOP_BY_HOP` headers to strip.
- `startup(state)` / `shutdown(state)`: create/tear down the shared
  `httpx.AsyncClient` and background tasks **once**, idempotently. Called either
  by the FastAPI `lifespan` (single-server) or directly by `serve()` (dual-port).
  This split is what lets the two ports share one client + one set of loops.
- `persist_loop(state)`: flushes stats + window state every ~5s.
- `compute_backoff(cfg, …)`: honors `Retry-After` (capped by remaining budget)
  else capped exponential backoff.
- `request_lane(cfg, request)`: decides `"human"` vs `"auto"` from the **server
  port** the request arrived on (`throttle_listen_port` ⇒ auto), read from the
  ASGI scope.
- `handle_proxy(state, path, request)` — the catch-all proxy flow:
  1. Read body, strip hop-by-hop headers, extract model, determine lane, start
     metrics.
  2. **Auto lane only:** `await pacer.gate()` before committing (holds no slot
     while parked); then `limiter.note_request()` (keeps its `(weight, token)` so
     `finalize` can `discount_request` the window count on a failed outcome).
  3. Loop: `limiter.acquire(lane)` → stream upstream via the shared client.
     - **Connection errors** retry up to `retry_max_attempts`.
     - **429/503/529** retry against a wall-clock budget
       (`retry_max_elapsed_seconds`) so a queued request can outlast a full quota
       window and run once quota resets. The backoff sleep is bracketed by
       `limiter.enter_rl_wait()` / `leave_rl_wait()` so the parked request shows
       up on the dashboard ("Waiting on 429" / statusline `⏳429×N`).
  4. On success, hand back a `StreamingResponse` whose `body_stream()` tees bytes
     through the usage extractor, then `release_*(was_probe, lane)` and finalizes
     metrics in its `finally`.
- `create_app(state)`: builds the FastAPI app — includes the `/_proxy/*` router
  **before** registering the catch-all proxy route, stores the state at
  `app.state.proxy`.
- Module level: `state = build_state()` + `app = create_app(state)` (so
  `uvicorn anthropic_proxy.server:app` / `uvicorn proxy:app` work), `serve()`
  (one `uvicorn.Server` per port with `lifespan="off"`, wrapped in a single
  `startup()`/`shutdown()`), and `main()` (the console-script entry).

### `routes.py` — `/_proxy/*` endpoints
All endpoints read the `AppState` via `request.app.state.proxy`.
- `GET /_proxy/` — dashboard (serves `dashboard/index.html`).
  `GET /_proxy/static/{styles.css,app.js}` — fixed allowlist, no traversal.
- `GET /_proxy/metrics` — full JSON snapshot. `GET /_proxy/series` — graph data.
  `GET /_proxy/status` — limiter snapshot.
- `GET /_proxy/statusline` — compact `plain|tmux|ansi` status line.
- `GET /_proxy/config` — effective config. `POST /_proxy/force_tier`,
  `POST /_proxy/boost` — runtime tier control. `POST /_proxy/schedule_high` /
  `POST /_proxy/schedule_low` (`{"at": <unix|"HH:MM"|ISO|null>}`) — arm/clear the
  **one-shot** scheduled LOW→HIGH / HIGH→LOW switch (the recurring **daily** ones
  are the `scheduled_high_at` / `scheduled_low_at` config keys; all four slots are
  independent). The two share `_set_oneshot_switch()`.
- `POST /_proxy/window/count` — set the active window's request count
  (`{"count": N}`). `POST /_proxy/window/start` — set/clear its start time
  (`{"started_at": <unix seconds>|null}`). Both persist to `window.json`
  immediately. Example:
  `curl -X POST localhost:8787/_proxy/window/count -d '{"count": 120}'`

### `dashboard/` — index.html, styles.css, app.js
Self-contained vanilla HTML/CSS/JS, no build step. `app.js` polls
`/_proxy/metrics` (2s) and `/_proxy/series` (15s) and draws SVG charts. The
files are read per request, so dashboard edits show up on browser reload
without restarting the proxy.
- **Layout**: the stat panels are wrapping **flexbox** rows (not CSS grid) —
  every `.stat` card is at least as wide as its content because `.sub` lines
  are `white-space: nowrap`; cards wrap as whole units. Keep sub-lines short
  (the Active Tier card renders pending scheduled switches as one sub-line
  each for this reason).
- **Theming**: all colors are CSS variables. Dark is the `:root` default; a
  `@media (prefers-color-scheme: light)` block auto-follows the OS when no
  theme is pinned; `:root[data-theme="light"|"dark"]` are the manual overrides.
  The header **theme button** cycles Auto → Light → Dark, persisted in
  `localStorage` (`theme`). A tiny inline `<head>` script in `index.html`
  applies the saved theme before first paint (no flash) — it must stay inline.
  SVG charts read `--grid` at draw time, so they repaint on theme/OS change.

## Running

```bash
uv run proxy.py                      # PEP 723 header on the shim
# or
pip install -e . && python -m anthropic_proxy
```

Two ports listen by default: the **human lane** on `listen_port` (8787) and the
**automation lane** on `throttle_listen_port` (8788). Point interactive clients
at 8787, scripts at 8788. Open the dashboard at `http://127.0.0.1:8787/_proxy/`.
`CONFIG_PATH` env var overrides the config file location.

Tests: `python -m pytest` (unit tests for the pure modules; no server needed).

## Conventions / things to know when editing

- **The event loop is single-threaded**, so `Limiter` / `AutoPacer` counters are
  mutated without locks where comments say "synchronous and await-free" (e.g.
  `note_request`, the window setters); preserve that atomicity — keep those
  methods sync. Lane in-flight counts + tier switches are mutated under
  `self._cond`.
- **Two lanes, one shared state.** The lanes differ *only* in admission policy
  (`request_lane` → `pacer.gate()` for auto, `acquire(lane)` priority). Don't add
  per-lane upstream clients or windows — humans + auto share one quota window.
- **Keep the module graph acyclic**: `usage`/`metrics`/`limiter` are leaves;
  `pacer` depends on `limiter` + `metrics`; `persistence` on `limiter` +
  `metrics`; `config` on all of those + `state`; `routes`/`server` at the top.
  No module reads globals — state is passed in (or read from
  `request.app.state.proxy` in routes).
- **Config is hot-reloaded** — new keys must be added to `DEFAULT_CONFIG`
  (`config.py`), wired through `build_state` AND `apply_config_change` (and
  `pacer.configure()` / `limiter.set_auto_params()` if pacing-related), and
  documented in `config.yaml`.
- **Three usage formats** must keep working anywhere usage is parsed
  (`normalize_usage` is the single chokepoint — route new formats through it).
- **The dashboard stays build-step-free** (vanilla JS, no bundler). Static files
  are served from a fixed allowlist in `routes.py`.
- **Persistent state lives in two gitignored files**: `stats.json` (long-horizon
  history) and `window.json` (current rolling-window count + start). Both are
  written by `persist_loop` (~5s) and on shutdown; deleting them only loses that
  state. `window.json` is discarded on load if its window has already elapsed.
- **Entry-point compatibility:** `serve()` runs two `uvicorn.Server`s under one
  `startup`/`shutdown`; don't reintroduce per-server lifespan double-init. Don't
  break `uv run proxy.py` / `uvicorn proxy:app` (the shim) or the
  `ANTHROPIC_BASE_URL=…:8787` workflow.
- **Add tests** for new behavior in the pure modules under `tests/` — they run
  without a server or network.
