# CLAUDE.md

Guidance for working in this repo.

## What this is

`anthropic_proxy` is a single-file, async **queueing reverse proxy for LLM APIs**.
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
| `proxy.py` | The entire application (~2000 lines): limiter, metrics, persistence, dashboard HTML/JS, and the proxy handler. |
| `config.yaml` | Runtime config. **Hot-reloaded** (polled every `config_poll_seconds`). Heavily commented — read it for the meaning of every knob. |
| `stats.json` | Disk-persisted long-horizon stats (gitignored). Written by `PersistentStats`. |
| `window.json` | Disk-persisted current rolling-window state — request count + start time (gitignored). Restored on boot unless already elapsed. |
| `statusline.sh` | Polls `/_proxy/statusline` for a compact tmux / Claude Code / zellij / wezterm status bar line. |
| `requirements.txt` | fastapi, httpx, uvicorn, pyyaml. (`proxy.py` also has a PEP 723 `# /// script` header for `uv run`.) |
| `README.md` | User-facing setup + usage docs. |

## How `proxy.py` is organized (where things live)

Everything is in `proxy.py`, in this order. Symbol names below are greppable —
exact line numbers are deliberately omitted since they drift on every edit.

### Config + defaults — top of file
- `DEFAULT_CONFIG`: every config key with its default and an inline explanation.
  Source of truth for what's configurable.
- `RATE_LIMIT_STATUSES = {429, 503, 529}`, `HOP_BY_HOP` headers to strip,
  `METRIC_WINDOWS`.

### Tier + concurrency limiter
- `Tier`: name + `max_concurrent` + its **own rolling request-quota window**
  (`window_seconds` / `window_limit`). LOW defaults to 600 / 5h, HIGH to
  99999 / 1h.
- `Limiter`: the heart of the queue. An `asyncio.Condition`
  guards `in_flight` / `waiters`. Key methods:
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
    `parse_switch_time()` resolves unix seconds / `HH:MM` / ISO-8601 to an
    absolute timestamp; `_next_time_of_day()` + `_local_tod_seconds()` drive the
    daily recurrence.
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
    `discount_request(weight,
    token, lane)` reverses both the total and the lane count when a request
    ultimately fails (so the window counts only quota that was actually
    consumed), no-op if the window has since rolled. `note_request` also adds the
    weight to an **in-flight accumulator** (`_inflight_weight` + lane split);
    `note_done(weight, lane)` — called once per request by the handler's
    `finalize` on any outcome — removes it. `_restart_window` **re-seeds** the
    fresh window from this in-flight tally, so a window restart mid-flight (e.g. a
    probe-driven LOW→HIGH promotion) keeps requests already running counted under
    the new tier instead of zeroing them out of the indicator. The split is
    persisted in `window.json` and restored on boot; the in-flight tally is
    ephemeral (no request survives a restart).
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

### Automation-lane pacing
- `AutoPacer`: paces the `"auto"` lane so it spends only the *leftover* quota.
  `gate()` blocks an auto request until it may proceed; `_usable_and_rate()`
  computes `usable = window_limit − used − safety·human_rate·min(time_left,
  lookahead) − floor` and a target rate `usable/time_left`, capped by
  `max_concurrent/avg_request_time`. The `min(…, lookahead)`
  (`human_demand_lookahead_seconds`) stops a long (e.g. 5h LOW) window from
  reserving nearly all quota off a small human rate. A pending scheduled LOW→HIGH
  switch shortens the horizon — but the capping now lives in the window snapshot
  (`effective_remaining_seconds`), which the pacer reads directly instead of
  re-deriving it from `scheduled_switch_at()`; this keeps the drain rate, the
  human/background projections, and the dashboard countdown all shortened in
  lockstep. So the LOW leftover drains over the shorter horizon. Near window
  end the predicted-human term vanishes so auto can drain ~100%; if humans already spent
  the window, `usable ≤ 0` and auto parks. `gate()` clamps its internal `_next`
  schedule to at most one *current* interval ahead, so a rate jump (window/tier
  change) releases parked requests immediately instead of stranding them behind a
  stale slow schedule. `_parked` counts requests held in `gate()`; `status()`
  exposes `{parked, usable, rate_per_min, next_seconds, reason, count_auto,
  projected_auto}` for the dashboard "Auto Pacing" card + statusline
  (`projected_auto = count_auto + remaining usable budget`). `configure()` is
  re-called on config reload. The human lane never touches the pacer.

### Metrics + usage parsing
- `normalize_usage()`: maps the three provider usage wire formats to one
  canonical 4-field shape (input / output / cache_creation / cache_read).
- `SSEUsageExtractor`: scrapes usage out of streaming SSE bodies (Anthropic
  `message_start`/`message_delta`, OpenAI `response.completed` / final-chunk
  `usage`).
- `JSONUsageExtractor`: buffers a non-streaming JSON body and pulls top-level
  `usage`. `make_extractor()` picks one by content-type.
- `extract_model()`: reads `model` from the request body.
- `compute_cost()`: applies `model_pricing` (per 1M tokens).
- `Metrics`: in-memory rolling window (default 24h) of completions; produces the
  `overall` + `per_model` windowed summaries. `avg_duration()` exposes a cheap
  O(1) EWMA latency that `AutoPacer` reads on every gate.

### Persistent long-horizon stats
- `PersistentStats`: folds completions into **hourly per-model buckets** +
  lifetime totals, flushes to `stats.json` on an interval.
  - `record()`: called from `Metrics.request_finished`.
  - `summary()`: 24h / 7d / 30d / lifetime totals.
  - `series()`: bucketed time series for the dashboard graphs.
  - Cost is computed at read time from current pricing (re-pricing is
    retroactive); percentiles aren't kept long-term (only `duration_sum` → avg).

### Config loading + hot-reload
- Module globals: `config`, `limiter`, `metrics`, `pstats`, `pacer`, `client`.
- `load_config_file()`: merges YAML over `DEFAULT_CONFIG`.
- `init_from_config()`: builds the limiter/metrics/pstats/**pacer** (returns all
  four) and calls `limiter.set_auto_params()`.
- `apply_config_change()`: applies a reloaded config live (incl.
  `limiter.set_auto_params()` + `pacer.configure()`). `upstream_base_url` /
  host / ports need a restart; everything else hot-reloads.
- `config_watch_loop()` + `persist_loop()`: background tasks. `persist_loop`
  also drives `save_window_file()` (window.json).
- `load_window_file()` / `save_window_file()`: window.json read/write helpers;
  the bootstrap block restores window state via `limiter.load_window_state()`.

### HTTP app
- `startup()` / `shutdown()`: create/tear down the shared `httpx.AsyncClient`
  and background tasks **once**, idempotently. Called either by the FastAPI
  `lifespan` (single-server) or directly by `serve()` (dual-port). This split is
  what lets the two ports share one client + one set of loops.
- `compute_backoff()`: honors `Retry-After` (capped by remaining budget) else
  capped exponential backoff.
- `DASHBOARD_HTML`: the **entire dashboard** — self-contained HTML/CSS/JS that
  polls `/_proxy/metrics` (2s) and `/_proxy/series` (15s) and draws SVG charts.
  No build step, no external assets.
  - **Theming**: all colors are CSS variables. Dark is the `:root` default; a
    `@media (prefers-color-scheme: light)` block auto-follows the OS when no
    theme is pinned; `:root[data-theme="light"|"dark"]` are the manual overrides.
    The header **theme button** cycles Auto → Light → Dark, persisted in
    `localStorage` (`theme`). A tiny `<head>` script applies the saved theme
    before first paint (no flash). SVG charts read `--grid` at draw time, so they
    repaint on theme/OS change.

### Endpoints
- `GET /_proxy/` — dashboard. `GET /_proxy/metrics` — full JSON snapshot.
- `GET /_proxy/series` — graph data. `GET /_proxy/status` — limiter snapshot.
- `GET /_proxy/statusline` — compact `plain|tmux|ansi` status line.
- `GET /_proxy/config` — effective config. `POST /_proxy/force_tier`,
  `POST /_proxy/boost` — runtime tier control. `POST /_proxy/schedule_high` /
  `POST /_proxy/schedule_low` (`{"at": <unix|"HH:MM"|ISO|null>}`) — arm/clear the
  **one-shot** scheduled LOW→HIGH / HIGH→LOW switch (the recurring **daily** ones
  are the `scheduled_high_at` / `scheduled_low_at` config keys; all four slots are
  independent).
- `POST /_proxy/window/count` — set the active window's request count
  (`{"count": N}`). `POST /_proxy/window/start` — set/clear its start time
  (`{"started_at": <unix seconds>|null}`). Both persist to `window.json`
  immediately. Example:
  `curl -X POST localhost:8787/_proxy/window/count -d '{"count": 120}'`

### The proxy handler
- `request_lane()`: decides `"human"` vs `"auto"` from the **server port** the
  request arrived on (`throttle_listen_port` ⇒ auto), read from the ASGI scope.
- `proxy()` is the catch-all route for every other path/method. Flow:
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
       up on the dashboard ("Waiting on 429" / statusline `⏳429×N`) instead of
       being invisible (it holds no slot and isn't a `waiter` while backing off).
  4. On success, hand back a `StreamingResponse` whose `body_stream()` tees bytes
     through the usage extractor, then `release_*(was_probe, lane)` and finalizes
     metrics in its `finally`.

`serve()` (run from `__main__` via `asyncio.run`) starts one `uvicorn.Server`
per port (human + auto) with `lifespan="off"`, wrapping them in a single
`startup()`/`shutdown()`. With `throttle_listen_port: null` it's a single port.

## Running

```bash
pip install -r requirements.txt
python proxy.py            # or: uv run proxy.py   (PEP 723 header)
```

Two ports listen by default: the **human lane** on `listen_port` (8787) and the
**automation lane** on `throttle_listen_port` (8788). Point interactive clients
at 8787, scripts at 8788. Open the dashboard at `http://127.0.0.1:8787/_proxy/`.
`CONFIG_PATH` env var overrides the config file location.

## Conventions / things to know when editing

- **Single file, no framework beyond FastAPI.** The dashboard is an inline HTML
  string with vanilla JS — keep it self-contained (no bundler).
- **The event loop is single-threaded**, so `Limiter` / `AutoPacer` counters are
  mutated without locks where comments say "synchronous and await-free" (e.g.
  `note_request`, the window setters); preserve that atomicity. Lane in-flight
  counts + tier switches are mutated under `self._cond`.
- **Two lanes, one shared state.** The lanes differ *only* in admission policy
  (`request_lane` → `pacer.gate()` for auto, `acquire(lane)` priority). Don't add
  per-lane upstream clients or windows — humans + auto share one quota window.
- **Config is hot-reloaded** — new keys should be added to `DEFAULT_CONFIG`, wired
  through `init_from_config` AND `apply_config_change` (and `pacer.configure()` /
  `limiter.set_auto_params()` if pacing-related), and documented in `config.yaml`.
- **Three usage formats** must keep working anywhere usage is parsed
  (`normalize_usage` is the single chokepoint — route new formats through it).
- **Persistent state lives in two gitignored files**: `stats.json` (long-horizon
  history) and `window.json` (current rolling-window count + start). Both are
  written by `persist_loop` (~5s) and on shutdown; deleting them only loses that
  state. `window.json` is discarded on load if its window has already elapsed.

## Planned refactor (single file → package)

`proxy.py` is ~2.1k lines and growing; the next maintainability step is to split
it into a package and lift the dashboard HTML/CSS/JS out of the Python string.
This is a **proposal, not yet done** — the file is still monolithic today.

### Why
- One 2k-line module mixes five concerns (limiter, metrics, persistence,
  dashboard, HTTP). Each is independently testable but currently can't be
  imported without pulling in FastAPI.
- The dashboard is a ~600-line triple-quoted string — no HTML/CSS/JS editor
  support, no linting, awkward diffs.
- Shared mutable module globals (`config`, `limiter`, `metrics`, `pstats`,
  `pacer`, `client`) make the hot-reload + dual-server wiring hard to follow and
  to test.

### Target structure
```
anthropic_proxy/
  pyproject.toml            # deps + console entrypoint (supersedes requirements.txt
                            # and the PEP 723 header; keep a thin proxy.py shim so
                            # `uv run proxy.py` still works)
  config.yaml
  statusline.sh
  anthropic_proxy/
    __init__.py
    __main__.py             # `python -m anthropic_proxy` → serve()
    state.py                # AppState dataclass: holds config/limiter/metrics/
                            # pstats/pacer/client instead of module globals
    config.py               # DEFAULT_CONFIG, load_config_file, make_tier,
                            # parse_window_weights, apply_config_change, watch loop
    limiter.py              # Tier, Limiter (concurrency, tiers, window, lanes,
                            # human-demand tracking)
    pacer.py                # AutoPacer (automation-lane quota pacing)
    usage.py                # normalize_usage, SSE/JSON extractors, make_extractor,
                            # extract_model        (pure, no FastAPI)
    metrics.py              # compute_cost, _stats, Metrics (+ EWMA avg)
    persistence.py          # PersistentStats + window.json load/save
    server.py               # FastAPI app, startup/shutdown, serve() (two ports),
                            # backoff helpers, request_lane, proxy handler
    routes.py               # all /_proxy/* endpoints (incl. window + boost)
    dashboard/
      index.html            # markup (keeps the tiny inline no-flash theme script)
      styles.css
      app.js
  tests/
    test_usage.py  test_limiter.py  test_metrics.py
    test_persistence.py  test_pacer.py
```

### Dashboard separation
- Move `<style>` → `dashboard/styles.css`, the main `<script>` → `dashboard/app.js`,
  markup → `dashboard/index.html` linking `/_proxy/static/...`.
- Serve `dashboard/` via `StaticFiles` (or read the files once at startup). Keep
  the ~8-line no-flash theme script inline in `<head>` (must run before paint).
- Net effect: real editor/linter support for the front-end; Python no longer
  carries a 600-line string.

### The central refactor: kill the module globals
Replace the six module-level globals with a single `AppState` object created in
`startup()` and stored on `app.state`. Routes/handlers read `request.app.state`.
This is the highest-leverage change — it's what makes `config.py`, `limiter.py`,
etc. independently importable and testable, and untangles hot-reload + the
two-server lifecycle.

### Suggested order (each step independently shippable)
1. Extract the **pure** modules first — `usage.py`, `metrics.py`, `limiter.py`,
   `pacer.py`, `persistence.py` — and add `tests/` for them (the stubbed-import
   tests in this session become real unit tests). No behavior change.
2. Extract `config.py` (loading + hot-reload).
3. Introduce `AppState`; convert globals → `app.state`.
4. Split out `routes.py` and `server.py` (incl. `serve()` / `startup` /
   `shutdown` / `request_lane`).
5. Externalize the dashboard into `dashboard/` + `StaticFiles`.
6. Add `pyproject.toml` + `__main__.py`; keep `proxy.py` as a shim importing
   `anthropic_proxy.server:app` so existing `uv run proxy.py` / docs still work.

### Watch out for
- **Atomicity:** `Limiter` / `AutoPacer` await-free counter mutations rely on the
  single-threaded loop — keep them sync when moving them.
- **Two-lane invariants:** one shared upstream client + quota window; lane only
  changes admission policy. `pacer.py` depends on `limiter.py` + `metrics.py`
  (avg latency) — keep that direction acyclic.
- **Hot-reload contract:** new config keys must still flow through
  `DEFAULT_CONFIG` → `config.py` builders → `apply_config_change`
  (+ `pacer.configure` / `limiter.set_auto_params`).
- **Entry-point compatibility:** `serve()` runs two `uvicorn.Server`s under one
  `startup`/`shutdown`; don't reintroduce per-server lifespan double-init. Don't
  break `uv run proxy.py` or the `ANTHROPIC_BASE_URL=…:8787` workflow.
