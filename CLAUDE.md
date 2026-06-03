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

The code is a Python **package** (`anthropic_proxy/`); `proxy.py` is now just a
thin compatibility shim. See [Package layout](#package-layout) for the module map.

| File | Purpose |
|------|---------|
| `anthropic_proxy/` | The application package (limiter, pacer, metrics, persistence, runtime/config, server, routes, dashboard). |
| `proxy.py` | ~20-line shim: re-exports `app` + `serve` so `python proxy.py` / `uv run proxy.py` / `uvicorn proxy:app` still work (keeps the PEP 723 header). |
| `pyproject.toml` | Package metadata, deps, `anthropic-proxy` console script. |
| `config.yaml` | Runtime config. **Hot-reloaded** (polled every `config_poll_seconds`). Heavily commented — read it for the meaning of every knob. |
| `stats.json` | Disk-persisted long-horizon stats (gitignored). Written by `PersistentStats`. |
| `window.json` | Disk-persisted current rolling-window state — request count + start time (gitignored). Restored on boot unless already elapsed. |
| `statusline.sh` | Polls `/_proxy/statusline` for a compact tmux / Claude Code / zellij / wezterm status bar line. |
| `requirements.txt` | fastapi, httpx, uvicorn, pyyaml. |
| `tests/` | pytest unit suite for the pure modules (usage / limiter / metrics / persistence / pacer). |
| `README.md` | User-facing setup + usage docs. |

## Where things live

The components below were split out of the old single file into the package
modules listed in [Package layout](#package-layout). Symbol names are greppable —
exact line numbers are deliberately omitted since they drift on every edit.

### Config + defaults — `runtime.py`
- `DEFAULT_CONFIG`: every config key with its default and an inline explanation.
  Source of truth for what's configurable.
- `RATE_LIMIT_STATUSES = {429, 503, 529}`, `HOP_BY_HOP` headers to strip,
  `METRIC_WINDOWS`.

### Tier + concurrency limiter — `limiter.py`
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
  - `_restart_window()`: called on **every tier change** (promotion, demotion,
    boost, or a forced config switch) — resets the count + timer so the window
    re-anchors under the new active tier's limit/duration.
  - `note_request()` / `_window_snapshot()` / `window_snapshot()`: track the
    rolling request-quota **window** for the *active* tier (dashboard "X / N this
    window" indicator — tracked, NOT enforced), including per-model
    `model_window_weights`.
  - `_note_human()` / `human_rate()`: track human arrivals (a `deque` over
    `human_demand_horizon`) and report a horizon-averaged rate for the pacer.
  - `set_window_count()` / `set_window_start()`: manual overrides behind the
    `/_proxy/window/*` endpoints.
  - `window_state()` / `load_window_state()`: serialize / restore the window for
    `window.json` persistence; a restored window is discarded if already elapsed.
  - `snapshot()`: the JSON state (incl. per-lane in-flight/queued + human rate)
    used by dashboard/statusline.

### Automation-lane pacing — `pacer.py`
- `AutoPacer`: paces the `"auto"` lane so it spends only the *leftover* quota.
  `gate()` blocks an auto request until it may proceed; `_usable_and_rate()`
  computes `usable = window_limit − used − safety·human_rate·time_left − floor`
  and a target rate `usable/time_left`, capped by `max_concurrent/avg_request_time`.
  Near window end the predicted-human term vanishes so auto can drain ~100%; if
  humans already spent the window, `usable ≤ 0` and auto parks. `configure()` is
  re-called on config reload. The human lane never touches the pacer.

### Metrics + usage parsing — `metrics.py` + `usage.py`
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

### Persistent long-horizon stats — `persistence.py`
- `PersistentStats`: folds completions into **hourly per-model buckets** +
  lifetime totals, flushes to `stats.json` on an interval.
  - `record()`: called from `Metrics.request_finished`.
  - `summary()`: 24h / 7d / 30d / lifetime totals.
  - `series()`: bucketed time series for the dashboard graphs.
  - Cost is computed at read time from current pricing (re-pricing is
    retroactive); percentiles aren't kept long-term (only `duration_sum` → avg).

### Config loading + hot-reload — `runtime.py`
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
  `bootstrap()` populates the runtime state at import and restores window state
  via `limiter.load_window_state()`.

### HTTP app — `server.py`
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

### Endpoints — `routes.py`
- `GET /_proxy/` — dashboard. `GET /_proxy/metrics` — full JSON snapshot.
- `GET /_proxy/series` — graph data. `GET /_proxy/status` — limiter snapshot.
- `GET /_proxy/statusline` — compact `plain|tmux|ansi` status line.
- `GET /_proxy/config` — effective config. `POST /_proxy/force_tier`,
  `POST /_proxy/boost` — runtime tier control.
- `POST /_proxy/window/count` — set the active window's request count
  (`{"count": N}`). `POST /_proxy/window/start` — set/clear its start time
  (`{"started_at": <unix seconds>|null}`). Both persist to `window.json`
  immediately. Example:
  `curl -X POST localhost:8787/_proxy/window/count -d '{"count": 120}'`

### The proxy handler — `server.py`
- `request_lane()`: decides `"human"` vs `"auto"` from the **server port** the
  request arrived on (`throttle_listen_port` ⇒ auto), read from the ASGI scope.
- `proxy()` is the catch-all route for every other path/method. Flow:
  1. Read body, strip hop-by-hop headers, extract model, determine lane, start
     metrics.
  2. **Auto lane only:** `await pacer.gate()` before committing (holds no slot
     while parked); then `limiter.note_request()`.
  3. Loop: `limiter.acquire(lane)` → stream upstream via the shared client.
     - **Connection errors** retry up to `retry_max_attempts`.
     - **429/503/529** retry against a wall-clock budget
       (`retry_max_elapsed_seconds`) so a queued request can outlast a full quota
       window and run once quota resets.
  4. On success, hand back a `StreamingResponse` whose `body_stream()` tees bytes
     through the usage extractor, then `release_*(was_probe, lane)` and finalizes
     metrics in its `finally`.

`serve()` (run from `__main__` via `asyncio.run`) starts one `uvicorn.Server`
per port (human + auto) with `lifespan="off"`, wrapping them in a single
`startup()`/`shutdown()`. With `throttle_listen_port: null` it's a single port.

## Running

```bash
pip install -r requirements.txt
python proxy.py                  # shim — or: uv run proxy.py (PEP 723 header)
python -m anthropic_proxy        # package entrypoint
uvicorn anthropic_proxy:app      # single-port (ASGI app directly)
pytest tests/                    # unit suite for the pure modules
```

Two ports listen by default: the **human lane** on `listen_port` (8787) and the
**automation lane** on `throttle_listen_port` (8788). Point interactive clients
at 8787, scripts at 8788. Open the dashboard at `http://127.0.0.1:8787/_proxy/`.
`CONFIG_PATH` env var overrides the config file location.

## Conventions / things to know when editing

- **Package, no framework beyond FastAPI.** The dashboard is plain
  HTML/CSS/JS under `anthropic_proxy/dashboard/`, served as static files — keep it
  dependency-free (no bundler). Edit `index.html` / `styles.css` / `app.js`
  directly; the no-flash theme `<script>` must stay inline in `<head>`.
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

## Package layout

The old single `proxy.py` was split into the package below (the file is now a
~20-line shim). Dependency direction is acyclic, with the pure modules at the
bottom and the web layer on top.

```
proxy.py                     # compatibility shim (re-exports app + serve)
pyproject.toml               # metadata, deps, `anthropic-proxy` console script
config.yaml  statusline.sh  requirements.txt
anthropic_proxy/
  __init__.py                # lazy `app` / `serve` (pure modules import w/o FastAPI)
  __main__.py                # `python -m anthropic_proxy` → serve()
  _log.py                    # shared logger
  usage.py                   # normalize_usage, SSE/JSON extractors, extract_model  (pure)
  metrics.py                 # compute_cost, _stats, Metrics (+ EWMA avg), METRIC_WINDOWS  (pure)
  persistence.py             # PersistentStats  (pure; depends on metrics.compute_cost)
  limiter.py                 # Tier, Limiter — tiers, window, lanes, human-demand  (pure)
  pacer.py                   # AutoPacer  (pure; reads limiter window + metrics avg)
  runtime.py                 # config + state (config/limiter/metrics/pstats/pacer/client),
                             # load/make_tier/parse_window_weights/init_from_config,
                             # apply_config_change, watch + persist loops, window.json,
                             # RATE_LIMIT_STATUSES/HOP_BY_HOP, bootstrap()
  server.py                  # FastAPI app, startup/shutdown/lifespan, serve() (two ports),
                             # backoff, request_lane, the catch-all proxy handler
  routes.py                  # all /_proxy/* endpoints + dashboard (StaticFiles mount)
  dashboard/                 # index.html + styles.css + app.js (served statically)
tests/                       # pytest unit suite for the pure modules
```

### State ownership
Rather than scattered module globals (or threading `app.state` through every
endpoint), all process-wide mutable state lives on the **`runtime` module**:
`runtime.config`, `runtime.limiter`, `runtime.metrics`, `runtime.pstats`,
`runtime.pacer`, `runtime.client`. `server.py` / `routes.py` read/write
`runtime.<name>`; `runtime.bootstrap()` builds them at import. This is the
singleton-module form of the "AppState" idea — one owner, easy hot-reload
rebinding (the `apply_config_change` / `config_watch_loop` functions live in
`runtime` and keep using `global`).

### Import / registration order (don't break it)
- `server.py` imports `runtime` and calls `bootstrap()`, defines `app`, then
  `from . import routes` (registers the `/_proxy/*` routes + `/_proxy/static`
  mount), and **only then** defines the catch-all `/{full_path:path}` handler —
  so specific routes match before the catch-all.
- `routes.py` does `from .server import app` (circular but safe: `app` already
  exists when routes is imported mid-`server`).
- `__init__.py` resolves `app`/`serve` lazily (PEP 562) so importing the pure
  modules (e.g. in tests) doesn't pull in FastAPI.

### If you extend it
- **Atomicity:** `Limiter` / `AutoPacer` await-free counter mutations rely on the
  single-threaded loop — keep them sync.
- **Two-lane invariants:** one shared upstream client + quota window; lane only
  changes admission policy. Keep `pacer` → `limiter`/`metrics` acyclic.
- **Hot-reload contract:** new config keys flow through `DEFAULT_CONFIG` →
  `runtime` builders → `apply_config_change` (+ `pacer.configure` /
  `limiter.set_auto_params`), and get documented in `config.yaml`.
- **Possible next steps (not done):** promote `runtime` to a real `AppState`
  dataclass on `app.state`; add a `pyproject` optional-deps group for tests; add
  HTTP-level integration tests alongside the unit suite.
