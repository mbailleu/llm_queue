# CLAUDE.md

Guidance for working in this repo.

## What this is

`anthropic_proxy` is an async **queueing reverse proxy for LLM APIs**.
It sits between a client (Claude Code, opencode, any Anthropic/OpenAI client) and
an upstream LLM endpoint, forwarding every path/header/body verbatim while adding:

- **A shared concurrency queue** with two auto-detected tiers (LOW / HIGH),
  each tracking a set of rolling quota budgets (requests / tokens / cost per
  window — the legacy single request-window config style still works). On HIGH
  the concurrency cap is **searched for** (AIMD on upstream 429s) rather than
  taken from config.
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
| `window.json` | Disk-persisted current rolling-window state — one entry per budget window (gitignored). Restored on boot unless already elapsed. |
| `calibration.json` | Disk-persisted price-calibration snapshots (gitignored). Written by `Calibrator`. |
| `statusline.sh` | Polls `/_proxy/statusline` for a compact tmux / Claude Code / zellij / wezterm status bar line. |
| `pyproject.toml` | Package metadata, deps, `anthropic-proxy` console script, pytest config. |
| `tests/` | Unit tests for the pure modules (`.venv/bin/python -m pytest`). |
| `README.md` | User-facing setup + usage docs. |

## How the package is organized (where things live)

Symbol names below are greppable — exact line numbers are deliberately omitted
since they drift on every edit.

### `state.py` — AppState
One dataclass holding everything the running proxy needs: `config`,
`config_path`/`config_mtime`, `limiter`, `metrics`, `pstats`, `pacer`,
`calibrator`, `gauges`, the
shared httpx `client`, and `bg_tasks`. The FastAPI app stores it at
`app.state.proxy`; routes and the proxy handler read it from there. There are
**no module globals** — this is what makes every other module importable and
testable on its own.

### `config.py` — defaults, loading, hot-reload
- `DEFAULT_CONFIG`: every config key with its default and an inline explanation.
  Source of truth for what's configurable.
- `load_config_file()` (YAML merged over defaults), `make_tier()` +
  `parse_budgets()` (a tier's `limits` list → validated `Budget`s, incl. each
  entry's optional `group`; since a group is one shared window, a member whose
  `window_seconds` disagrees with the group's first-seen length is logged and
  **kept ungrouped** — its quota still needs tracking, just on its own timer.
  Without a `limits` list the legacy per-tier `window_seconds`/`window_limit` —
  falling back to the top-level `rate_window_*` — become a single requests
  budget, so old request-based configs keep working; `limits` wins if both are
  present),
  `parse_window_weights()`, `parse_switch_time()` (unix seconds / `HH:MM` /
  ISO-8601 → absolute timestamp).
- `build_state()`: the bootstrap — builds the limiter/metrics/pstats/pacer,
  arms the daily switches, restores `window.json`, returns the `AppState`.
- `apply_config_change(state, new_cfg)`: applies a reloaded config live (incl.
  `limiter.set_auto_params()` + `limiter.set_assumed_units()` +
  `apply_adaptive_config()` (the `high_adaptive_*` keys; a learned concurrency
  estimate survives, re-clamped to the new bounds) +
  `pacer.configure()` + the shared client's `upstream_timeout`). Only
  `upstream_base_url` / host / the two ports need a restart; everything else
  hot-reloads.
- `config_watch_loop(state)`: background task; also polls
  `limiter.apply_scheduled_switch()` so scheduled tier switches land within
  `config_poll_seconds` of their target time.

### `limiter.py` — tiers, queue, lanes, quota windows
- `Budget`: one quota dimension — `limit` units of `metric` (`"requests"` |
  `"tokens"` | `"cost"`) per `window_seconds`, plus an optional **`group`** name.
  `BudgetWindow`: the live rolling state for one Budget (start / count / human /
  auto lane split) plus `siblings` — the list of windows sharing its timer.
- **Budget groups.** Budgets that name the same `group` share **one timer**: they
  anchor together and roll together. Without a group each window anchors on
  whatever event first touches it (`note_request` for requests, `note_usage` for
  tokens/cost) and re-anchors independently on every roll, so equal-length
  budgets **drift apart** — the group is what keeps "20 req/min + 500k tok/min +
  $50/min" on one shared minute. A group **is** one window, so every member must
  declare the same `window_seconds` (`parse_budgets` enforces this); grouping is
  **opt-in** — an ungrouped budget keeps its own timer exactly as before.
  `_link_groups()` builds the shared `siblings` list, `_anchor_group()` restarts
  all members at once, `_roll_if_elapsed()` rolls a window *and its group*, and
  `_sync_group_starts()` re-establishes one start per group after a config reload
  or a `window.json` restore (a member that joined a running group adopts the
  group's clock rather than sitting dormant). **Invariant: all siblings always
  hold the same `start`.**
- `Tier`: name + `max_concurrent` + its **own list of rolling quota budgets**
  (`budgets`). The new plan defaults: LOW = 20 req/min + 500k tok/min + $50/min
  + $30/5h at 4 concurrent; HIGH = 1000 req/min + 500k tok/min + $50/min +
  $100/1h at 1000 concurrent. The legacy ctor kwargs `window_seconds` /
  `window_limit` still work and compile to one requests budget (the old
  request-based style stays fully supported).
- `local_tod_seconds()` / `next_time_of_day()`: local time-of-day helpers for
  the daily switch recurrence.
- `Limiter`: the heart of the queue. An `asyncio.Condition`
  guards `in_flight` / `waiters`. Read-only `active` / `forced` /
  `effective_max_concurrent` properties expose the current tier and the cap
  actually in force. Key methods:
  - `acquire(lane)`: admits a request for the `"human"` or `"auto"` lane, or
    **probes HIGH** when LOW is saturated and the promotion cooldown has elapsed.
    Returns `was_probe`. **Human priority**: auto is never admitted while a human
    waits, and auto in-flight is capped at `max_concurrent - auto_concurrency_reserve`;
    only humans probe.
  - `release_success` / `release_rate_limited` / `release_other_error` (each
    takes `lane`): release a slot via `_release_slot()` and drive **auto-tier
    switching** — a successful probe promotes LOW→HIGH; a rate-limit on HIGH
    demotes to LOW *unless* adaptive concurrency absorbs it (below).
  - **Adaptive HIGH concurrency (AIMD).** `set_adaptive_params()` (config
    `high_adaptive_*`, hot-reloaded via `apply_adaptive_config`),
    `_shrink_adaptive()` / `_grow_adaptive()`, `_adaptive_estimate()` /
    `_adaptive_floor()` / `_adaptive_ceiling()`, `adaptive_snapshot()`. On HIGH
    the tier's `max_concurrent` is an upper bound: a 429 shrinks the estimate to
    `floor(in_flight_at_the_429 × factor)` — **the observed concurrency, not the
    configured cap**, so a 1000 cap that only ever runs 30 in parallel converges
    in one step — at most once per `cooldown`, and the tier is kept. Successes
    grow it by `step` every `increase_after` completions **that ran saturated**
    (`observed >= cap` or waiters present); unsaturated successes must never
    grow it or an idle lane ratchets back to the ceiling. Only when the estimate
    is at its floor and a further rate-limit arrives (past the cooldown) does
    `release_rate_limited` fall through to the old demotion — and only if
    `demote_at_min`. Applies under `force_tier="high"` too (the cap moves, the
    tier doesn't). **`_restart_window` clears the estimate whenever the active
    tier lands on HIGH**, so a new HIGH stint re-searches instead of arriving at
    the floor where the first 429 demotes it again. LOW is never adapted.
    `effective_max_concurrent` (NOT `active.max_concurrent`) is what `acquire`,
    the pacer's throughput cap, and `snapshot()["max_concurrent"]` read.
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
    boost, or a forced config switch) — rebuilds `_windows` (one `BudgetWindow`
    per active-tier Budget) so every counter + timer re-anchors under the new
    tier's budgets. `_reconcile_windows()` is the same-tier variant used by
    `update_tiers` on config reload: windows whose (metric, window_seconds,
    **group**) survive keep their count/start and pick up a changed limit;
    added/removed budgets are created/dropped, and moving a budget to a
    different group restarts its window. Both end by re-linking groups so the
    shared-start invariant holds.
  - `note_request()` / `note_usage()` / `note_done()` / `discount_request()` /
    `_window_snapshot()` / `window_snapshot()`: track the rolling quota
    **windows** for the *active* tier (dashboard quota cards — tracked, NOT
    enforced for humans). The three metrics land at different times:
    **requests** are counted at admission by `note_request(model, lane)` (per
    `model_window_weights`, requests budgets only; returns `(weight, token)`
    where the token records each requests-window's start); **tokens/cost** land
    at completion via `note_usage(tokens, cost, lane)` — success only, all four
    usage fields summed, cost priced by the caller (unpriced models add $0).
    `note_request` anchors every window (dormant/elapsed ones re-anchor
    independently — a 60s window rolls many times inside a 5h one — **except
    within a group, which anchors and rolls as a unit**); fresh requests windows
    carry the in-flight tally forward, token/cost windows start empty. Each window keeps a human/auto split (invariant: they sum to
    its count). The snapshot exposes a `windows` list (per window: metric,
    limit, group, count, lane split, `utilization`, countdowns, `projected_human` =
    `human + human_rate·units_per_request(metric, "human")·effective_remaining`) plus a
    `binding` index (highest utilization) whose fields are **mirrored at the
    top level** for the statusline/legacy consumers. A pending LOW→HIGH switch
    ends every window early via `effective_remaining_seconds` + `switch_at`
    exactly as before. `discount_request(weight, token, lane)` reverses the
    requests counts for a failed request, skipping windows that rolled since
    (per-window start match); token/cost need no discount (only successes add).
    `note_done(weight, lane)` — called once per request by the handler's
    `finalize` on any outcome — removes the weight from the in-flight
    accumulator that fresh requests windows are re-seeded from. Splits are
    persisted in `window.json` and restored on boot (human re-derived as
    `count - auto`); the in-flight tally is ephemeral.
  - `note_usage` also feeds per-request **token/cost EWMAs** — one blended
    pair plus a pair **per lane** (interactive and automation requests can
    differ in size by orders of magnitude); `units_per_request(metric,
    lane=None)` reports them (1.0 for requests; a lane's own EWMA falls back
    to the blended one, then to the `auto_assumed_tokens_per_request` /
    `auto_assumed_cost_per_request` config fallbacks via `set_assumed_units()`
    before any data). This is how the pacer/projections convert between
    requests and token/$ units without the limiter importing metrics — the
    pacer sizes predicted human demand with the human estimate and converts
    leftovers into auto requests with the auto estimate.
  - `_note_human()` / `human_rate()`: track human arrivals (a `deque` over
    `human_demand_horizon`) and report a horizon-averaged rate for the pacer.
  - `set_window_count()` / `set_window_start()`: manual overrides behind the
    `/_proxy/window/*` endpoints; optional `metric` / `window_seconds` / `group`
    selectors pick the budget window(s) (count defaults to the requests window,
    start to all windows; both return None when nothing matches). Group-aware:
    setting a start applies it to each match **and its siblings**, and touching a
    stale window anchors its whole group — you can't move one member off its
    group's clock.
  - `window_state()` / `load_window_state()`: serialize / restore the windows
    for `window.json` persistence (v2: one entry per anchored window, matched
    back by metric + window_seconds + group; elapsed/unmatched entries are
    dropped; an entry with no `group` key — written before groups existed —
    matches any group, and a legacy single-window file restores into the first
    requests budget). A restore ends with `_sync_group_starts()`, so a budget
    added to a group while the proxy was down adopts its group's clock.
  - `enter_rl_wait()` / `leave_rl_wait()`: bump a **rate-limit retry-backoff
    gauge** (`_rl_waiting` + per-lane), bracketed by the proxy handler around the
    429/503/529 backoff sleep. A request waiting out upstream pushback holds no
    slot and isn't a `waiter`, so without this gauge it's invisible on the
    dashboard while the client is actually waiting. `release_rate_limited` also
    stamps `_last_rate_limited_at` (wall-clock) for an "upstream limiting us"
    indicator. The sleep itself is `rl_backoff_sleep(backoff)`: it waits on a
    wake event that `_restart_window` fires whenever the tier lands on HIGH
    (probe promotion, boost, scheduled switch, forced config), so a request
    honoring a long `Retry-After` from the old LOW window retries immediately
    after a LOW→HIGH switch instead of sleeping out the stale wait. Demotions
    to LOW never wake sleepers (upstream just rate-limited us).
  - `snapshot()`: the JSON state (incl. per-lane in-flight/queued + human rate +
    `rate_limited_waiting` / `last_rate_limited_at`) used by dashboard/statusline.

### `pacer.py` — automation-lane pacing
- `AutoPacer`: paces the `"auto"` lane so it spends only the *leftover* quota.
  `gate()` blocks an auto request until it may proceed. `_evaluate()` scores
  **every budget window** (one `_evaluate_window()` call each, which keeps every
  intermediate term and a `skip` reason of
  `inactive`/`rolling`/`unconvertible`) and the **binding** (slowest) one sets
  the pace: per
  budget, in its own units, `usable_units = limit − used −
  safety·human_rate·units_per_request(human)·min(time_left, lookahead) − floor`
  (floor for requests budgets only), converted back to requests via
  `units_per_request(metric, "auto")` and to a rate `usable_reqs/time_left`;
  the final rate is the min across budgets, capped by
  `max_concurrent/avg_request_time`. The two per-lane estimates matter:
  predicted human demand is sized from what human requests actually consume,
  while the leftover is turned into auto requests at auto's own (often much
  smaller) per-request size. Windows **shorter than
  `auto_pace_min_window_seconds`** (default 300) are **exempt from pacing
  entirely** — they never bind the rate and never park auto, even when fully
  spent; a per-minute limit self-heals in ≤60s, so overruns there are
  throttled by the lane's own mechanisms (the concurrency queue + the
  upstream 429 retry/backoff loop) while pacing lives on the long (e.g. 5h
  cost) budgets. Exempt windows are still evaluated (entry `paces: False`) so
  the dashboard keeps their leftover/projection. Independently, windows
  shorter than `human_reserve_min_window_seconds` (default 300) skip the
  predicted-human term — humans are protected on them by queue priority +
  upstream 429 retries, so the reservation too lives on the long budgets.
  Inactive windows, windows about to roll, and unconvertible ones
  (`units_per_request ≤ 0`, e.g. cost with no pricing — which can't fill
  either) can never bind. `_usable_and_rate()` stays the 2-tuple wrapper. The
  `min(…, lookahead)` (`human_demand_lookahead_seconds`) stops a long (e.g. 5h)
  window from reserving nearly all quota off a small human rate. A pending
  scheduled LOW→HIGH switch shortens every horizon via the snapshot's
  `effective_remaining_seconds`, keeping drain rate, projections, and dashboard
  countdowns in lockstep. Near a window's end the predicted-human term vanishes
  so auto can drain ~100%; if humans already spent any budget, `usable ≤ 0` and
  auto parks. `gate()` clamps its internal `_next` schedule to at most one
  *current* interval ahead, so a rate jump (window/tier change) releases parked
  requests immediately instead of stranding them behind a stale slow schedule.
  Timing uses `time.monotonic()` (not the event-loop clock), so `status()`
  works outside a running loop (e.g. in tests). `_parked` counts requests held
  in `gate()`; `status()` exposes `{parked, usable, rate_per_min, next_seconds,
  reason, count_auto, projected_auto, binding_index, binding_metric,
  binding_window_seconds, explain, windows}` — `windows` aligns by index with
  the limiter snapshot's windows and carries each budget's `usable_units` +
  `projected_auto` in its own units, its `paces`/`binds` flags and its own
  `explain` lines for the dashboard quota cards; top-level `usable` is in
  requests and `projected_auto` follows the snapshot's binding window.
  `configure()` is re-called on config reload. The human lane never touches the
  pacer.
- **`explain()` / `_explain_window()`** render the kept terms as sentences (not
  an aligned table — the dashboard shows them in a native `title` tooltip, which
  uses a proportional font): which budget binds, each term of its arithmetic,
  the throughput cap, and a roll-up of the other budgets. When the
  predicted-human reserve alone is what parks auto, the text says so and names
  `human_demand_safety` / `human_demand_lookahead_seconds` — that combination
  (long window × busy human lane) is the usual cause of "pacing is too
  aggressive". Any new term in the pacing math belongs in the entry dict AND in
  these lines; an explanation that drifts from the code is worse than none.

### `calibrate.py` — per-model price calibration (pure, no FastAPI)
- `Calibrator`: stores snapshots pairing the provider's **cumulative cost
  counters** (uncached input / cached input / output, since the plan changed)
  with the proxy's own cumulative per-model token counters
  (`PersistentStats.lifetime_tokens()`), persisted to `calibration.json`
  (`calibration_persist_path`). Consecutive snapshot pairs become intervals;
  `solve()` estimates per-model $/Mtok prices by least squares — one linear
  system per upstream cost counter, with cache writes as a second unknown
  inside the uncached-input equation (the provider bills them there). Prices
  come back with a confidence: `direct` (an interval isolated that model),
  `regression` (joint solve), or `unidentifiable` (rank-deficient — e.g. two
  models always in the same ratio). Cumulative counters mean the baselines
  cancel in deltas; an interval where any counter went backwards
  (plan/stats reset) is skipped. **If no interval saw a single cache_read
  token** (upstream doesn't report cached-token counts, so the proxy lumps
  cached prompt tokens into `input`), the two input cost counters are summed
  and solved as **one blended input price** (`"blended": true` on the entry, a
  note, and a `# blended` comment in the YAML) instead of underestimating
  `input` by dividing only the uncached cost by all input tokens. Residuals are
  keyed by the cost counters a system consumed, joined with `+`. `solve()` also reports per-counter
  residuals (unexplained cost = traffic bypassing the proxy or a price
  change) and a ready-to-paste `model_pricing_yaml` block. Endpoints:
  `POST /_proxy/calibrate/snapshot`, `GET /_proxy/calibrate/prices`,
  `POST /_proxy/calibrate/reset`.

### `gauges.py` — live queue-gauge history (pure, no FastAPI)
- `GaugeHistory`: a trailing deque of sampled counters for the dashboard's
  "requests in the system" chart — `upstream` (holding a slot), `queued`,
  `backoff` (in 429 retry sleep), `parked` (held by the pacer), and their sum
  as `total`. A gauge has no completion event, so `server.gauge_loop` samples
  it every `gauge_sample_seconds` (bare counter reads via
  `limiter.gauge_counts()` + `pacer.parked` — deliberately not `snapshot()`,
  which does window/schedule math). In memory only: `gauge_history_seconds` of
  history, dropped on restart. `configure()` re-trims on config reload.

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
  `overall` + `per_model` windowed summaries. `cost_of(model, usage)` is the
  public per-request pricing hook the proxy handler uses to feed cost budgets
  (None when the model has no pricing).
- **Two clocks per request** — `request_finished(..., upstream_seconds)` splits
  every duration into the **upstream** half (time a concurrency slot was held,
  measured by the handler) and the **wait** half (`duration - upstream`: pacer
  parking, queueing, 429 backoff). Both feed EWMAs: `avg_duration()` (total,
  client-visible) and **`avg_upstream_time()` — the one `AutoPacer` must use**
  for its `concurrency / service_time` cap. Using the total there creates a
  feedback loop (pacing delay → longer measured duration → lower cap → more
  delay) that one 429 backoff is enough to start. `upstream_seconds=None` means
  "not measured" and counts the whole duration as upstream, so the split
  degrades to the old single number instead of reporting a fake zero wait.
  Summaries expose `avg/p50/p95_seconds` (total, unchanged) plus
  `avg/p50/p95_upstream_seconds` and `avg/p95_wait_seconds`.

### `persistence.py` — stats.json + window.json
- `PersistentStats`: folds completions into **hourly per-model buckets** +
  lifetime totals, flushes to `stats.json` on an interval.
  - `record()`: called from `Metrics.request_finished`; `upstream_sum` tracks
    the slot-hold half of `duration_sum` (see `metrics.py`). `_avg_split()`
    returns None/None for buckets written before that counter existed, so old
    history reads as "unknown", never as "0s upstream, all wait".
  - `summary()`: 24h / 7d / 30d / lifetime totals.
  - `lifetime_tokens()`: cumulative per-model token counters by class
    (model_pricing key names) — the proxy-side half of a calibration snapshot.
  - `series(window, model_limit=8)`: bucketed time series for the dashboard
    graphs, plus a `models` map (per-model `avg_seconds` /
    `avg_upstream_seconds` per bucket, aligned to the shared `points` x-axis)
    for the latency-per-model chart. Buckets where a model ran nothing are
    `null`, never 0 — the chart must draw a gap, not a dive to zero — and only
    the busiest `model_limit` models are returned.
  - Cost is computed at read time from current pricing (re-pricing is
    retroactive); percentiles aren't kept long-term (only `duration_sum` → avg).
- `load_window_file(path)` / `save_window_file(limiter, path)`: window.json
  read/write helpers (atomic temp-file writes; the file is cleared when no
  window is active — i.e. `window_state()["started_at"]` is None, meaning no
  budget window is anchored).

### `server.py` — HTTP app + proxy handler
- `RATE_LIMIT_STATUSES = {429, 503, 529}`, `HOP_BY_HOP` headers to strip.
- `startup(state)` / `shutdown(state)`: create/tear down the shared
  `httpx.AsyncClient` and background tasks **once**, idempotently. Called either
  by the FastAPI `lifespan` (single-server) or directly by `serve()` (dual-port).
  This split is what lets the two ports share one client + one set of loops.
- `persist_loop(state)`: flushes stats + window state every ~5s.
- `gauge_loop(state)`: samples the live queue gauges into `state.gauges` every
  `gauge_sample_seconds` (see `gauges.py`).
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
  3b. **Slot-hold timing.** `slot_started = time.monotonic()` right after
     `acquire()` returns, and `note_slot_released()` at *every* release site
     (connection error, 429, and the `body_stream` finally) accumulates
     `upstream_seconds` across attempts. `metrics.request_finished` gets it as
     the service-time half. Keep a `note_slot_released` call paired with every
     `release_*`: a missed one silently under-reports service time, which
     inflates the pacer's throughput cap.
  4. On success, hand back a `StreamingResponse` whose `body_stream()` tees bytes
     through the usage extractor, then `release_*(was_probe, lane)` and finalizes
     metrics in its `finally`. `finalize` on a success also sums the four usage
     token fields + prices them (`metrics.cost_of`) and calls
     `limiter.note_usage(tokens, cost, lane)` so token/cost budget windows fill
     at completion time.
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
- `GET /_proxy/metrics` — full JSON snapshot. `GET /_proxy/series` — graph data
  (incl. per-model latency). `GET /_proxy/gauges` — sampled requests-in-system
  history. `GET /_proxy/status` — limiter snapshot.
- `GET /_proxy/statusline` — compact `plain|tmux|ansi` status line.
- `GET /_proxy/config` — effective config. `POST /_proxy/force_tier`,
  `POST /_proxy/boost` — runtime tier control. `POST /_proxy/schedule_high` /
  `POST /_proxy/schedule_low` (`{"at": <unix|"HH:MM"|ISO|null>}`) — arm/clear the
  **one-shot** scheduled LOW→HIGH / HIGH→LOW switch (the recurring **daily** ones
  are the `scheduled_high_at` / `scheduled_low_at` config keys; all four slots are
  independent). The two share `_set_oneshot_switch()`.
- `POST /_proxy/pacer/release` — `pacer.release_all()`: one free pass per
  request currently parked in the gate (the rate itself is untouched).
  `POST /_proxy/pacer/enabled` (`{"enabled": bool}`, omit to toggle) — the
  throttle switch; goes through `apply_config_change` on
  `auto_pacing_enabled`, so it behaves exactly like editing the config (and a
  later file edit overrides it).
- `POST /_proxy/calibrate/snapshot` (the provider's three cumulative cost
  counters) / `GET /_proxy/calibrate/prices` / `POST /_proxy/calibrate/reset`
  — per-model price calibration (see `calibrate.py`).
- `POST /_proxy/window/count` — set one budget window's count (`{"count": N}`
  plus optional `"metric"` / `"window_seconds"` / `"group"` selectors; default:
  the requests window). `POST /_proxy/window/start` — set/clear start times
  (`{"started_at": <unix seconds>|null}`, optional selectors; default: all
  windows, null = full restart). Both 404 when nothing matches and persist to
  `window.json` immediately. A `"group"` selector addresses a whole shared timer;
  a start always moves every sibling with it. Examples:
  `curl -X POST localhost:8787/_proxy/window/count -d '{"count": 12.5, "metric": "cost", "window_seconds": 18000}'`
  `curl -X POST localhost:8787/_proxy/window/start -d '{"started_at": null, "group": "minute"}'`

### `dashboard/` — index.html, styles.css, app.js
Self-contained vanilla HTML/CSS/JS, no build step. `app.js` polls
`/_proxy/metrics` (2s), `/_proxy/series` (15s) and `/_proxy/gauges` (4s), and
draws SVG charts: `drawChart()` for stacked bars, `drawLines()` for the
latency-per-model and requests-in-system charts (a `null` value **breaks** the
polyline instead of plotting 0, which is what "no data in that bucket" must
look like; per-model colors come from `MODEL_COLORS` indexed by sorted
position, so a model keeps its color between polls). The
files are read per request, so dashboard edits show up on browser reload
without restarting the proxy. The state grid renders **one quota card per
budget window** (from `limiter.window.windows`; a window's `group` is appended
to its card label, so members of one shared timer are visibly a set) with
per-lane spend + projections
(`projected_human` from
the limiter snapshot, `projected_auto` from `pacer.windows[i]`, index-aligned);
a one-entry list is synthesized from the top-level mirror if a stale poll ever
lacks `windows`.
- **`.stat.binding` = the budget that sets the auto pace** (`pacer.binding_index`),
  *not* the most-utilized window — a per-minute budget that can't pace must never
  be highlighted (it's labelled `not paced` instead, from `pacer.windows[i].paces`).
  Only with no pacer window data at all (pacing off) does it fall back to the
  limiter snapshot's utilization mirror.
- The header carries the pacer controls: **⏭ Release all** (shows the parked
  count, disabled at 0) and the **🐢 Throttled / 🐇 Unthrottled** switch, which
  renders the pacer's *state* (`.on` / `.off` classes) rather than the action.
- Latency tables and throughput cards show `Upstream` / `Wait` next to the
  total (`avg_upstream_seconds` / `avg_wait_seconds`); a wait larger than the
  upstream half is highlighted, since that means the proxy — not the API — is
  what the caller is waiting on.
- **Hover-to-explain**: cards carrying a server-built derivation get
  `class="has-why"` + a `title` attribute (`pacer.explain`,
  `pacer.windows[i].explain`, and the adaptive-concurrency text on In Flight).
  Because `#state-grid` is re-rendered wholesale every poll — which would destroy
  the hovered element and its tooltip — `tick()` **skips the grid update while
  `.has-why:hover` matches**. Keep that guard when touching the render path, and
  run everything server-built through `esc()`.
The header's **⚖ Calibrate** button opens a native `<dialog>`
(price-calibration form → `POST /_proxy/calibrate/snapshot`, estimates table +
paste-ready YAML from `GET /_proxy/calibrate/prices`, two-step reset button —
no blocking `confirm()`).
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
- **Keep the module graph acyclic**: `usage`/`metrics`/`limiter`/`calibrate`/
  `gauges` are leaves; `pacer` depends on `limiter` + `metrics`; `persistence` on `limiter` +
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
- **Persistent state lives in three gitignored files**: `stats.json`
  (long-horizon history) and `window.json` (current rolling-window state) are
  written by `persist_loop` (~5s) and on shutdown; `calibration.json`
  (price-calibration snapshots) is written on each snapshot add. Deleting them
  only loses that state. `window.json` entries are discarded on load if their
  window has already elapsed.
- **Entry-point compatibility:** `serve()` runs two `uvicorn.Server`s under one
  `startup`/`shutdown`; don't reintroduce per-server lifespan double-init. Don't
  break `uv run proxy.py` / `uvicorn proxy:app` (the shim) or the
  `ANTHROPIC_BASE_URL=…:8787` workflow.
- **Add tests** for new behavior in the pure modules under `tests/` — they run
  without a server or network.
