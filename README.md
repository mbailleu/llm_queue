# anthropic_proxy

A local queueing proxy for LLM APIs. Sits between your client (Claude Code,
opencode, or any Anthropic-/OpenAI-SDK client) and an upstream service that has
unpredictable concurrency and rate limits. Clients fire as many requests as
they want; the proxy queues, retries, and reports.

It forwards every path and header verbatim, so it's API-shape agnostic: the
Anthropic Messages API (`/v1/messages`) and the OpenAI-compatible API
(`/v1/chat/completions`, `/v1/responses`) both go through the **same queue and
the same limiter**, and both get token/cost metrics (the proxy understands
Anthropic, OpenAI Chat Completions, and OpenAI Responses `usage` shapes).

- **Concurrency cap with queue.** Extra requests wait for a slot instead of
  failing. No preemptive rate pacing — requests go out as fast as the cap
  allows, and when upstream rate-limits, the retry below makes callers wait.
- **Two-tier auto-detection.** Discovers whether upstream is currently allowing
  4 or 1000 concurrent requests by sending a speculative "probe" request when
  the lower cap is saturated.
- **Per-tier quota budgets.** Each tier carries a list of rolling quota windows
  — **requests, tokens, and/or cost (USD)** per window — all tracked at once
  (e.g. LOW: 20 req/min + 500k tok/min + $50/min + $30/5h). The most-utilized
  budget is highlighted as "binding", the indicators restart whenever the tier
  switches, window state is persisted across restarts, and each window can be
  set by hand over HTTP. The old single request-window config style still
  works.
- **Auto-retry on `429` / `503` / `529`** with `Retry-After` honored.
- **Hot-reloaded YAML config.** Edit `config.yaml`, save, ~2s later it's live.
- **Web dashboard** with per-model latency, throughput, tokens, and cost, plus a
  **light / dark / auto theme** toggle (auto follows your OS).
- **Persisted stats.** Weekly / monthly / lifetime totals and request/token
  graphs survive restarts (written to disk on an interval, not per request).
- **Statusline endpoint** for tmux / Claude Code status bars.

## Layout

```
anthropic_proxy/  the application package
  limiter.py        concurrency tiers, queue, lanes, quota window
  pacer.py          automation-lane pacing
  usage.py          provider usage parsing (Anthropic + OpenAI shapes)
  metrics.py        rolling metrics + pricing
  persistence.py    stats.json + window.json
  config.py         defaults, loading, hot-reload
  server.py         FastAPI app, proxy handler, dual-port serve()
  routes.py         /_proxy/* endpoints
  dashboard/        index.html / styles.css / app.js
proxy.py          thin shim (PEP 723 inline deps) so `uv run proxy.py` works
config.yaml       all settings; hot-reloaded
statusline.sh     wrapper for Claude Code / tmux status bars
pyproject.toml    package metadata + deps (`pip install -e .`)
tests/            unit tests for the pure modules
stats.json        persisted long-horizon stats (auto-created; git-ignored)
window.json       persisted current quota-window state (auto-created; git-ignored)
```

## Run it

Option A — `uv` (no install needed, deps declared inline in `proxy.py`):

```sh
cd /Users/maurice/projects/anthropic_proxy
uv run proxy.py
```

Option B — pip + venv:

```sh
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
python -m anthropic_proxy        # or: python proxy.py
```

It listens on **two ports** by default: `8787` for the human lane and `8788`
for the throttled automation lane (see [Two lanes](#two-lanes-human-vs-automation)).
Logs go to stderr.

## Point a client at it

The proxy forwards every path and header, so any Anthropic- or OpenAI-compatible
client works — just aim its base URL at the proxy.

**Claude Code** (Anthropic Messages API):

```sh
export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
```

**opencode** (OpenAI-compatible API) — point a custom provider at the proxy in
`~/.config/opencode/opencode.json`:

```json
{
  "provider": {
    "myproxy": {
      "npm": "@ai-sdk/openai-compatible",
      "options": { "baseURL": "http://127.0.0.1:8787/v1" },
      "models": { "gpt-4o": {} }
    }
  }
}
```

(opencode reads the key from the matching env var, e.g. `OPENAI_API_KEY`; the
proxy forwards the `Authorization` header upstream unchanged.) Any other
OpenAI-SDK client works the same way — set `OPENAI_BASE_URL=http://127.0.0.1:8787/v1`.

For a **script or tight loop**, point it at the automation port (`8788`)
instead, so it gets paced and can't exhaust the request budget the human needs:

```sh
ANTHROPIC_BASE_URL=http://127.0.0.1:8788 ./my-batch-job.sh
```

In `config.yaml`, set `upstream_base_url` to whatever your custom service is.
The one upstream can serve both API shapes; the proxy doesn't care which path
the client hits:

```yaml
upstream_base_url: "https://your-custom-service.example.com"
```

## Two lanes (human vs automation)

The proxy listens on two ports that share **one** upstream, queue, tier
auto-detector, and set of quota windows — they differ only in *admission
policy*:

| Lane | Port (default) | Policy |
|---|---|---|
| **human** | `8787` (`listen_port`) | Never throttled. Concurrency **priority** — a human is admitted ahead of any queued automation, and under saturation triggers a HIGH probe rather than waiting. |
| **automation** | `8788` (`throttle_listen_port`) | **Paced** so it spends only the *leftover* request budget, ramping toward 100% as the window ends but never (statistically) starving the human. |

Point interactive tools (Claude Code, opencode with a person driving) at `8787`,
and scripts / tight loops at `8788`. Set `throttle_listen_port: null` to disable
the second lane entirely (single-port mode).

### How the automation lane is paced

Every time an automation request arrives, the pacer evaluates **every quota
budget** of the active tier and admits at the rate of the most constraining
("binding") one. Per budget, in that budget's units (requests, tokens, or $):

```
usable = limit − used − human_demand_safety · human_rate · units_per_request · time_left
                − human_quota_floor                        # requests budgets only
rate   = (usable / units_per_request) / time_left  # spread the leftover evenly
rate   = min(rate over all budgets, free_slots / avg_request_time)  # never outrun the pipe
```

- **`used`** is the whole window's count (human + automation), so the lanes
  compete for one shared budget.
- **`units_per_request`** converts between requests and tokens/$ — a live EWMA
  of what one request actually consumes (before any traffic, the
  `auto_assumed_tokens_per_request` / `auto_assumed_cost_per_request` config
  values apply). That's how "500k tokens left over 40 minutes" becomes a
  request rate.
- **`human_rate`** is the observed human request rate, *averaged over
  `human_demand_horizon_seconds`* so a short human burst isn't extrapolated into
  a giant reservation. `human_demand_safety` (default `1.5`) padding decides how
  much predicted human demand to hold back.
- As a window nears its end, `time_left → 0`, the predicted-human term
  vanishes, and automation is free to **drain up to 100%** of what's left.
  Early on it holds back exactly what humans are statistically expected to need.
- If humans have already spent **any** budget down to that prediction,
  `usable ≤ 0` and automation **parks** until the window advances or resets.
  The dashboard's Auto Pacing card names the budget that currently binds.
- `avg_request_time` (the tracked EWMA latency) and the tier's concurrency cap
  set the physical ceiling, so the pacer never schedules faster than requests
  can actually drain. In the low tier that's `max_concurrent` (4) slots.

Real-time spikes are covered separately: automation never occupies a reserved
slot (`auto_concurrency_reserve`, default 0) and always yields freed slots to
waiting humans, so even a sudden human burst isn't blocked while the slower
quota prediction catches up. All knobs are in
[the config reference](#config-reference) and hot-reload.

## Two-tier model

The proxy assumes upstream allows one of two concurrency caps. Each tier also
carries its own list of rolling **quota budgets** (see below):

| Tier | Max concurrent | Quota budgets (default) |
|------|---:|---|
| `low`  | 4    | 20 req/min · 500k tokens/min · $50/min · **$30 / 5h** |
| `high` | 1000 | 1000 req/min · 500k tokens/min · $50/min · **$100 / 1h** |

The concurrency cap is enforced (requests queue past it). The quota budgets are
**tracked, not enforced** — if upstream rejects you because a quota is spent,
you'll see a `429`, and the retry/backoff (below) is what makes callers wait.

### Quota budget indicators

Each budget is `limit` units of a metric per `window_seconds`:

- **requests** — weighted request count (see per-model weighting below),
  counted when the request is admitted.
- **tokens** — input + output + cache-write + cache-read tokens, counted when
  the response completes.
- **cost** — USD priced via `model_pricing`, counted at completion. **A model
  with no pricing entry contributes $0**, so configure pricing for every model
  you use if you rely on cost budgets.

The proxy doesn't enforce them for interactive traffic, but tracks them all so
the dashboard can show how far you are into each window (the automation lane
*is* paced against the most constraining budget — see the pacing section):

- Every window is **anchored at the first request** sent after the previous one
  expired ("the window starts when the first request goes out"), and each rolls
  **independently** — the 1-minute windows roll hundreds of times inside the 5h
  cost window.
- They're **per tier**: whenever the active tier switches (LOW ⇄ HIGH, by
  auto-detect, boost, or a forced change) all counters **restart** under the
  new tier's budgets.
- The **Current State** panel shows one card per budget — `used / limit` in the
  budget's units, time elapsed/left, per-lane spend and projections, and a
  progress bar; the most-utilized budget is highlighted as **binding** and is
  also what the statusline shows.
- Configure per tier with `tiers.<tier>.limits`, a list of
  `{metric, limit, window_seconds}` entries (see `config.yaml`). **Legacy
  style:** a tier without `limits` uses its `window_seconds` / `window_limit`
  as a single requests budget, falling back to the top-level
  `rate_window_seconds` / `rate_window_limit` — so old request-based configs
  keep working as-is.

**Persisted across restarts.** Every window's count + start time is written to
`window_persist_path` (default `window.json`) every ~5s and on shutdown, then
restored on boot — windows that already elapsed (or whose budget was removed
from config) are discarded and start fresh. A `window.json` from an older
single-window version restores into the requests budget.

**Set them by hand.** Useful after a restart mid-window, or to sync an
indicator with what upstream actually thinks you've used. Optional `metric` /
`window_seconds` select the budget window (count defaults to the requests
window; start applies to all windows when unselected):

```sh
# Set the current count for the requests window
curl -X POST http://127.0.0.1:8787/_proxy/window/count \
     -H 'content-type: application/json' -d '{"count": 120}'

# Set the 5h cost window's spend to $12.50
curl -X POST http://127.0.0.1:8787/_proxy/window/count \
     -H 'content-type: application/json' \
     -d '{"count": 12.5, "metric": "cost", "window_seconds": 18000}'

# Set (or clear with null) window start times, as unix seconds
curl -X POST http://127.0.0.1:8787/_proxy/window/start \
     -H 'content-type: application/json' -d '{"started_at": 1733250000}'
```

### Per-model window weighting

A single request can cost more than one unit toward **requests** budgets (e.g.
an Opus call costing 4× a Haiku call). Set `model_window_weights` (matched by
exact model name, then substring) and `default_window_weight` in `config.yaml`.
Token/cost budgets are unaffected (they measure real consumption), and this
affects **only** the window indicators — per-request metrics and per-model
stats still count every request exactly once.

### Surviving a full window (retry budget)

When upstream rate-limits you because the quota is spent, you usually want the
queued request to **wait out the window and run once the quota resets**, not get
dropped after a few minutes. Retry handling is therefore split:

- **Connection errors** (upstream unreachable) give up after
  `retry_max_attempts` — a down upstream won't be fixed by waiting.
- **Rate-limit responses** (`429` / `503` / `529`) retry against a wall-clock
  budget, `retry_max_elapsed_seconds` (default `18900` = 5h15m, i.e. a bit more
  than the 5h window). A server `Retry-After` is honored in full (up to the
  remaining budget), so a long reset is slept out in one wait instead of
  hammering upstream every `retry_max_delay`.

While a request is backing off it does **not** hold a concurrency slot, so a
pile of waiting requests won't block fresh ones.

> **Client timeout caveat.** The proxy can hold a request for hours, but the
> *client* must keep its connection open that long too. Claude Code and most
> SDKs apply their own request timeout, so in practice a request survives until
> whichever side gives up first. Raise the budget all you like; it can't outlast
> the client.

### How auto-detection works

- Start in `initial_tier` (default `low`).
- When the LOW cap is saturated AND at least one caller is queued AND
  the post-demotion cooldown has elapsed, the proxy lets one extra
  ("probe") request through.
- Probe **succeeds** → upstream is HIGH → promote, new cap is 1000.
- Probe **gets `429`** → stay LOW, reset cooldown.
- Any `429 / 503 / 529` while in HIGH → demote back to LOW.

Probes only fire under load (concurrent requests in flight or queued).
If you never push past 4 concurrent, the proxy stays LOW indefinitely. To force
HIGH:

```sh
curl -X POST http://127.0.0.1:8787/_proxy/force_tier \
     -H 'content-type: application/json' \
     -d '{"tier": "high"}'
```

or set `force_tier: high` in `config.yaml` (picked up within ~2s).

## Endpoints

| Path | Method | What |
|---|---|---|
| `/_proxy/`              | GET  | Web dashboard |
| `/_proxy/metrics`        | GET  | All metrics as JSON (incl. `persistent` weekly/monthly/lifetime) |
| `/_proxy/series`         | GET  | Bucketed time series for graphs (`?window=24h\|7d\|30d\|lifetime`) |
| `/_proxy/status`         | GET  | Limiter snapshot only |
| `/_proxy/config`         | GET  | Currently-loaded config |
| `/_proxy/statusline`     | GET  | One-line text status |
| `/_proxy/force_tier`     | POST | Pin a tier: `{"tier": "low" \| "high" \| null}` |
| `/_proxy/boost`          | POST | Jump to HIGH temporarily; auto-demotes on the next rate-limit |
| `/_proxy/window/count`   | POST | Set the active quota window's count: `{"count": N}` |
| `/_proxy/window/start`   | POST | Set/clear the window start time: `{"started_at": <unix s> \| null}` |
| any other path           | any  | Forwarded to `upstream_base_url` |

## Dashboard

Open <http://127.0.0.1:8787/_proxy/>. Refreshes every 2 seconds.

What's shown:
- **Current State.** Active tier, in-flight / cap, queued count, a **Lanes** card
  (human / automation in-flight, paced-auto backlog, observed human rate), the
  rolling **quota window** (`X / N` requests used, time elapsed / left, with a
  progress bar), and lifetime counters (rate-limited, promotions, demotions,
  probes).
- **Throughput.** Per-window request count, average latency, error badge,
  and total cost (if priced). Windows: 1m / 10m / 1h / 5h / 24h.
- **Overall Latency.** Count, OK, errors, avg, p50, p95 per window.
- **Overall Tokens & Cost.** Input, output, cache-write, cache-read,
  cost per window.
- **Totals (persisted).** 24h / weekly / monthly / lifetime request counts,
  tokens in/out, average latency, and cost — read from the on-disk store, so
  they survive restarts (see [Persisted statistics](#persisted-statistics)).
- **Graphs.** Stacked-bar time series of requests (ok vs. errors) and tokens
  (input / cache / output), with a `24h / 7d / 30d / lifetime` window switch.
  24h and 7d are bucketed hourly; 30d and lifetime are bucketed daily.
- **Per Model.** Two tables (latency + tokens) for every model that's been
  seen — keyed off the `model` field in request bodies.

Header controls:
- **Theme** button cycles **Auto → Light → Dark** (persisted in your browser).
  *Auto* follows your OS `prefers-color-scheme` and flips live when the OS does.
- **⚡ Boost HIGH** temporarily promotes to HIGH (`POST /_proxy/boost`); it
  auto-demotes on the first rate-limit. Disabled while a `force_tier` is pinned.

## Statusline integration

`statusline.sh` hits `/_proxy/statusline`. Pass `tmux`, `ansi`, or `plain` as
the first arg to pick a color format. Falls back to `proxy?` if the proxy is
unreachable. The line reflects the proxy's own state, which is identical no
matter which client drove the traffic — so **one bar covers Claude Code,
opencode, and everything else at once**.

### Claude Code

Add to `~/.claude/settings.json`:

```json
{
  "statusLine": {
    "type": "command",
    "command": "/Users/maurice/projects/anthropic_proxy/statusline.sh"
  }
}
```

### opencode

opencode has no native command-statusline (it's an open feature request), so
surface the proxy line through whatever terminal multiplexer opencode runs
inside. Use the multiplexer recipe below — it shows regardless of whether the
active pane is opencode, Claude Code, or a shell.

### tmux

Add to `~/.tmux.conf` (works for opencode and Claude Code alike):

```tmux
set -g status-interval 2
set -g status-right '#(/Users/maurice/projects/anthropic_proxy/statusline.sh tmux) | %H:%M'
```

### zellij / wezterm / bare terminal

Use the `ansi` format (raw escape codes rather than tmux's `#[fg=...]`):

```sh
/Users/maurice/projects/anthropic_proxy/statusline.sh ansi
```

- **zellij:** run it from a command pane in your layout/status config.
- **wezterm:** poll it from `update-right-status` via `io.popen(... "plain")`
  and apply colors in Lua, or use `ansi` if your status renders escapes.

### Format

```
HIGH 12/1000 q0 1m:42 1.8s
LOW  4/4     q5 1m:1  4.1s !2err probe
```

Fields: tier · in-flight/cap · queue depth · count in window · avg latency · optional error count · `probe` when one is in flight.

Query string options on the endpoint:
- `fmt=plain|tmux|ansi`
- `window=1m|10m|1h|5h|24h`
- `cost=1` to append cost for the chosen window (needs `model_pricing`)

## Token & cost tracking

The proxy parses `usage` blocks from both streaming and non-streaming
responses, and attributes input/output/cache-write/cache-read tokens per model.
It understands three wire formats:

- **Anthropic Messages** — `message_start` + `message_delta` SSE events, or
  top-level `usage` on non-streaming.
- **OpenAI Chat Completions** — top-level `usage` (`prompt_tokens` /
  `completion_tokens` / `prompt_tokens_details.cached_tokens`). For *streaming*
  responses OpenAI only emits usage when the client sets
  `stream_options.include_usage: true`; without it, token counts for that
  request show as 0 (latency/throughput still tracked).
- **OpenAI Responses** — `usage` on `response.completed` (`input_tokens` /
  `output_tokens` / `input_tokens_details.cached_tokens`).

OpenAI's `prompt_tokens` / `input_tokens` are *inclusive* of cached tokens, so
the proxy splits the cached portion out into cache-read to keep the columns
disjoint (and priced correctly). OpenAI has no cache-write charge, so
cache-write stays 0 for those models.

To get dollar amounts, fill in `model_pricing` in `config.yaml`. Rates are
**per 1,000,000 tokens** in whatever unit your provider bills in. If
`cache_creation` / `cache_read` are omitted, they default to the input rate.

```yaml
model_pricing:
  claude-opus-4-7:
    input: 15.0
    output: 75.0
    cache_creation: 18.75
    cache_read: 1.50
  claude-sonnet-4-6:
    input: 3.0
    output: 15.0
```

Models not in `model_pricing` still show token counts; cost just renders as
`—`. Hot-reload applies new prices to all future requests (already-recorded
completions keep the cost they were tagged with at the time).

## Persisted statistics

The rolling-window metrics (1m … 24h, with percentiles) live in memory only.
On top of that, the proxy keeps a **long-horizon aggregated store** so weekly,
monthly, and lifetime totals — plus the dashboard graphs — survive restarts.

- Each completion is folded into an **hourly per-model counter bucket** (count,
  success/errors, the four token columns, and summed duration for an average),
  plus a **running lifetime total** that's never pruned.
- The store is written to `stats_persist_path` (default `stats.json`) on an
  interval (`stats_flush_seconds`, default 60s) — **not on every request** —
  and atomically (`stats.json.tmp` → rename). It's also flushed on shutdown and
  re-read on startup.
- Hourly buckets older than `stats_retention_days` (default 120) are dropped;
  that bounds the graph history and file size. Lifetime totals are unaffected.
- **Cost is not stored** — it's computed at read time from the current
  `model_pricing`, so re-pricing applies retroactively to historical totals.
  Percentiles aren't kept long-term (they can't be merged across buckets); only
  averages are.

Read it via `/_proxy/metrics` (the `persistent` object, keyed `24h` / `7d` /
`30d` / `lifetime`) or graph it via `/_proxy/series?window=…`.

```sh
curl -s http://127.0.0.1:8787/_proxy/metrics | jq .persistent.lifetime.overall
curl -s "http://127.0.0.1:8787/_proxy/series?window=7d" | jq '.points | length'
```

The file is plain JSON and git-ignored. Delete it to reset all long-horizon
history; the proxy recreates it on the next flush.

The current **quota-window** state (count + start) is persisted separately to
`window.json` on the same interval, and restored on boot unless it has already
elapsed — see [Quota window indicator](#quota-window-indicator).

## Config reference

All of `config.yaml` is hot-reloaded except `upstream_base_url`, `listen_host`,
and `listen_port` — those require a restart.

| Key | Default | Notes |
|---|---|---|
| `upstream_base_url` | `https://api.anthropic.com` | Where to forward. |
| `listen_host` / `listen_port` | `127.0.0.1` / `8787` | Human-lane socket. |
| `throttle_listen_port` | `8788` | Automation-lane port; `null` disables the second lane. |
| `auto_pacing_enabled` | `true` | Master switch for automation-lane pacing. |
| `human_demand_safety` | `1.5` | Multiplier on predicted human demand (higher = more headroom, slower auto). |
| `human_demand_horizon_seconds` | `3600` | Window over which the human request rate is averaged. |
| `human_quota_floor` | `0` | Hard floor of requests always kept free for humans (0 = purely statistical). |
| `auto_concurrency_reserve` | `0` | Concurrency slots reserved for humans (auto capped at `max_concurrent −` this). |
| `auto_assumed_request_seconds` | `30.0` | Assumed request time before latency is measured. |
| `auto_assumed_tokens_per_request` | `20000` | Assumed tokens/request before any completion is measured (token-budget pacing). |
| `auto_assumed_cost_per_request` | `0.05` | Assumed USD/request before any completion is measured (cost-budget pacing). |
| `auto_poll_seconds` | `1.0` | How often a parked/over-pace auto request re-checks. |
| `initial_tier` | `low` | `low` or `high`. |
| `force_tier` | `null` | `null` = auto, `"low"` / `"high"` = pin. |
| `tiers.<tier>.max_concurrent` | `4` / `1000` | Concurrency cap for each tier (`low` / `high`). |
| `tiers.<tier>.limits` | new-plan budgets | List of `{metric, limit, window_seconds}` quota budgets; metric is `requests` / `tokens` / `cost`. |
| `tiers.<tier>.window_seconds` | — | **Legacy** quota-window length (used only without `limits`). Falls back to `rate_window_seconds`. |
| `tiers.<tier>.window_limit` | — | **Legacy** requests-per-window (used only without `limits`). Falls back to `rate_window_limit`. |
| `promotion_cooldown_seconds` | `300` | Min seconds between failed probe / demotion and next probe. |
| `retry_max_attempts` | `12` | Max **connection-error** retries per request. |
| `retry_base_delay` / `retry_max_delay` | `1.0` / `60.0` | Exponential backoff bounds. |
| `retry_max_elapsed_seconds` | `18900` | Total time a **rate-limited** request keeps retrying (outlasts the 5h window). |
| `rate_window_seconds` | `18000` | **Legacy fallback** quota-window length for tiers with neither `limits` nor their own window keys. |
| `rate_window_limit` | `600` | **Legacy fallback** requests-per-window for those tiers. |
| `model_window_weights` / `default_window_weight` | `{}` / `1` | Per-model units charged to **requests** budgets (indicator only). |
| `window_persist_path` | `window.json` | File for the persisted current quota-window state. |
| `upstream_timeout` | `600` | Per-request timeout (s). |
| `log_level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR`. |
| `config_poll_seconds` | `2.0` | How often `config.yaml` is checked. |
| `metrics_window_seconds` | `86400` | Rolling-window cap for the completions deque. |
| `model_pricing` | `{}` | Per-million-token rates. |
| `stats_persist_path` | `stats.json` | File for persisted weekly/monthly/lifetime stats + graph history. |
| `stats_flush_seconds` | `60.0` | Min seconds between disk writes (only when there's new data). |
| `stats_retention_days` | `120` | How long hourly graph buckets are kept; lifetime totals are kept forever. |

Override the config file path with `CONFIG_PATH=/some/path/config.yaml`.

## Things to know

- **Concurrency safety on demotion.** When a `429` arrives, the limit is
  halved only once per `promotion_cooldown_seconds` window — so a burst of
  concurrent failures won't crash the cap to `1`.
- **Streaming.** Forwarded chunk-by-chunk via `httpx.aiter_bytes()`. Responses
  are decoded (gzip stripped), and `content-encoding` is stripped on the way
  out — clients receive identity-encoded bodies.
- **Body buffering.** Request bodies are read fully into memory so retries
  work. Fine for `/v1/messages`; if you have a giant request, expect that
  much RAM per in-flight call.
- **Model identification.** Pulled from the JSON request body's `model`
  field. Requests with no parseable body get bucketed as `(unknown)` /
  `(no-body)`.
- **Persistence.** The rolling-window metrics (1m … 24h, with percentiles) are
  in-memory and reset on restart. Weekly / monthly / lifetime totals and the
  graph history are persisted to `stats.json`; the current quota-window state is
  persisted to `window.json`. Both survive restarts — see
  [Persisted statistics](#persisted-statistics).

## Manual control

```sh
# Inspect current state
curl http://127.0.0.1:8787/_proxy/status | jq

# All metrics including per-model
curl http://127.0.0.1:8787/_proxy/metrics | jq

# Persisted weekly / monthly / lifetime totals
curl http://127.0.0.1:8787/_proxy/metrics | jq '.persistent | {weekly: ."7d".overall, monthly: ."30d".overall, lifetime: .lifetime.overall}'

# Pin tier
curl -X POST http://127.0.0.1:8787/_proxy/force_tier \
     -H 'content-type: application/json' -d '{"tier":"high"}'

# Back to auto
curl -X POST http://127.0.0.1:8787/_proxy/force_tier \
     -H 'content-type: application/json' -d '{"tier":null}'

# Temporarily jump to HIGH (auto-demotes on the next rate-limit)
curl -X POST http://127.0.0.1:8787/_proxy/boost

# Sync the quota-window indicator with what upstream has actually used
curl -X POST http://127.0.0.1:8787/_proxy/window/count \
     -H 'content-type: application/json' -d '{"count":120}'
curl -X POST http://127.0.0.1:8787/_proxy/window/start \
     -H 'content-type: application/json' -d '{"started_at":1733250000}'
```
