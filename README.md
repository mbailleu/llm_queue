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
- **Auto-retry on `429` / `503` / `529`** with `Retry-After` honored.
- **Hot-reloaded YAML config.** Edit `config.yaml`, save, ~2s later it's live.
- **Web dashboard** with per-model latency, throughput, tokens, and cost.
- **Statusline endpoint** for tmux / Claude Code status bars.

## Layout

```
proxy.py          single-file FastAPI app (PEP 723 inline deps)
config.yaml       all settings; hot-reloaded
statusline.sh     wrapper for Claude Code / tmux status bars
requirements.txt  for pip users
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
pip install -r requirements.txt
python proxy.py
```

It listens on `http://127.0.0.1:8787` by default. Logs go to stderr.

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

In `config.yaml`, set `upstream_base_url` to whatever your custom service is.
The one upstream can serve both API shapes; the proxy doesn't care which path
the client hits:

```yaml
upstream_base_url: "https://your-custom-service.example.com"
```

## Two-tier model

The proxy assumes upstream allows one of two concurrency caps:

| Tier | Max concurrent |
|------|---:|
| `low`  | 4    |
| `high` | 1000 |

Each tier is purely a concurrency cap — there is no per-window request cap or
preemptive pacing. If upstream enforces a rolling request quota, you'll hit it
as a `429`, and the retry/backoff (below) is what makes callers wait.

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
| `/_proxy/metrics`        | GET  | All metrics as JSON |
| `/_proxy/status`         | GET  | Limiter snapshot only |
| `/_proxy/config`         | GET  | Currently-loaded config |
| `/_proxy/statusline`     | GET  | One-line text status |
| `/_proxy/force_tier`     | POST | `{"tier": "low" \| "high" \| null}` |
| any other path           | any  | Forwarded to `upstream_base_url` |

## Dashboard

Open <http://127.0.0.1:8787/_proxy/>. Refreshes every 2 seconds.

What's shown:
- **Current State.** Active tier, in-flight / cap, queued count, and lifetime
  counters (rate-limited, promotions, demotions, probes).
- **Throughput.** Per-window request count, average latency, error badge,
  and total cost (if priced). Windows: 1m / 10m / 1h / 5h / 24h.
- **Overall Latency.** Count, OK, errors, avg, p50, p95 per window.
- **Overall Tokens & Cost.** Input, output, cache-write, cache-read,
  cost per window.
- **Per Model.** Two tables (latency + tokens) for every model that's been
  seen — keyed off the `model` field in request bodies.

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

## Config reference

All of `config.yaml` is hot-reloaded except `upstream_base_url`, `listen_host`,
and `listen_port` — those require a restart.

| Key | Default | Notes |
|---|---|---|
| `upstream_base_url` | `https://api.anthropic.com` | Where to forward. |
| `listen_host` / `listen_port` | `127.0.0.1` / `8787` | Local socket. |
| `initial_tier` | `low` | `low` or `high`. |
| `force_tier` | `null` | `null` = auto, `"low"` / `"high"` = pin. |
| `tiers.low / tiers.high` | `4` / `1000` | `max_concurrent` only — the concurrency cap for each tier. |
| `promotion_cooldown_seconds` | `300` | Min seconds between failed probe / demotion and next probe. |
| `retry_max_attempts` | `12` | Per request. |
| `retry_base_delay` / `retry_max_delay` | `1.0` / `60.0` | Exponential backoff bounds. |
| `upstream_timeout` | `600` | Per-request timeout (s). |
| `log_level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR`. |
| `config_poll_seconds` | `2.0` | How often `config.yaml` is checked. |
| `metrics_window_seconds` | `86400` | Rolling-window cap for the completions deque. |
| `model_pricing` | `{}` | Per-million-token rates. |

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
- **Persistence.** Metrics live in memory only — a restart wipes the 24h
  history.

## Manual control

```sh
# Inspect current state
curl http://127.0.0.1:8787/_proxy/status | jq

# All metrics including per-model
curl http://127.0.0.1:8787/_proxy/metrics | jq

# Pin tier
curl -X POST http://127.0.0.1:8787/_proxy/force_tier \
     -H 'content-type: application/json' -d '{"tier":"high"}'

# Back to auto
curl -X POST http://127.0.0.1:8787/_proxy/force_tier \
     -H 'content-type: application/json' -d '{"tier":null}'
```
