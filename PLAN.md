# Plan: multi-dimensional quota budgets (requests / tokens / cost)

## Why

The upstream plan changed from "N requests per window" to a set of simultaneous
limits per tier:

| | LOW | HIGH |
|---|---|---|
| requests / minute | 20 | 1000 |
| tokens / minute | 500 000 | 500 000 |
| USD / minute | 50 | 50 |
| concurrent requests | 4 | 1000 |
| long-horizon USD | 30 / 5 h | 100 / 1 h |

The proxy today tracks exactly **one** rolling window per tier, measured in
(weighted) request counts. This plan generalizes that to a **list of budgets
per tier**, each budget being `(metric, limit, window_seconds)` with
`metric ∈ {requests, tokens, cost}` — while keeping the old single
request-window config working unchanged (the plan may revert).

Concurrency needs no new machinery — `max_concurrent` per tier already exists;
only the default numbers change (LOW 4, HIGH 1000 — already the defaults).

## Philosophy (unchanged — do not drift)

- Budgets are **tracked, not enforced** for the human lane. Upstream 429s +
  retry/backoff remain the real pacing. Nothing in this plan adds admission
  blocking for humans.
- The **auto lane** is preemptively paced (AutoPacer) to spend only the
  leftover; with multiple budgets it paces against the **binding** (most
  constraining) budget.
- The event loop is single-threaded: all window mutation methods stay
  **sync and await-free** (atomic without locks). Keep them so.
- Module graph stays acyclic; `limiter` stays a leaf (no imports from
  `metrics`). Cost is computed *outside* the limiter and passed in as a number.

## Core design

### 1. `Budget` and `BudgetWindow` (limiter.py)

```python
class Budget:
    __slots__ = ("metric", "limit", "window_seconds")
    # metric: "requests" | "tokens" | "cost"
    # limit: float  (> 0)
    # window_seconds: float  (> 0)
```

`Tier` drops nothing but gains `budgets: list[Budget]`. Its legacy
constructor args `window_seconds` / `window_limit` remain accepted and, when
no `budgets` list is given, compile to
`[Budget("requests", window_limit, window_seconds)]`. (Existing tests build
`Tier(...)` with the old signature — they must keep passing.)

Each budget of the **active tier** gets its own independent window state
(the current `_window_*` fields, extracted into a small class):

```python
class BudgetWindow:
    budget: Budget
    start: float | None     # unix; None = dormant, next event anchors it
    count: float            # units of budget.metric consumed this window
    human: float            # lane split; invariant human + auto == count
    auto: float
```

The limiter holds `self._windows: list[BudgetWindow]`, rebuilt on every tier
change. **Each window anchors and rolls independently** (a 60 s window rolls
many times inside a 5 h one). Rolling rule per window, same as today: if
`start is None or now - start >= window_seconds`, re-anchor at `now`.

Ordering: keep the config order. Define `binding_index()` = the window with
the highest utilization `count / limit` among *active* windows (ties: first).
Used for the top-level snapshot mirror and the statusline.

### 2. Feeding the windows

Two feed points, because the three metrics become known at different times:

- **requests** — at admission, exactly as today. `note_request(model, lane)`
  adds `_window_weight_for(model)` to every *requests*-metric window
  (per-model `model_window_weights` apply **only** to requests budgets).
  `discount_request(...)` reverses it on failure; `note_done(...)` drops the
  in-flight tally. The in-flight carry-forward on `_restart_window` /
  window roll applies **only to requests windows** (that's the only metric
  with a known in-flight weight).
- **tokens / cost** — at completion. New sync method:

  ```python
  def note_usage(self, tokens: float, cost: float, lane: str = "human") -> None
  ```

  Folds `tokens` into every *tokens* window and `cost` into every *cost*
  window of the active tier (anchoring fresh windows exactly like
  `note_request` does — but with **no** in-flight carry: `count = 0.0` on a
  fresh anchor before adding). Called once per **successful** request; failed
  requests report no usage and consume no token/cost quota — this matches the
  existing "window counts only quota actually consumed" rule, and no
  discount path is needed for tokens/cost.

  A request that straddles a window roll or tier switch lands its tokens/cost
  in whatever window is current at completion. Accepted and documented; do
  not build attribution-to-start-window machinery.

`note_usage` also updates two EWMAs owned by the limiter (same 0.2/0.8
blend as `Metrics._ewma_duration`):

```python
self._ewma_tokens_per_req: float | None
self._ewma_cost_per_req: float | None
```

These let the limiter/pacer convert the human *request* rate into token/$
rates without the limiter importing metrics. Fallbacks before first data come
from config (see §4): `auto_assumed_tokens_per_request`,
`auto_assumed_cost_per_request`. Expose
`units_per_request(metric) -> float` returning 1.0 for `"requests"`, the EWMA
(or assumed fallback) for `"tokens"` / `"cost"`.

**Who computes cost:** `server.py`'s `finalize`. Add a public
`Metrics.cost_of(model, usage) -> float | None` (thin wrapper over the
existing `_cost`). `finalize` computes
`tokens = input + output + cache_creation + cache_read` and
`cost = metrics.cost_of(model, usage) or 0.0`, then calls
`limiter.note_usage(tokens, cost, lane)` — **only when**
`200 <= status < 400` and `usage` is not None. Unpriced models therefore
contribute 0 to cost windows; document in config.yaml that cost budgets
require `model_pricing` entries to mean anything.

Token definition: all four usage fields count. (If the upstream's TPM turns
out to exclude cache reads, that's a one-line follow-up; don't add a knob
now.)

### 3. Snapshot shape (limiter `_window_snapshot`)

New shape — a `windows` list plus top-level fields mirroring the **binding**
window so the statusline and any legacy consumer keep working:

```jsonc
{
  "active": true,                // any window active
  "tier": "low",
  "windows": [
    {
      "metric": "requests", "limit": 20, "window_seconds": 60,
      "active": true, "count": 7, "count_human": 5, "count_auto": 2,
      "utilization": 0.35,
      "started_at": ..., "elapsed_seconds": ..., "remaining_seconds": ...,
      "effective_remaining_seconds": ..., "switch_at": ...,
      "projected_human": 11
    },
    { "metric": "tokens", ... },
    { "metric": "cost", "limit": 30, "window_seconds": 18000, ... }
  ],
  "binding": 2,                  // index into windows (highest utilization)
  // top-level mirror of windows[binding] for legacy consumers:
  "limit": 30, "window_seconds": 18000, "count": 12.4, "metric": "cost",
  "count_human": ..., "count_auto": ..., "projected_human": ...,
  "started_at": ..., "elapsed_seconds": ..., "remaining_seconds": ...,
  "effective_remaining_seconds": ..., "switch_at": ...
}
```

Per-window details:

- `effective_remaining_seconds` / `switch_at`: the pending LOW→HIGH switch
  caps **every** window's effective remaining (same logic as today, applied
  per window, only while LOW).
- `projected_human` per window:
  `human + human_rate() * units_per_request(metric) * effective_remaining`,
  capped at `limit`.
- `utilization = count / limit` (0 when inactive). Round counts with the
  existing `_n()` helper; round cost to 4 decimals, utilization to 3.
- When **no** window is active, return the inactive shape with the same
  `windows` list (each entry `active: false, count: 0`) and binding 0.

### 4. Config format (config.py + config.yaml)

New per-tier key `limits` — a list of explicit budget entries:

```yaml
tiers:
  low:
    max_concurrent: 4
    limits:
      - {metric: requests, limit: 20,     window_seconds: 60}
      - {metric: tokens,   limit: 500000, window_seconds: 60}
      - {metric: cost,     limit: 50,     window_seconds: 60}
      - {metric: cost,     limit: 30,     window_seconds: 18000}
  high:
    max_concurrent: 1000
    limits:
      - {metric: requests, limit: 1000,   window_seconds: 60}
      - {metric: tokens,   limit: 500000, window_seconds: 60}
      - {metric: cost,     limit: 50,     window_seconds: 60}
      - {metric: cost,     limit: 100,    window_seconds: 3600}
```

Rules (implement as `parse_budgets(cfg, tier_name) -> list[Budget] | None`
next to `parse_window_weights`, with the same defensive style):

- `limits` present and valid → it wins. Entries with unknown `metric`,
  non-numeric or ≤ 0 `limit`/`window_seconds` are dropped with a
  `log.warning`. An empty/fully-invalid list → fall back to legacy (warn).
- `limits` absent → **legacy path, byte-for-byte today's behavior**: one
  requests budget from tier `window_seconds`/`window_limit`, falling back to
  top-level `rate_window_seconds`/`rate_window_limit`. This is the
  "old request based style" compatibility requirement.
- If both `limits` and `window_seconds`/`window_limit` are present on a tier,
  `limits` wins; log an info line.

`DEFAULT_CONFIG` changes:

- `tiers` default becomes the new-plan `limits` shown above (keep
  `max_concurrent` 4 / 1000).
- New keys, with inline comments:
  - `auto_assumed_tokens_per_request: 20000` — pacer/projection fallback
    before any completion has been measured; tune to your traffic.
  - `auto_assumed_cost_per_request: 0.05` — same, in USD.
- Keep `rate_window_seconds` / `rate_window_limit` and the per-tier legacy
  keys documented as the legacy single-window style.

Wire through **both** `build_state` and `apply_config_change`:
`make_tier` builds `Tier(name, max_concurrent, budgets=parse_budgets(...))`;
the assumed-units keys go to the limiter via a new
`limiter.set_assumed_units(tokens, cost)` (called next to
`set_auto_params`) and to nothing else. Update `config.yaml` (heavily
commented, like the rest of the file) and its tiers example.

The log lines in `release_success` / `release_rate_limited` /
`apply_scheduled_switch` / `boost_high` that print
`window={limit}/{seconds}s` must be updated to print the budget list
compactly, e.g. `budgets=[20req/60s, 500000tok/60s, 50$/60s, 30$/18000s]`
(add a small `Budget.__str__` / helper).

### 5. Pacer (pacer.py) — pace on the binding budget

`_usable_and_rate()` becomes: for each window in the snapshot's `windows`,
compute a per-budget usable and request-rate, then take the **minimum rate**
(and report that budget's usable):

```
units_per_req = limiter.units_per_request(metric)   # 1.0 for requests
horizon       = min(effective_remaining, lookahead)
expected_human_units = safety * human_rate * units_per_req * horizon
floor_units   = floor * units_per_req if metric == "requests" else 0.0
                # human_quota_floor stays a request count; ignore for
                # tokens/cost rather than inventing per-metric floors
usable_units  = limit - count - expected_human_units - floor_units
usable_reqs   = usable_units / units_per_req
rate          = usable_reqs / effective_remaining
```

- Inactive window (not yet anchored): skip it (unconstrained until traffic
  anchors it) — mirrors today's "no window open yet → let it through".
- No windows at all / snapshot inactive → `(1.0, inf)` as today.
- Any budget with `usable_reqs <= 0` → the whole result is
  `(that usable, 0.0)` (park), and remember **which** budget bound.
- Otherwise take `min` over budgets' rates, then apply the existing physical
  capacity cap (`max_concurrent / avg_duration`).

`status()` additions (keep every existing key so the dashboard JS degrades
gracefully): `binding_metric` (e.g. `"cost"`), `binding_window_seconds`, and
per-budget `projected_auto` folded into the snapshot consumer if cheap —
minimum: keep the existing top-level `usable` / `projected_auto` semantics
but computed in **requests** (divide the binding budget's usable units by
`units_per_req` — that is `usable_reqs` above, which is what the current
key already means).

The gate loop itself is unchanged (park / interval clamp / poll) — only the
rate computation changes.

### 6. Server (server.py)

In `handle_proxy.finalize(status, usage)` after the existing bookkeeping:

```python
if 200 <= status < 400 and usage:
    tokens = sum(int(usage.get(k, 0) or 0) for k in (
        "input_tokens", "output_tokens",
        "cache_creation_input_tokens", "cache_read_input_tokens"))
    cost = metrics.cost_of(model, usage) or 0.0
    limiter.note_usage(tokens, cost, lane)
```

Nothing else in the handler changes (note_request/discount/note_done flow is
untouched).

### 7. Persistence (persistence.py + limiter serialize/load)

`window_state()` v2:

```jsonc
{
  "version": 2,
  "tier": "low",
  "windows": [
    {"metric": "cost", "window_seconds": 18000, "started_at": ...,
     "count": 12.4, "count_auto": 3.1},
    ...
  ]
}
```

`load_window_state()`:

- v2: match each saved entry to a current active-tier budget by
  `(metric, window_seconds)` (first unmatched wins if duplicates); drop
  entries already elapsed (`now - started_at >= window_seconds`) or matching
  no current budget. Restore `count`, derive human as `count - min(auto,
  count)` (the existing hand-edit-safe invariant). Return True if **any**
  window restored.
- Legacy file (no `version` / top-level `started_at`): restore into the
  **first requests budget** using the saved `window_seconds` for the elapsed
  check, exactly today's semantics.
- `save_window_file` already clears the file when nothing is active — "no
  window active" now means *no* BudgetWindow has an unelapsed start.

### 8. Manual overrides (routes.py + limiter setters)

`POST /_proxy/window/count` body gains optional selectors:
`{"count": N, "metric": "requests", "window_seconds": 60}` —

- `set_window_count(count, metric=None, window_seconds=None)`: target the
  matching window (metric required if the tier has >1 budget; if omitted,
  default to the **first requests budget** — preserves the old API calls
  byte-for-byte on legacy configs). Anchor at now if inactive, clamp the
  lane split as today. 404-style JSON error if no window matches.
- `POST /_proxy/window/start` `{"started_at": ..., "metric": ...,
  "window_seconds": ...}`: with selectors, set that window's start; without,
  apply to **all** windows (and `null` still means full `_restart_window()`).

Both still persist to window.json immediately.

### 9. Dashboard (dashboard/app.js, index.html, styles.css) + statusline (routes.py)

- **Quota window card**: render one meter row per entry in
  `window.windows` — label (`20 req / 60s`, `$30 / 5h`), a progress bar at
  `utilization`, `count / limit`, countdown from `effective_remaining_seconds`.
  Highlight the `binding` row. Keep each sub-line short and `nowrap`-safe
  (flexbox card layout — see CLAUDE.md layout note). Format tokens with
  k/M suffixes and cost as `$X.XX`.
- **Auto Pacing card**: show `binding_metric` in the reason line
  (e.g. `paced (cost/5h)`).
- The JS must tolerate the old shape (no `windows` key) for one poll cycle
  during a live upgrade — guard with `snap.windows || [legacy-synth]`.
- **Statusline** (`/_proxy/statusline` renderer in routes.py): the window
  segment shows the **binding** window, with a unit-aware format:
  `140/600` (requests), `312k/500k tok` (tokens), `$12.3/$30` (cost). Human/
  auto split and projections keep reading the top-level mirror fields.

### 10. Docs

- `config.yaml`: new `limits` blocks (commented), assumed-units keys, note
  that cost budgets need `model_pricing`, and a "legacy style still
  supported" paragraph.
- `CLAUDE.md`: update the `limiter.py` section (Budget/BudgetWindow,
  note_usage, snapshot shape, persistence v2), `pacer.py` (binding budget),
  config section (limits + assumed units), routes (window/count selectors).
- `README.md`: brief mention of multi-budget limits + the new tiers example.

## Implementation order (each step leaves tests green)

1. **limiter.py**: `Budget`, `BudgetWindow`, Tier gains `budgets` (legacy
   ctor args still work), rewrite `_restart_window` / `note_request` /
   `note_done` / `discount_request` / `_window_snapshot` /
   `set_window_count` / `set_window_start` / `window_state` /
   `load_window_state` over the windows list; add `note_usage`,
   `units_per_request`, `set_assumed_units`, log-line formatting.
   Update/extend `tests/test_limiter.py` (see test list). The discount token
   changes from a single float to an opaque per-window token (spec: a tuple
   of `(window_index, start)` pairs captured at note time; `discount_request`
   only reverses entries whose window still has that start).
2. **config.py**: `parse_budgets`, DEFAULT_CONFIG (new tiers default +
   assumed-units keys), `make_tier`, `build_state` + `apply_config_change`
   wiring (`set_assumed_units`). Tests for parsing/fallbacks.
3. **metrics.py**: add `cost_of()` (public wrapper). One-liner test.
4. **pacer.py**: multi-budget `_usable_and_rate` + status additions. Tests:
   binding selection, token-limited vs cost-limited scenarios, assumed-unit
   fallback, park on spent cost budget, inactive-window skip.
5. **server.py**: `finalize` → `note_usage` on success.
6. **persistence.py** + limiter (if not already in step 1): v2 file format +
   legacy load. Round-trip tests incl. a legacy-format file.
7. **routes.py**: window/count + window/start selectors; statusline binding
   format.
8. **dashboard/**: multi-meter window card, pacing card metric, old-shape
   guard.
9. **Docs** (config.yaml, CLAUDE.md, README.md).

## Test checklist (tests/, pure modules, no server)

- Legacy `Tier(name, mc, window_seconds, window_limit)` still constructs and
  behaves identically for all existing tests (they must pass unmodified
  except where they assert snapshot internals — update those to read the
  top-level mirror or `windows[0]`).
- `parse_budgets`: valid list; unknown metric dropped; ≤0 dropped; empty →
  legacy fallback; `limits` + legacy keys → limits wins.
- Multi-window independence: 60 s requests window rolls while 5 h cost
  window keeps counting.
- `note_request` feeds only requests windows; weights apply; discount
  reverses only unrolled windows (roll one window between note and discount,
  assert the other still reverses).
- `note_usage`: feeds tokens/cost windows, anchors fresh ones, no in-flight
  carry, updates EWMAs; lane split invariant `human + auto == count` on every
  window after arbitrary sequences.
- `_restart_window` on tier switch: requests windows re-seed from in-flight,
  token/cost windows reset to 0; HIGH-landing still fires `_rl_wake`.
- Snapshot: `binding` picks max utilization; top-level mirror equals
  `windows[binding]`; `effective_remaining_seconds` capped by a pending
  switch on **every** window; `projected_human` uses `units_per_request`.
- Persistence v2 round-trip; legacy window.json restores into the requests
  budget; elapsed entries dropped individually.
- Pacer: with fabricated snapshots — requests-bound, tokens-bound,
  cost-bound each produce the min rate; `usable ≤ 0` on any budget parks;
  no active windows → open.
- `set_window_count` with and without selectors; error on no match.

## Non-goals (explicitly out of scope)

- No enforcement/blocking of the human lane on any budget.
- No per-metric `human_quota_floor` variants, no cache-read token knob, no
  attribution of straddling usage to its start window.
- No change to retry/backoff, tier probing/promotion, scheduling, or the
  429-wake feature currently uncommitted in the working tree (rebase this
  work on top of it).
