"""Price calibration: derive per-model $/Mtok rates from upstream cost counters.

Pure module: no FastAPI. The provider reports *cumulative* cost counters per
token class (uncached input, cached input, output) but not per model; the
proxy independently records cumulative per-model token counts per class. Each
pair of snapshots therefore yields one interval where both sides are known:

    ΔCost_output       = Σ_m price_output[m]     · ΔTok_output[m]
    ΔCost_input_cached = Σ_m price_cache_read[m] · ΔTok_cache_read[m]
    ΔCost_input_uncached
                       = Σ_m price_input[m]      · ΔTok_input[m]
                       + Σ_m price_cache_creation[m] · ΔTok_cache_creation[m]

(cache writes are billed inside the provider's "uncached input" counter, so
that equation carries two unknowns per model). Collect intervals and solve
each system by least squares; a model's price is "direct" when some interval
isolates it (only that model consumed that class), "regression" when it comes
out of the joint solve, and unidentifiable when the data can't separate it
(e.g. two models always run in the same ratio). Since all counters are
cumulative, the "since the plan changed" baseline cancels in the deltas — the
proxy's counters don't need to start at the same moment, only to be monotone
across the span. An interval where any counter went backwards (plan reset,
stats.json wiped) is skipped.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("proxy.calibrate")

COST_KEYS = ("cost_input_uncached", "cost_input_cached", "cost_output")
TOKEN_CLASSES = ("input", "cache_creation", "cache_read", "output")

# Each system: (name, upstream cost key, [(token class, price key), ...]).
# Price keys deliberately match the model_pricing config schema.
_SYSTEMS = (
    ("output", "cost_output", (("output", "output"),)),
    ("input_cached", "cost_input_cached", (("cache_read", "cache_read"),)),
    ("input_uncached", "cost_input_uncached",
     (("input", "input"), ("cache_creation", "cache_creation"))),
)


def _solve_normal(m: list[list[float]], v: list[float],
                  eps_rel: float = 1e-9) -> list[float | None]:
    """Solve the normal equations M·x = v, returning None per unknown the data
    cannot determine (zero/degenerate column, or coupled to a free unknown)."""
    n = len(v)
    a = [row[:] + [v[i]] for i, row in enumerate(m)]
    scale = max((abs(a[i][i]) for i in range(n)), default=0.0)
    if scale <= 0.0:
        return [None] * n
    eps = scale * eps_rel
    pivot_row_of_col: list[int] = [-1] * n
    r = 0
    for c in range(n):
        pr = max(range(r, n), key=lambda i: abs(a[i][c]))
        if abs(a[pr][c]) <= eps:
            continue  # free column -> unidentifiable unknown
        a[r], a[pr] = a[pr], a[r]
        pv = a[r][c]
        a[r] = [x / pv for x in a[r]]
        for i in range(n):
            if i != r and a[i][c] != 0.0:
                f = a[i][c]
                a[i] = [x - f * y for x, y in zip(a[i], a[r])]
        pivot_row_of_col[c] = r
        r += 1
    out: list[float | None] = []
    for c in range(n):
        pr = pivot_row_of_col[c]
        if pr < 0:
            out.append(None)
            continue
        # A pivot that still mixes with a free column isn't determined either.
        if any(pivot_row_of_col[f] < 0 and abs(a[pr][f]) > eps_rel
               for f in range(n) if f != c):
            out.append(None)
            continue
        out.append(a[pr][n])
    return out


def _lstsq(rows: list[tuple[list[float], float]], n: int) -> list[float | None]:
    """Least squares over rows of (coefficients, target) via normal equations."""
    m = [[0.0] * n for _ in range(n)]
    v = [0.0] * n
    for coeffs, y in rows:
        for i in range(n):
            ci = coeffs[i]
            if ci == 0.0:
                continue
            v[i] += ci * y
            for j in range(n):
                if coeffs[j] != 0.0:
                    m[i][j] += ci * coeffs[j]
    return _solve_normal(m, v)


class Calibrator:
    """Stores (upstream costs, proxy token counters) snapshots and solves them.

    Snapshots are appended by the API and persisted to disk immediately (they
    are rare, manual events). All mutation is sync + await-free, atomic under
    the single-threaded event loop like the rest of the proxy.
    """

    def __init__(self, path: str):
        self._path = Path(path).resolve()
        self._snapshots: list[dict[str, Any]] = []
        self._load()

    # -- persistence --

    def configure(self, path: str) -> None:
        self._path = Path(path).resolve()

    def _load(self) -> None:
        try:
            if not self._path.exists():
                return
            with open(self._path) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError, ValueError) as e:
            log.warning(f"calibrate: could not load {self._path}: {e!r}")
            return
        snaps = data.get("snapshots") if isinstance(data, dict) else None
        if isinstance(snaps, list):
            self._snapshots = [s for s in snaps if isinstance(s, dict)]
            log.info(f"calibrate: loaded {len(self._snapshots)} snapshots "
                     f"from {self._path}")

    def _save(self) -> None:
        tmp = self._path.with_name(self._path.name + ".tmp")
        try:
            tmp.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w") as f:
                json.dump({"version": 1, "snapshots": self._snapshots}, f,
                          separators=(",", ":"))
            os.replace(tmp, self._path)
        except OSError as e:
            log.warning(f"calibrate: save to {self._path} failed: {e!r}")

    # -- recording --

    def add_snapshot(self, costs: dict[str, float],
                     tokens: dict[str, dict[str, float]],
                     at: float | None = None) -> dict[str, Any]:
        """Record one snapshot: upstream cumulative costs + the proxy's current
        cumulative per-model token counters. Returns the stored snapshot."""
        snap = {
            "at": float(at) if at is not None else time.time(),
            "costs": {k: max(0.0, float(costs.get(k, 0) or 0)) for k in COST_KEYS},
            "tokens": {
                str(m): {c: max(0.0, float((cls or {}).get(c, 0) or 0))
                         for c in TOKEN_CLASSES}
                for m, cls in (tokens or {}).items()
            },
        }
        self._snapshots.append(snap)
        self._snapshots.sort(key=lambda s: s["at"])
        self._save()
        return snap

    def reset(self) -> int:
        """Drop all snapshots (e.g. the provider's counters were reset again).
        Returns how many were discarded."""
        n = len(self._snapshots)
        self._snapshots = []
        self._save()
        return n

    def snapshot_count(self) -> int:
        return len(self._snapshots)

    # -- solving --

    @staticmethod
    def _interval(s0: dict[str, Any], s1: dict[str, Any]) -> dict[str, Any] | None:
        """Deltas between two consecutive snapshots, or None if any counter
        went backwards (provider plan reset / proxy stats wiped)."""
        dc = {}
        for k in COST_KEYS:
            d = float(s1["costs"].get(k, 0) or 0) - float(s0["costs"].get(k, 0) or 0)
            if d < -1e-9:
                return None
            dc[k] = max(0.0, d)
        models = set(s0.get("tokens") or {}) | set(s1.get("tokens") or {})
        dt: dict[str, dict[str, float]] = {}
        for m in models:
            a = (s0.get("tokens") or {}).get(m) or {}
            b = (s1.get("tokens") or {}).get(m) or {}
            d = {}
            for c in TOKEN_CLASSES:
                dv = float(b.get(c, 0) or 0) - float(a.get(c, 0) or 0)
                if dv < -1e-9:
                    return None
                d[c] = max(0.0, dv)
            if any(v > 0 for v in d.values()):
                dt[m] = d
        return {"costs": dc, "tokens": dt}

    def solve(self) -> dict[str, Any]:
        """Estimate per-model $/Mtok prices from all stored snapshots.

        Returns per model+class: price (or None), and a confidence of
        "direct" (some interval isolated it), "regression" (from the joint
        least squares), or "unidentifiable". Residuals report, per upstream
        cost counter, how much observed cost the solved prices explain —
        persistent unexplained cost means traffic is bypassing the proxy (or
        a price changed; reset and re-snapshot then).
        """
        intervals: list[dict[str, Any]] = []
        skipped = 0
        for s0, s1 in zip(self._snapshots, self._snapshots[1:]):
            iv = self._interval(s0, s1)
            if iv is None:
                skipped += 1
            else:
                intervals.append(iv)

        prices: dict[str, dict[str, dict[str, Any]]] = {}
        residuals: dict[str, dict[str, float]] = {}
        notes: list[str] = []
        if skipped:
            notes.append(f"{skipped} interval(s) skipped: a counter went "
                         f"backwards (plan/stats reset)")

        for name, cost_key, parts in _SYSTEMS:
            # Unknowns: one per (model, price_key) with any tokens observed.
            unknowns: list[tuple[str, str]] = []
            for iv in intervals:
                for m, d in iv["tokens"].items():
                    for cls, pkey in parts:
                        if d.get(cls, 0) > 0 and (m, pkey) not in unknowns:
                            unknowns.append((m, pkey))
            unknowns.sort()
            if not unknowns:
                continue
            rows: list[tuple[list[float], float]] = []
            direct: set[tuple[str, str]] = set()
            for iv in intervals:
                coeffs = []
                for m, pkey in unknowns:
                    cls = next(c for c, pk in parts if pk == pkey)
                    coeffs.append(iv["tokens"].get(m, {}).get(cls, 0.0) / 1e6)
                if not any(coeffs):
                    continue
                rows.append((coeffs, iv["costs"][cost_key]))
                nz = [i for i, c in enumerate(coeffs) if c > 0]
                if len(nz) == 1 and iv["costs"][cost_key] > 0:
                    direct.add(unknowns[nz[0]])
            solution = _lstsq(rows, len(unknowns))
            for (m, pkey), val in zip(unknowns, solution):
                entry: dict[str, Any] = {"price": None, "confidence": "unidentifiable"}
                if val is not None:
                    if val < 0:
                        notes.append(f"{m}.{pkey}: negative estimate "
                                     f"({val:.4f}) clamped to 0")
                        val = 0.0
                    entry["price"] = round(val, 4)
                    entry["confidence"] = "direct" if (m, pkey) in direct else "regression"
                prices.setdefault(m, {})[pkey] = entry
            # Residual over rows whose unknowns are all identified.
            actual = explained = 0.0
            usable = 0
            for coeffs, y in rows:
                if any(coeffs[i] > 0 and solution[i] is None
                       for i in range(len(unknowns))):
                    continue
                usable += 1
                actual += y
                explained += sum(c * (solution[i] or 0.0)
                                 for i, c in enumerate(coeffs))
            residuals[cost_key] = {
                "intervals": usable,
                "actual": round(actual, 4),
                "explained": round(explained, 4),
                "unexplained": round(actual - explained, 4),
            }

        return {
            "snapshots": len(self._snapshots),
            "intervals": len(intervals),
            "models": prices,
            "residuals": residuals,
            "notes": notes,
            "model_pricing_yaml": self._to_yaml(prices),
        }

    @staticmethod
    def _to_yaml(prices: dict[str, dict[str, dict[str, Any]]]) -> str:
        """Ready-to-paste model_pricing block (identified prices only)."""
        lines = ["model_pricing:"]
        for m in sorted(prices):
            known = {k: v["price"] for k, v in prices[m].items()
                     if v["price"] is not None}
            if not known:
                continue
            lines.append(f"  {m}:")
            for key in ("input", "output", "cache_creation", "cache_read"):
                if key in known:
                    lines.append(f"    {key}: {known[key]}")
        return "\n".join(lines) if len(lines) > 1 else ""
