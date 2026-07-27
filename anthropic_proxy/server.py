"""HTTP layer: the FastAPI app, the catch-all proxy handler, and serve().

startup()/shutdown() create/tear down the shared httpx client and background
tasks once, idempotently — called either by the FastAPI lifespan (single-server
`uvicorn proxy:app`) or directly by serve() (the dual-port default). That split
is what lets the two ports share one client + one set of loops.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .config import build_state, config_watch_loop
from .persistence import save_window_file
from .routes import router
from .state import AppState
from .usage import extract_model, make_extractor

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("proxy")

RATE_LIMIT_STATUSES = {429, 503, 529}
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
    "content-encoding",  # stripped because we forward already-decoded chunks
}


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def compute_backoff(cfg: dict[str, Any], attempt: int, retry_after: float | None,
                    retry_after_cap: float | None = None) -> float:
    """Backoff before the next retry.

    A server-provided Retry-After is honored up to `retry_after_cap` (the
    remaining retry budget) so we can sleep out a long quota reset in one wait
    instead of hammering upstream every `retry_max_delay`. Without Retry-After
    we fall back to capped exponential backoff.
    """
    if retry_after is not None and retry_after > 0:
        cap = retry_after_cap if retry_after_cap is not None else float(cfg["retry_max_delay"])
        return min(retry_after, cap)
    delay = float(cfg["retry_base_delay"]) * (2 ** (attempt - 1))
    return min(delay, float(cfg["retry_max_delay"]))


def request_lane(cfg: dict[str, Any], request: Request) -> str:
    """Which ingress lane a request arrived on, decided by listening port.

    The automation port (`throttle_listen_port`) is the paced lane; the human
    `listen_port` (and anything else) is unthrottled. Reads the server port from
    the ASGI scope, falling back to the URL port.
    """
    auto_port = cfg.get("throttle_listen_port")
    if auto_port is None:
        return "human"
    server = request.scope.get("server")
    port = server[1] if server and len(server) >= 2 else request.url.port
    return "auto" if port == int(auto_port) else "human"


# ---------- lifecycle ----------

async def persist_loop(state: AppState) -> None:
    """Flush aggregated stats + window state to disk on a fixed interval."""
    while True:
        try:
            await state.pstats.maybe_flush()
            await asyncio.to_thread(save_window_file, state.limiter,
                                    state.window_persist_path())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"stats: periodic flush error: {e!r}")
        await asyncio.sleep(5.0)


async def startup(state: AppState) -> None:
    if state.client is not None:
        return
    cfg = state.config
    state.client = httpx.AsyncClient(
        base_url=str(cfg["upstream_base_url"]).rstrip("/"),
        timeout=httpx.Timeout(float(cfg["upstream_timeout"]), connect=15.0),
        limits=httpx.Limits(max_connections=None, max_keepalive_connections=100),
    )
    state.bg_tasks.append(asyncio.create_task(config_watch_loop(state)))
    state.bg_tasks.append(asyncio.create_task(persist_loop(state)))
    human = f"http://{cfg['listen_host']}:{cfg['listen_port']}"
    auto_port = cfg.get("throttle_listen_port")
    auto = f" | auto-lane http://{cfg['listen_host']}:{auto_port}" if auto_port else ""
    log.info(
        f"anthropic_proxy human-lane {human}{auto} -> {cfg['upstream_base_url']} "
        f"| tier={state.limiter.active.name} forced={state.limiter.forced} "
        f"| dashboard: /_proxy/"
    )


async def shutdown(state: AppState) -> None:
    for task in state.bg_tasks:
        task.cancel()
    for task in state.bg_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
    state.bg_tasks.clear()
    await state.pstats.maybe_flush(force=True)
    await asyncio.to_thread(save_window_file, state.limiter,
                            state.window_persist_path())
    if state.client is not None:
        await state.client.aclose()
        state.client = None


# ---------- the proxy handler ----------

async def handle_proxy(state: AppState, full_path: str, request: Request):
    limiter, metrics, pacer = state.limiter, state.metrics, state.pacer
    body = await request.body()
    target = "/" + full_path
    if request.url.query:
        target = f"{target}?{request.url.query}"
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP
    }
    model = extract_model(request.method, body)
    lane = request_lane(state.config, request)
    started_at = metrics.request_started(model)
    # Automation lane: wait for the pacer before committing the request to the
    # quota window. The human lane is never paced. (gate() can park for a while
    # when the window is nearly spent; it holds no concurrency slot meanwhile.)
    if lane == "auto":
        await pacer.gate()
    noted_weight, noted_token = limiter.note_request(model, lane)

    finished = False
    handed_off = False
    # Time this request actually held a concurrency slot, summed over attempts.
    # `started_at` above is the client's clock and deliberately starts before
    # pacing and queueing; this one only runs between `acquire()` returning and
    # the matching `release_*`, so pacer parking, queue waiting and 429 backoff
    # (all of which happen with no slot held) are excluded by construction.
    # That makes it the request's service time — the input the pacer's
    # throughput cap needs, and the "upstream" half of the dashboard's latency
    # split. Retries are summed because every attempt occupies the pipe.
    upstream_seconds = 0.0

    def note_slot_released(slot_started: float) -> None:
        nonlocal upstream_seconds
        upstream_seconds += max(0.0, time.monotonic() - slot_started)

    def finalize(status: int, usage: dict | None = None) -> None:
        nonlocal finished
        if not finished:
            finished = True
            # Window counts requests that actually consumed upstream quota: a
            # request that ultimately failed (rate-limited out / connection
            # error / client abort) is taken back off the window count.
            if not (200 <= status < 400):
                limiter.discount_request(noted_weight, noted_token, lane)
            elif usage:
                # Token/cost quota is only known now: fold this request's usage
                # into the token/cost budget windows (requests were counted at
                # admission by note_request).
                tokens = sum(int(usage.get(k, 0) or 0) for k in (
                    "input_tokens", "output_tokens",
                    "cache_creation_input_tokens", "cache_read_input_tokens"))
                cost = metrics.cost_of(model, usage) or 0.0
                limiter.note_usage(tokens, cost, lane)
            # Always drop it from the in-flight tally (any outcome ends its life).
            limiter.note_done(noted_weight, lane)
            metrics.request_finished(model, started_at, status, usage,
                                     upstream_seconds)

    try:
        # Connection errors give up after retry_max_attempts (a down upstream
        # won't be fixed by waiting). Rate-limit (429/503/529) retries instead
        # run against a wall-clock budget (retry_max_elapsed_seconds, > the 5h
        # quota window) so a queued request can outlast a full window and run
        # once the quota resets, rather than being purged after a few minutes.
        max_conn_attempts = int(state.config["retry_max_attempts"])
        deadline = time.monotonic() + float(state.config.get("retry_max_elapsed_seconds", 18900))
        attempt = 0
        conn_errors = 0
        while True:
            attempt += 1
            was_probe = await limiter.acquire(lane)
            slot_started = time.monotonic()   # the slot is now held
            try:
                outbound = state.client.build_request(
                    method=request.method, url=target,
                    content=body, headers=headers,
                )
                response = await state.client.send(outbound, stream=True)
            except httpx.HTTPError as e:
                note_slot_released(slot_started)
                await limiter.release_other_error(was_probe, lane)
                conn_errors += 1
                log.warning(
                    f"upstream error attempt={attempt} "
                    f"conn_errors={conn_errors}/{max_conn_attempts}: {e!r}"
                )
                if conn_errors >= max_conn_attempts:
                    finalize(502)
                    return JSONResponse(
                        {"error": "upstream_unreachable", "detail": str(e)},
                        status_code=502,
                    )
                await asyncio.sleep(compute_backoff(state.config, conn_errors, None))
                continue

            if response.status_code in RATE_LIMIT_STATUSES:
                conn_errors = 0
                retry_after = parse_retry_after(response.headers.get("retry-after"))
                try:
                    rl_body = await response.aread()
                finally:
                    await response.aclose()
                note_slot_released(slot_started)
                await limiter.release_rate_limited(was_probe, lane)
                remaining = deadline - time.monotonic()
                backoff = compute_backoff(
                    state.config, attempt, retry_after,
                    retry_after_cap=max(0.0, remaining),
                )
                log.info(
                    f"upstream {response.status_code} attempt={attempt} "
                    f"retry_after={retry_after} backoff={backoff:.1f}s "
                    f"budget_left={remaining:.0f}s probe={was_probe}"
                )
                if remaining - backoff <= 0:
                    finalize(response.status_code)
                    return Response(
                        content=rl_body,
                        status_code=response.status_code,
                        headers={
                            k: v for k, v in response.headers.items()
                            if k.lower() not in HOP_BY_HOP
                        },
                    )
                # Parked waiting out upstream pushback — surface it so the
                # dashboard shows the client is waiting (the request holds no
                # slot here and isn't a concurrency waiter). The sleep ends
                # early if the tier switches to HIGH mid-wait: the Retry-After
                # being honored was computed against the old window.
                limiter.enter_rl_wait(lane)
                try:
                    woke_early = await limiter.rl_backoff_sleep(backoff)
                finally:
                    limiter.leave_rl_wait(lane)
                if woke_early:
                    log.info(
                        f"429 backoff cut short by LOW->HIGH switch "
                        f"(attempt={attempt}, was sleeping {backoff:.1f}s)"
                    )
                continue

            is_success = 200 <= response.status_code < 400
            status_code = response.status_code
            out_headers = {
                k: v for k, v in response.headers.items()
                if k.lower() not in HOP_BY_HOP
            }
            extractor = make_extractor(response.headers.get("content-type", ""))

            async def body_stream():
                try:
                    async for chunk in response.aiter_bytes():
                        extractor.feed(chunk)
                        yield chunk
                finally:
                    try:
                        await response.aclose()
                    except Exception:
                        pass
                    # The slot is held for the whole stream — the response body
                    # occupies the pipe until the client has drained it.
                    note_slot_released(slot_started)
                    if is_success:
                        await limiter.release_success(was_probe, lane)
                    else:
                        await limiter.release_other_error(was_probe, lane)
                    usage = extractor.final_usage() if is_success else None
                    finalize(status_code, usage)

            handed_off = True
            return StreamingResponse(
                body_stream(),
                status_code=status_code,
                headers=out_headers,
            )
    finally:
        # Reached without handing off a response (e.g. cancellation/unexpected
        # error escaping the loop). Run finalize so the request is dropped from
        # the in-flight tally and its window count reversed, not just recorded.
        if not handed_off and not finished:
            finalize(0)


# ---------- app wiring ----------

def create_app(state: AppState) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await startup(state)
        try:
            yield
        finally:
            await shutdown(state)

    app = FastAPI(lifespan=lifespan)
    app.state.proxy = state
    app.include_router(router)

    # Catch-all proxy route — registered after the router so /_proxy/* wins.
    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
    )
    async def proxy(full_path: str, request: Request):
        return await handle_proxy(state, full_path, request)

    return app


# Default state + app, built from CONFIG_PATH (or ./config.yaml) at import time
# so `uvicorn proxy:app` / `uvicorn anthropic_proxy.server:app` keep working.
state = build_state()
app = create_app(state)


async def serve() -> None:
    """Run the human lane and (if configured) the automation lane together.

    Both ports serve the same app + shared state; startup()/shutdown() run once
    around them, so there is a single upstream client, queue, and set of
    background tasks regardless of how many ports are listening.
    """
    import uvicorn

    cfg = state.config
    host = str(cfg["listen_host"])
    log_level = str(cfg.get("log_level", "info")).lower()
    ports = [int(cfg["listen_port"])]
    auto_port = cfg.get("throttle_listen_port")
    if auto_port is not None and int(auto_port) not in ports:
        ports.append(int(auto_port))

    await startup(state)
    servers = [
        uvicorn.Server(uvicorn.Config(
            app, host=host, port=p, lifespan="off",
            log_level=log_level, access_log=False,
        ))
        for p in ports
    ]
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        await shutdown(state)


def main() -> None:
    asyncio.run(serve())
