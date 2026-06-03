"""FastAPI app, process lifecycle (startup/shutdown/serve), and the catch-all
proxy handler. Shared mutable state lives in `runtime`; this module wires it to
HTTP and runs the two listening lanes."""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import runtime
from ._log import log
from .usage import extract_model, make_extractor

# Build the runtime state (config, limiter, metrics, pstats, pacer) at import,
# so `uvicorn anthropic_proxy.server:app` and the dual-port serve() agree.
runtime.bootstrap()

# Background tasks live here so startup()/shutdown() can be called either from
# the FastAPI lifespan (single-server) or directly from serve() (dual-port),
# without double-initializing.
_bg_tasks: list[asyncio.Task] = []


async def startup() -> None:
    if runtime.client is not None:
        return
    cfg = runtime.config
    runtime.client = httpx.AsyncClient(
        base_url=str(cfg["upstream_base_url"]).rstrip("/"),
        timeout=httpx.Timeout(float(cfg["upstream_timeout"]), connect=15.0),
        limits=httpx.Limits(max_connections=None, max_keepalive_connections=100),
    )
    _bg_tasks.append(asyncio.create_task(runtime.config_watch_loop()))
    _bg_tasks.append(asyncio.create_task(runtime.persist_loop()))
    human = f"http://{cfg['listen_host']}:{cfg['listen_port']}"
    auto_port = cfg.get("throttle_listen_port")
    auto = f" | auto-lane http://{cfg['listen_host']}:{auto_port}" if auto_port else ""
    log.info(
        f"anthropic_proxy human-lane {human}{auto} -> {cfg['upstream_base_url']} "
        f"| tier={runtime.limiter._active.name} forced={runtime.limiter._forced} "
        f"| dashboard: /_proxy/"
    )


async def shutdown() -> None:
    for task in _bg_tasks:
        task.cancel()
    for task in _bg_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
    _bg_tasks.clear()
    await runtime.pstats.maybe_flush(force=True)
    await asyncio.to_thread(runtime.save_window_file)
    if runtime.client is not None:
        await runtime.client.aclose()
        runtime.client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    await startup()
    try:
        yield
    finally:
        await shutdown()


app = FastAPI(lifespan=lifespan)


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def compute_backoff(attempt: int, retry_after: float | None,
                    retry_after_cap: float | None = None) -> float:
    """Backoff before the next retry.

    A server-provided Retry-After is honored up to `retry_after_cap` (the
    remaining retry budget) so we can sleep out a long quota reset in one wait
    instead of hammering upstream every `retry_max_delay`. Without Retry-After
    we fall back to capped exponential backoff.
    """
    cfg = runtime.config
    if retry_after is not None and retry_after > 0:
        cap = retry_after_cap if retry_after_cap is not None else float(cfg["retry_max_delay"])
        return min(retry_after, cap)
    delay = float(cfg["retry_base_delay"]) * (2 ** (attempt - 1))
    return min(delay, float(cfg["retry_max_delay"]))


# Register the /_proxy/* endpoints + dashboard on `app`. This import must come
# BEFORE the catch-all route below so the specific routes match first.
from . import routes  # noqa: E402,F401


# ---------- Proxy handler ----------

def request_lane(request: Request) -> str:
    """Which ingress lane a request arrived on, decided by listening port.

    The automation port (`throttle_listen_port`) is the paced lane; the human
    `listen_port` (and anything else) is unthrottled. Reads the server port from
    the ASGI scope, falling back to the URL port.
    """
    auto_port = runtime.config.get("throttle_listen_port")
    if auto_port is None:
        return "human"
    server = request.scope.get("server")
    port = server[1] if server and len(server) >= 2 else request.url.port
    return "auto" if port == int(auto_port) else "human"


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def proxy(full_path: str, request: Request):
    limiter = runtime.limiter
    metrics = runtime.metrics
    body = await request.body()
    target = "/" + full_path
    if request.url.query:
        target = f"{target}?{request.url.query}"
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in runtime.HOP_BY_HOP
    }
    model = extract_model(request.method, body)
    lane = request_lane(request)
    started_at = metrics.request_started(model)
    # Automation lane: wait for the pacer before committing the request to the
    # quota window. The human lane is never paced. (gate() can park for a while
    # when the window is nearly spent; it holds no concurrency slot meanwhile.)
    if lane == "auto":
        await runtime.pacer.gate()
    limiter.note_request(model)

    finished = False
    handed_off = False

    def finalize(status: int, usage: dict | None = None) -> None:
        nonlocal finished
        if not finished:
            finished = True
            metrics.request_finished(model, started_at, status, usage)

    try:
        # Connection errors give up after retry_max_attempts (a down upstream
        # won't be fixed by waiting). Rate-limit (429/503/529) retries instead
        # run against a wall-clock budget (retry_max_elapsed_seconds, > the 5h
        # quota window) so a queued request can outlast a full window and run
        # once the quota resets, rather than being purged after a few minutes.
        cfg = runtime.config
        max_conn_attempts = int(cfg["retry_max_attempts"])
        deadline = time.monotonic() + float(cfg.get("retry_max_elapsed_seconds", 18900))
        attempt = 0
        conn_errors = 0
        while True:
            attempt += 1
            was_probe = await limiter.acquire(lane)
            try:
                outbound = runtime.client.build_request(
                    method=request.method, url=target,
                    content=body, headers=headers,
                )
                response = await runtime.client.send(outbound, stream=True)
            except httpx.HTTPError as e:
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
                await asyncio.sleep(compute_backoff(conn_errors, None))
                continue

            if response.status_code in runtime.RATE_LIMIT_STATUSES:
                conn_errors = 0
                retry_after = parse_retry_after(response.headers.get("retry-after"))
                try:
                    rl_body = await response.aread()
                finally:
                    await response.aclose()
                await limiter.release_rate_limited(was_probe, lane)
                remaining = deadline - time.monotonic()
                backoff = compute_backoff(
                    attempt, retry_after, retry_after_cap=max(0.0, remaining)
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
                            if k.lower() not in runtime.HOP_BY_HOP
                        },
                    )
                await asyncio.sleep(backoff)
                continue

            is_success = 200 <= response.status_code < 400
            status_code = response.status_code
            out_headers = {
                k: v for k, v in response.headers.items()
                if k.lower() not in runtime.HOP_BY_HOP
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
        if not handed_off and not finished:
            metrics.request_finished(model, started_at, 0)


async def serve() -> None:
    """Run the human lane and (if configured) the automation lane together.

    Both ports serve the same app + shared state; startup()/shutdown() run once
    around them, so there is a single upstream client, queue, and set of
    background tasks regardless of how many ports are listening.
    """
    import uvicorn

    cfg = runtime.config
    host = str(cfg["listen_host"])
    log_level = str(cfg.get("log_level", "info")).lower()
    ports = [int(cfg["listen_port"])]
    auto_port = cfg.get("throttle_listen_port")
    if auto_port is not None and int(auto_port) not in ports:
        ports.append(int(auto_port))

    await startup()
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
        await shutdown()
