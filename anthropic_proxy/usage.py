"""Usage parsing: provider wire formats -> one canonical token shape.

Pure module (no FastAPI/httpx): `normalize_usage` is the single chokepoint for
all three supported usage formats; the extractors scrape it out of streaming
SSE or buffered JSON response bodies.
"""
from __future__ import annotations

import json
from typing import Any


def normalize_usage(usage: Any) -> dict | None:
    """Map a provider `usage` block to our canonical 4-field shape.

    Handles three wire formats:
      - Anthropic Messages:   input_tokens / output_tokens /
                              cache_creation_input_tokens / cache_read_input_tokens
      - OpenAI Responses:     input_tokens / output_tokens, with the cached
                              subset in input_tokens_details.cached_tokens
                              (input_tokens is inclusive of cached)
      - OpenAI Chat/Completions: prompt_tokens / completion_tokens, with the
                              cached subset in prompt_tokens_details.cached_tokens
                              (prompt_tokens is inclusive of cached)

    For the OpenAI shapes we split the cached tokens out of the prompt so
    `input_tokens` and `cache_read_input_tokens` stay disjoint (matching how
    Anthropic reports them, and how the per-token pricing is applied).
    OpenAI has no separate cache-write charge, so cache_creation stays 0.
    """
    if not isinstance(usage, dict):
        return None

    if "input_tokens" in usage or "output_tokens" in usage:
        inp = int(usage.get("input_tokens", 0) or 0)
        out = int(usage.get("output_tokens", 0) or 0)
        cc = int(usage.get("cache_creation_input_tokens", 0) or 0)
        cr = int(usage.get("cache_read_input_tokens", 0) or 0)
        details = usage.get("input_tokens_details")
        if isinstance(details, dict) and "cached_tokens" in details:
            # OpenAI Responses API: input_tokens is inclusive of cached.
            cr = int(details.get("cached_tokens", 0) or 0)
            inp = max(0, inp - cr)
        return {
            "input_tokens": inp,
            "output_tokens": out,
            "cache_creation_input_tokens": cc,
            "cache_read_input_tokens": cr,
        }

    if "prompt_tokens" in usage or "completion_tokens" in usage:
        prompt = int(usage.get("prompt_tokens", 0) or 0)
        completion = int(usage.get("completion_tokens", 0) or 0)
        details = usage.get("prompt_tokens_details")
        cached = 0
        if isinstance(details, dict):
            cached = int(details.get("cached_tokens", 0) or 0)
        return {
            "input_tokens": max(0, prompt - cached),
            "output_tokens": completion,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": cached,
        }

    return None


class SSEUsageExtractor:
    """Watches an SSE byte stream for usage info (Anthropic + OpenAI).

    Anthropic emits a `message_start` event whose data contains
    `message.usage` (input_tokens, cache_*), and a `message_delta` event whose
    data contains `usage.output_tokens`. OpenAI Chat Completions emit a final
    chunk carrying a top-level `usage` (only when the client sets
    `stream_options.include_usage`); the OpenAI Responses API nests usage under
    `response.usage` on `response.completed`. We accumulate field-wise maxima
    across whatever arrives and return them once the stream ends.
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        self._input = 0
        self._output = 0
        self._cache_creation = 0
        self._cache_read = 0
        self._got_any = False

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._buf.extend(chunk)
        while True:
            sep = self._buf.find(b"\n\n")
            if sep < 0:
                if len(self._buf) > 65536:
                    # Drop everything but the last 64KB to bound memory.
                    del self._buf[: len(self._buf) - 65536]
                return
            event = bytes(self._buf[:sep])
            del self._buf[: sep + 2]
            self._parse_event(event)

    def _parse_event(self, block: bytes) -> None:
        data_parts: list[bytes] = []
        for line in block.split(b"\n"):
            if line.startswith(b"data:"):
                data_parts.append(line[5:].lstrip())
        if not data_parts:
            return
        try:
            obj = json.loads(b"\n".join(data_parts))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        if not isinstance(obj, dict):
            return
        et = obj.get("type")
        if et == "message_start":
            self._merge((obj.get("message") or {}).get("usage"))
            return
        if et == "message_delta":
            self._merge(obj.get("usage"))
            return
        # OpenAI Responses API: usage rides on response.completed/.incomplete.
        resp = obj.get("response")
        if isinstance(resp, dict) and isinstance(resp.get("usage"), dict):
            self._merge(resp["usage"])
            return
        # OpenAI Chat Completions: final chunk carries a top-level usage.
        if isinstance(obj.get("usage"), dict):
            self._merge(obj["usage"])

    def _merge(self, usage: Any) -> None:
        norm = normalize_usage(usage)
        if norm is None:
            return
        self._got_any = True
        self._input = max(self._input, norm["input_tokens"])
        self._output = max(self._output, norm["output_tokens"])
        self._cache_creation = max(self._cache_creation, norm["cache_creation_input_tokens"])
        self._cache_read = max(self._cache_read, norm["cache_read_input_tokens"])

    def final_usage(self) -> dict | None:
        if not self._got_any:
            return None
        return {
            "input_tokens": self._input,
            "output_tokens": self._output,
            "cache_creation_input_tokens": self._cache_creation,
            "cache_read_input_tokens": self._cache_read,
        }


class JSONUsageExtractor:
    """Buffers a non-streaming JSON response body and extracts top-level usage.

    Works for Anthropic Messages and OpenAI Chat Completions / Responses, all of
    which put a `usage` object at the top level of the response body.
    """

    def __init__(self, max_bytes: int = 8 * 1024 * 1024) -> None:
        self._buf = bytearray()
        self._max = max_bytes
        self._oversize = False

    def feed(self, chunk: bytes) -> None:
        if self._oversize or not chunk:
            return
        if len(self._buf) + len(chunk) > self._max:
            self._oversize = True
            self._buf.clear()
            return
        self._buf.extend(chunk)

    def final_usage(self) -> dict | None:
        if self._oversize or not self._buf:
            return None
        try:
            obj = json.loads(bytes(self._buf))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        if not isinstance(obj, dict):
            return None
        return normalize_usage(obj.get("usage"))


def make_extractor(content_type: str) -> SSEUsageExtractor | JSONUsageExtractor:
    if "text/event-stream" in content_type.lower():
        return SSEUsageExtractor()
    return JSONUsageExtractor()


def extract_model(method: str, body: bytes) -> str:
    """Extract `model` from a JSON request body. Falls back to '(unknown)'."""
    if method == "GET" or not body:
        return "(no-body)"
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        return "(unknown)"
    if isinstance(data, dict):
        m = data.get("model")
        if isinstance(m, str) and m:
            return m
    return "(unknown)"
