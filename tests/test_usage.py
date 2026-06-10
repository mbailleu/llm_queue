import json

from anthropic_proxy.usage import (
    JSONUsageExtractor,
    SSEUsageExtractor,
    extract_model,
    make_extractor,
    normalize_usage,
)


# ---- normalize_usage: the three provider wire formats ----

def test_normalize_anthropic():
    u = normalize_usage({
        "input_tokens": 10, "output_tokens": 20,
        "cache_creation_input_tokens": 3, "cache_read_input_tokens": 4,
    })
    assert u == {
        "input_tokens": 10, "output_tokens": 20,
        "cache_creation_input_tokens": 3, "cache_read_input_tokens": 4,
    }


def test_normalize_openai_responses_splits_cached_out_of_input():
    u = normalize_usage({
        "input_tokens": 100, "output_tokens": 5,
        "input_tokens_details": {"cached_tokens": 60},
    })
    assert u["input_tokens"] == 40          # inclusive input minus cached
    assert u["cache_read_input_tokens"] == 60
    assert u["cache_creation_input_tokens"] == 0


def test_normalize_openai_chat_splits_cached_out_of_prompt():
    u = normalize_usage({
        "prompt_tokens": 100, "completion_tokens": 7,
        "prompt_tokens_details": {"cached_tokens": 30},
    })
    assert u == {
        "input_tokens": 70, "output_tokens": 7,
        "cache_creation_input_tokens": 0, "cache_read_input_tokens": 30,
    }


def test_normalize_rejects_non_usage():
    assert normalize_usage(None) is None
    assert normalize_usage("x") is None
    assert normalize_usage({"foo": 1}) is None


def test_normalize_null_fields_count_as_zero():
    u = normalize_usage({"input_tokens": None, "output_tokens": 5})
    assert u["input_tokens"] == 0 and u["output_tokens"] == 5


# ---- SSE extractor ----

def _sse(obj) -> bytes:
    return b"data: " + json.dumps(obj).encode() + b"\n\n"


def test_sse_anthropic_message_start_plus_delta():
    ex = SSEUsageExtractor()
    ex.feed(_sse({"type": "message_start",
                  "message": {"usage": {"input_tokens": 11,
                                        "cache_read_input_tokens": 5}}}))
    ex.feed(_sse({"type": "message_delta", "usage": {"output_tokens": 42}}))
    assert ex.final_usage() == {
        "input_tokens": 11, "output_tokens": 42,
        "cache_creation_input_tokens": 0, "cache_read_input_tokens": 5,
    }


def test_sse_event_split_across_chunks():
    ex = SSEUsageExtractor()
    whole = _sse({"type": "message_delta", "usage": {"output_tokens": 9}})
    ex.feed(whole[:10])
    ex.feed(whole[10:])
    assert ex.final_usage()["output_tokens"] == 9


def test_sse_openai_chat_final_chunk():
    ex = SSEUsageExtractor()
    ex.feed(_sse({"choices": [], "usage": {"prompt_tokens": 8, "completion_tokens": 3}}))
    u = ex.final_usage()
    assert u["input_tokens"] == 8 and u["output_tokens"] == 3


def test_sse_openai_responses_completed():
    ex = SSEUsageExtractor()
    ex.feed(_sse({"type": "response.completed",
                  "response": {"usage": {"input_tokens": 6, "output_tokens": 2}}}))
    u = ex.final_usage()
    assert u["input_tokens"] == 6 and u["output_tokens"] == 2


def test_sse_no_usage_returns_none():
    ex = SSEUsageExtractor()
    ex.feed(_sse({"type": "content_block_delta"}))
    assert ex.final_usage() is None


# ---- JSON extractor ----

def test_json_extractor_top_level_usage():
    ex = JSONUsageExtractor()
    body = json.dumps({"usage": {"input_tokens": 4, "output_tokens": 1}}).encode()
    ex.feed(body[:5]); ex.feed(body[5:])
    assert ex.final_usage()["input_tokens"] == 4


def test_json_extractor_oversize_gives_up():
    ex = JSONUsageExtractor(max_bytes=10)
    ex.feed(b"x" * 11)
    assert ex.final_usage() is None


def test_json_extractor_invalid_json():
    ex = JSONUsageExtractor()
    ex.feed(b"not json")
    assert ex.final_usage() is None


# ---- make_extractor / extract_model ----

def test_make_extractor_picks_by_content_type():
    assert isinstance(make_extractor("text/event-stream; charset=utf-8"), SSEUsageExtractor)
    assert isinstance(make_extractor("application/json"), JSONUsageExtractor)


def test_extract_model():
    assert extract_model("POST", json.dumps({"model": "claude-x"}).encode()) == "claude-x"
    assert extract_model("GET", b"") == "(no-body)"
    assert extract_model("POST", b"") == "(no-body)"
    assert extract_model("POST", b"garbage") == "(unknown)"
    assert extract_model("POST", json.dumps({"model": 3}).encode()) == "(unknown)"
