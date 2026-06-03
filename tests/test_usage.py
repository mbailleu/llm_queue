"""Usage-block parsing across the three provider wire formats."""
from anthropic_proxy.usage import (
    normalize_usage, extract_model, make_extractor,
    SSEUsageExtractor, JSONUsageExtractor,
)


def test_anthropic_shape():
    u = normalize_usage({
        "input_tokens": 100, "output_tokens": 20,
        "cache_creation_input_tokens": 5, "cache_read_input_tokens": 7,
    })
    assert u == {"input_tokens": 100, "output_tokens": 20,
                 "cache_creation_input_tokens": 5, "cache_read_input_tokens": 7}


def test_openai_chat_splits_cached_out_of_prompt():
    u = normalize_usage({
        "prompt_tokens": 100, "completion_tokens": 30,
        "prompt_tokens_details": {"cached_tokens": 40},
    })
    assert u["input_tokens"] == 60 and u["cache_read_input_tokens"] == 40
    assert u["output_tokens"] == 30 and u["cache_creation_input_tokens"] == 0


def test_openai_responses_cached_subset():
    u = normalize_usage({
        "input_tokens": 100, "output_tokens": 10,
        "input_tokens_details": {"cached_tokens": 25},
    })
    assert u["input_tokens"] == 75 and u["cache_read_input_tokens"] == 25


def test_normalize_none():
    assert normalize_usage(None) is None
    assert normalize_usage({"unrelated": 1}) is None


def test_extract_model():
    assert extract_model("POST", b'{"model":"claude-x"}') == "claude-x"
    assert extract_model("GET", b"") == "(no-body)"
    assert extract_model("POST", b"not json") == "(unknown)"


def test_json_extractor_roundtrip():
    ex = make_extractor("application/json")
    assert isinstance(ex, JSONUsageExtractor)
    ex.feed(b'{"usage": {"input_tokens": 3, "output_tokens": 4}}')
    assert ex.final_usage()["input_tokens"] == 3


def test_sse_extractor_anthropic_stream():
    ex = make_extractor("text/event-stream")
    assert isinstance(ex, SSEUsageExtractor)
    ex.feed(b'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":50,"cache_read_input_tokens":10}}}\n\n')
    ex.feed(b'event: message_delta\ndata: {"type":"message_delta","usage":{"output_tokens":12}}\n\n')
    u = ex.final_usage()
    assert u["input_tokens"] == 50 and u["output_tokens"] == 12 and u["cache_read_input_tokens"] == 10
