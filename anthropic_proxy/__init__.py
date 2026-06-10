"""anthropic_proxy — queueing reverse proxy for LLM APIs.

Forwards every path/header verbatim, so it fronts both the Anthropic Messages
API (/v1/messages) and the OpenAI-compatible API (/v1/chat/completions,
/v1/responses) through one shared queue, adding concurrency tiers, retry on
rate limits, token/cost metrics, and a live dashboard.

Run it with `python -m anthropic_proxy` (or the `proxy.py` shim).
"""
