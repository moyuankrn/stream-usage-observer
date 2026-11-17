---
name: stream-usage-observer
description: Diagnose missing or inconsistent token and prompt-cache usage in JSON or SSE AI responses, including split UTF-8 and gzip or deflate streams. Use when instrumenting an AI gateway or reconciling its reported usage.
---

# Stream usage observer

Use the bundled `usage_observer.py` to observe provider-reported usage while passing the original response chunks through unchanged. It requires Python 3.9+ and no third-party packages. Work from a redacted captured response or the user's gateway code.

## Work on the user's request

1. Identify the response format (`application/json` or `text/event-stream`), `Content-Encoding`, provider, and any gateway-specific usage aliases. Check whether the provider actually emits usage in this response mode before interpreting an empty result.
2. Feed raw response bytes to `UsageObserver(content_type=..., content_encoding=...)` as they arrive; forward the same bytes to the client. Call `finish()` after the stream ends. If using a local file, the bundled CLI `inspect` command can show normalized counters without printing answer text.
3. Call `normalize(observer.usage)` and distinguish missing fields (`null`) from reported zero. For Anthropic-style usage, `input_tokens` is combined with cache reads and writes; OpenAI-style `prompt_tokens` is already inclusive. If the gateway uses different semantics, adapt it before normalizing and document that choice.
4. When the user wants history, call `record(path, model, channel, metric)` with request status and timings supplied by the caller, then `stats(...)`. The SQLite file stores counters and metadata, not raw responses. Check the Boolean result from `record`; report a storage failure without claiming that metrics were saved.
5. Validate with one known response sample: compare raw provider fields to the normalized values, verify the original stream bytes are unchanged, and explain any missing usage. A terminal SSE marker does not by itself establish HTTP success.

## Boundaries

- Unsupported encodings, malformed events, and overlarge buffers can disable observation; the response should still be delivered by the host application. Do not invent counters or costs for those cases.
- A cache hit rate is computed only from requests with both input and cache-read values. Provider token counters do not prove billing savings; reconcile against provider billing before making a cost claim.
- The included module does not send data over the network. Treat response captures and local metrics files as private.

The reference implementation is MIT licensed: https://github.com/moyuankrn/stream-usage-observer .
