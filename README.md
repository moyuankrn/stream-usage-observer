# Stream Usage Observer

A small Python observer extracted from a chat gateway. Feed it raw JSON or Server-Sent Events response chunks to collect provider-reported token and prompt-cache usage. It handles split UTF-8, gzip/deflate, and SSE records that omit blank separators. The response body can pass through unchanged; the observer does not persist answer text.

Python 3.9 or newer. No third-party dependencies.

```python
from usage_observer import UsageObserver, normalize, record, stats

observer = UsageObserver(content_type="text/event-stream", content_encoding="gzip")
for chunk in upstream_chunks:
    observer.feed(chunk)
    yield chunk  # Pass the original bytes to your client.
observer.finish()

tokens = normalize(observer.usage)
record("./usage.sqlite3", model="example-model", channel="web", metric={
    "status": "complete" if observer.stopped else "incomplete",
    "usage": observer.usage,
})
print(stats("./usage.sqlite3", days=7, tz_name="UTC")["summary"])
```

`record` writes normalized token counts and request metadata to SQLite, with file mode `0600`. It does **not** store raw provider responses or a raw usage blob. The method returns `False` if statistics storage fails, so a failed metrics write need not break a user response. Callers should set the request status and timing from their own stream lifecycle; an SSE terminal marker alone does not prove a successful HTTP request.

Missing usage remains `null`, distinct from a reported zero. Cache hit rate only uses requests that report both input and cache-read tokens. Anthropic-style `input_tokens` excludes cache reads/writes, so those are added; OpenAI-style `prompt_tokens` is treated as already inclusive. A gateway that uses different semantics may require an adapter before `normalize`.

The CLI can inspect a saved response file without printing its answer text:

```sh
python3 usage_observer.py inspect response.sse
python3 usage_observer.py stats --db ./usage.sqlite3 --days 7 --tz Asia/Shanghai
```

`inspect` can optionally take `--content-type application/json`, `--encoding gzip`, and `--db ./usage.sqlite3` to save the result. Input files and databases are ignored by `.gitignore`; keep them private.

```sh
PYTHONPATH=. python3 -m unittest discover -s tests -v
```

MIT licensed. See [LICENSE](LICENSE).
