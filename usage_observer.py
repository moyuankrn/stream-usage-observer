#!/usr/bin/env python3
"""Observe provider-reported token usage in JSON or streamed SSE responses."""

import argparse
import codecs
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time
import zlib
from zoneinfo import ZoneInfo


MAX_BUFFER = 2 * 1024 * 1024
TOKEN_FIELDS = ("input_tokens", "output_tokens", "cached_tokens", "cache_write_tokens", "total_tokens")


class UsageObserver:
    """Feed raw response chunks; only the provider's usage object is retained."""

    def __init__(self, content_type="text/event-stream", content_encoding=""):
        self.sse = "text/event-stream" in content_type.lower()
        encoding = content_encoding.lower().strip()
        self.enabled = encoding in ("", "identity", "gzip", "deflate")
        self.compression = (zlib.decompressobj(16 + zlib.MAX_WBITS if encoding == "gzip" else zlib.MAX_WBITS)
                            if encoding in ("gzip", "deflate") else None)
        self.decoder = codecs.getincrementaldecoder("utf-8")()
        self.buffer = ""
        self.fields = []
        self.usage = {}
        self.stopped = False

    def _event(self, event):
        if event == "[DONE]":
            self.stopped = True
            return
        if not isinstance(event, dict):
            return
        nested = event.get("message")
        if isinstance(nested, dict) and isinstance(nested.get("usage"), dict):
            self.usage.update(nested["usage"])
        if isinstance(event.get("usage"), dict):
            self.usage.update(event["usage"])
        if event.get("type") == "message_stop" or not self.sse:
            self.stopped = True

    def _parse_fields(self):
        if not self.fields:
            return
        value = "\n".join(self.fields)
        if value.strip() == "[DONE]":
            self.fields.clear()
            self._event("[DONE]")
            return
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            return
        self.fields.clear()
        self._event(event)

    def _line(self, line):
        line = line.rstrip("\r").removeprefix("\ufeff")
        if line.startswith("data:"):
            self.fields.append(line[5:].removeprefix(" "))
            # Some compatible gateways omit the blank SSE separator.
            self._parse_fields()
        elif not line:
            self._parse_fields()

    def _text(self, value):
        self.buffer += value
        if self.sse:
            while "\n" in self.buffer:
                line, self.buffer = self.buffer.split("\n", 1)
                self._line(line)
        if len(self.buffer) > MAX_BUFFER or sum(map(len, self.fields)) > MAX_BUFFER:
            self.enabled = False
            self.buffer = ""
            self.fields.clear()

    def feed(self, chunk):
        if not self.enabled:
            return
        try:
            raw = self.compression.decompress(chunk) if self.compression else chunk
            self._text(self.decoder.decode(raw))
        except (UnicodeError, zlib.error):
            self.enabled = False
            self.buffer = ""
            self.fields.clear()

    def finish(self):
        if not self.enabled:
            return
        try:
            tail = self.compression.flush() if self.compression else b""
            self._text(self.decoder.decode(tail, final=True))
            if self.sse:
                if self.buffer:
                    self._line(self.buffer)
                self._parse_fields()
            elif self.buffer:
                try:
                    self._event(json.loads(self.buffer))
                except json.JSONDecodeError:
                    pass
        except (UnicodeError, zlib.error):
            self.enabled = False
        finally:
            self.buffer = ""
            self.fields.clear()


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    if value < 0 or value > 9_223_372_036_854_775_807 or int(value) != value:
        return None
    return int(value)


def _first(*values):
    return next((number for value in values if (number := _number(value)) is not None), None)


def normalize(usage):
    """Normalize OpenAI/Anthropic usage aliases without inventing missing values."""
    data = usage if isinstance(usage, dict) else {}
    details = data.get("prompt_tokens_details") or data.get("input_tokens_details") or {}
    if not isinstance(details, dict):
        details = {}
    cached = _first(details.get("cached_tokens"), data.get("cache_read_input_tokens"), data.get("prompt_cache_hit_tokens"))
    write = _first(details.get("cached_creation_tokens"), details.get("cache_write_tokens"),
                   details.get("cache_creation_tokens"), data.get("cache_creation_input_tokens"))
    if write is None:
        parts = [_number(data.get(name)) for name in ("claude_cache_creation_5_m_tokens", "claude_cache_creation_1_h_tokens")]
        if any(part is not None for part in parts):
            write = sum(part or 0 for part in parts)
    input_tokens = _number(data.get("prompt_tokens"))
    if input_tokens is None:
        input_tokens = _number(data.get("input_tokens"))
        if input_tokens is not None and data.get("usage_semantic") != "openai":
            input_tokens += (cached or 0) + (write or 0)
    output_tokens = _first(data.get("completion_tokens"), data.get("output_tokens"))
    total_tokens = _number(data.get("total_tokens"))
    if total_tokens is None and input_tokens is not None and output_tokens is not None:
        total_tokens = input_tokens + output_tokens
    return {"input_tokens": input_tokens, "output_tokens": output_tokens,
            "cached_tokens": cached, "cache_write_tokens": write, "total_tokens": total_tokens}


def database(path):
    path = Path(path)
    if path.is_symlink():
        raise ValueError("database path must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        except FileExistsError:
            pass
    db = sqlite3.connect(path, timeout=5)
    try:
        os.chmod(path, 0o600)
        db.execute("""CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY, ts REAL, model TEXT, channel TEXT, status TEXT,
            elapsed_ms INTEGER, first_token_ms INTEGER, input_tokens INTEGER,
            output_tokens INTEGER, cached_tokens INTEGER, cache_write_tokens INTEGER,
            total_tokens INTEGER)""")
        db.execute("CREATE INDEX IF NOT EXISTS requests_ts ON requests(ts)")
        return db
    except Exception:
        db.close()
        raise


def record(path, model, channel, metric):
    """Best-effort write; returns False if stats storage fails."""
    values = normalize(metric.get("usage"))
    try:
        with database(path) as db:
            db.execute("""INSERT INTO requests(ts,model,channel,status,elapsed_ms,first_token_ms,
                input_tokens,output_tokens,cached_tokens,cache_write_tokens,total_tokens)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (metric.get("ts", time.time()), model, channel, metric.get("status", "unknown"),
                 metric.get("elapsed_ms"), metric.get("first_token_ms"),
                 *(values[name] for name in TOKEN_FIELDS)))
        return True
    except (sqlite3.Error, OSError, ValueError):
        return False


def _summary(rows):
    result = {"attempts": len(rows),
              "reported": sum(row["input_tokens"] is not None or row["output_tokens"] is not None for row in rows),
              "cache_reported": sum(row["input_tokens"] is not None and row["cached_tokens"] is not None for row in rows),
              "completed": sum(row["status"] == "complete" for row in rows),
              "cancelled": sum(row["status"] == "cancelled" for row in rows)}
    result["missing_usage"] = result["attempts"] - result["reported"]
    for name in TOKEN_FIELDS:
        known = [row[name] for row in rows if row[name] is not None]
        result[name] = sum(known) if known else None
    paired = [row for row in rows if row["input_tokens"] is not None and row["cached_tokens"] is not None]
    denominator = sum(row["input_tokens"] for row in paired)
    result["cache_input_tokens"] = denominator if paired else None
    result["cache_read_known"] = sum(row["cached_tokens"] for row in paired) if paired else None
    result["hit_rate"] = round(100 * result["cache_read_known"] / denominator, 1) if denominator else None
    return result


def stats(path, days=1, tz_name="UTC", channel=None, now=None):
    if not 0 <= days <= 365:
        raise ValueError("days must be 0..365; 0 means all history")
    tz = ZoneInfo(tz_name)
    now = time.time() if now is None else now
    cutoff = now - days * 86400 if days else 0
    with database(path) as db:
        db.row_factory = sqlite3.Row
        if channel is None:
            rows = [dict(row) for row in db.execute("SELECT * FROM requests WHERE ts >= ? ORDER BY ts,id", (cutoff,))]
        else:
            rows = [dict(row) for row in db.execute("SELECT * FROM requests WHERE ts >= ? AND channel = ? ORDER BY ts,id", (cutoff, channel))]
    by_model = [{"model": model, **_summary([row for row in rows if row["model"] == model])}
                for model in sorted({row["model"] for row in rows})]
    buckets = {}
    for row in rows:
        local = datetime.fromtimestamp(row["ts"], tz)
        bucket = local.strftime("%Y-%m-%d %H:00 %z") if days == 1 else local.strftime("%Y-%m-%d")
        buckets.setdefault(bucket, []).append(row)
    series = [{"bucket": bucket, **_summary(group)} for bucket, group in sorted(buckets.items())]
    public = ("id", "ts", "model", "channel", "status", "elapsed_ms", "first_token_ms", *TOKEN_FIELDS)
    return {"summary": _summary(rows), "by_model": by_model, "series": series,
            "recent": [{key: row[key] for key in public} for row in reversed(rows[-20:])],
            "timezone": tz.key, "days": days}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="Read a local response file and print normalized usage")
    inspect.add_argument("file", type=Path)
    inspect.add_argument("--content-type", default="text/event-stream")
    inspect.add_argument("--encoding", default="")
    inspect.add_argument("--db", type=Path, help="Optionally record the usage in SQLite")
    inspect.add_argument("--model", default="unknown")
    inspect.add_argument("--channel", default="inspect")
    report = commands.add_parser("stats", help="Summarize a local SQLite usage database")
    report.add_argument("--db", type=Path, required=True)
    report.add_argument("--days", type=int, default=1)
    report.add_argument("--tz", default="UTC")
    report.add_argument("--channel")
    args = parser.parse_args(argv)
    try:
        if args.command == "stats":
            result = stats(args.db, args.days, args.tz, args.channel)
        else:
            observer = UsageObserver(args.content_type, args.encoding)
            with args.file.open("rb") as source:
                for chunk in iter(lambda: source.read(4096), b""):
                    observer.feed(chunk)
            observer.finish()
            result = normalize(observer.usage)
            if args.db and not record(args.db, args.model, args.channel,
                                      {"status": "complete" if observer.stopped else "incomplete", "usage": observer.usage}):
                raise OSError("could not write usage database")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(f"usage observer: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
