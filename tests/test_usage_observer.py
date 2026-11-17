import contextlib
import gzip
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import usage_observer as usage


class UsageObserverTests(unittest.TestCase):
    def test_split_gzip_sse_keeps_usage_and_drops_answer(self):
        events = [
            {"type": "message_start", "message": {"usage": {
                "input_tokens": 20, "cache_read_input_tokens": 80, "cache_creation_input_tokens": 10}}},
            {"type": "content_block_delta", "delta": {"text": "private answer 🌊"}},
            {"type": "message_delta", "usage": {"output_tokens": 4}},
            {"type": "message_stop"},
        ]
        wire = gzip.compress("".join("data: " + json.dumps(event, ensure_ascii=False) + "\n\n" for event in events).encode())
        observer = usage.UsageObserver("text/event-stream", "gzip")
        for start in range(0, len(wire), 7):
            observer.feed(wire[start:start + 7])
        observer.finish()
        self.assertTrue(observer.stopped)
        self.assertEqual(usage.normalize(observer.usage), {
            "input_tokens": 110, "output_tokens": 4, "cached_tokens": 80,
            "cache_write_tokens": 10, "total_tokens": 114})
        self.assertEqual(observer.buffer, "")
        self.assertNotIn("private answer", str(observer.__dict__))

    def test_json_response_and_gateway_aliases(self):
        body = json.dumps({"choices": [{"message": {"content": "private"}}], "usage": {
            "prompt_tokens": 5547, "completion_tokens": 14, "total_tokens": 5561,
            "input_tokens": 5547, "output_tokens": 0, "usage_semantic": "openai",
            "prompt_tokens_details": {"cached_tokens": 4066, "cached_creation_tokens": 451},
            "claude_cache_creation_5_m_tokens": 451,
        }}).encode()
        observer = usage.UsageObserver("application/json")
        for chunk in (body[:13], body[13:]):
            observer.feed(chunk)
        observer.finish()
        self.assertTrue(observer.stopped)
        self.assertEqual(usage.normalize(observer.usage), {
            "input_tokens": 5547, "output_tokens": 14, "cached_tokens": 4066,
            "cache_write_tokens": 451, "total_tokens": 5561})
        self.assertNotIn("private", str(observer.__dict__))
        self.assertIsNone(usage.normalize({})["cached_tokens"])
        self.assertEqual(usage.normalize({"prompt_tokens_details": {"cached_tokens": 0},
                                          "cache_read_input_tokens": 7})["cached_tokens"], 0)

    def test_missing_separators_and_invalid_content(self):
        observer = usage.UsageObserver("text/event-stream")
        observer.feed(b'data: {"usage":{"prompt_tokens":10}}\ndata: [DONE]\n')
        observer.finish()
        self.assertEqual(observer.usage["prompt_tokens"], 10)
        self.assertTrue(observer.stopped)
        bad = usage.UsageObserver("application/json", "unsupported")
        bad.feed(b"private")
        self.assertFalse(bad.enabled)

    def test_storage_summary_tracks_unknown_and_permissions(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "usage.sqlite3"
            ts = 1767286800
            self.assertTrue(usage.record(db, "model-a", "web", {"ts": ts, "status": "complete", "usage": {
                "prompt_tokens": 1000, "completion_tokens": 20,
                "prompt_tokens_details": {"cached_tokens": 800, "cached_creation_tokens": 50}}}))
            self.assertTrue(usage.record(db, "model-a", "web", {"ts": ts + 1, "status": "cancelled", "usage": {}}))
            self.assertTrue(usage.record(db, "model-b", "call", {"ts": ts + 2, "status": "complete", "usage": {
                "prompt_tokens": 100, "completion_tokens": 10}}))
            result = usage.stats(db, days=7, tz_name="Asia/Shanghai", now=ts + 10)
            summary = result["summary"]
            self.assertEqual((summary["attempts"], summary["missing_usage"], summary["cache_reported"]), (3, 1, 1))
            self.assertEqual(summary["hit_rate"], 80.0)
            self.assertEqual(summary["input_tokens"], 1100)
            self.assertEqual(len(result["by_model"]), 2)
            self.assertEqual(usage.stats(db, days=7, channel="call", now=ts + 10)["summary"]["attempts"], 1)
            self.assertEqual(db.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("raw_usage", db.read_bytes().decode("latin1"))

    def test_cli_inspect_prints_only_normalized_fields(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "response.sse"
            path.write_text('data: {"choices":[{"delta":{"content":"private answer"}}]}\n\n'
                            'data: {"usage":{"prompt_tokens":12,"completion_tokens":3}}\n\n'
                            'data: [DONE]\n\n')
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(usage.main(["inspect", str(path)]), 0)
            self.assertEqual(json.loads(output.getvalue())["total_tokens"], 15)
            self.assertNotIn("private answer", output.getvalue())
            with patch.object(usage, "MAX_BUFFER", 10):
                oversized = usage.UsageObserver("application/json")
                oversized.feed(b"a" * 11)
                self.assertFalse(oversized.enabled)


if __name__ == "__main__":
    unittest.main()
