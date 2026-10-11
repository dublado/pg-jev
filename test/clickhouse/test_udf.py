#!/usr/bin/env python3
"""Drives clickhouse/jev_udf.py over stdin/stdout the way ClickHouse does, against the mock API on a free port.

Run: python3 -m unittest discover -s test/clickhouse -v
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import mock_api  # noqa: E402

UDF = os.path.join(HERE, "..", "..", "clickhouse", "jev_udf.py")


def chunk(rows):
    return "%d\n%s" % (len(rows), "".join(json.dumps(r) + "\n" for r in rows))


def noul(row, question):
    return {"row": row, "question": question, "kind": "noul", "options": []}


class UdfTest(unittest.TestCase):
    def setUp(self):
        self.server = mock_api.make_server(0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.tmp = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.tmp.name, "jev.json")
        self.write_config()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def write_config(self, **overrides):
        cfg = {"api_key": "test-key", "api_url": "http://127.0.0.1:%d/v1/systemone" % self.server.server_port,
               "cache_path": os.path.join(self.tmp.name, "cache.sqlite")}
        cfg.update(overrides)
        with open(self.config_path, "w") as f:
            json.dump(cfg, f)

    def run_udf(self, stdin, args=None):
        env = {k: v for k, v in os.environ.items() if k != "TYPESAFE_API_KEY"}
        return subprocess.run([sys.executable, UDF] + (args or [self.config_path]), input=stdin, env=env,
                              capture_output=True, text=True, timeout=120)

    def answers(self, proc):
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return [json.loads(json.loads(line)["result"]) for line in proc.stdout.splitlines()]

    def stats(self):
        with self.server.lock:
            return dict(self.server.stats)

    def test_noul_is_batched_and_keeps_row_order(self):
        rows = [noul(json.dumps({"id": i, "city": "Berlin" if i % 3 == 0 else "Lima"}), "the city is berlin")
                for i in range(45)]
        got = self.answers(self.run_udf(chunk(rows)))
        self.assertEqual([a["noul"] for a in got], [0.9 if i % 3 == 0 else 0.1 for i in range(45)])
        self.assertEqual(sorted(self.stats()["batch_sizes"]), [5, 20, 20])

    def test_plain_text_row_is_sent_as_string(self):
        got = self.answers(self.run_udf(chunk([noul("angry customer", "mentions angry"), noul("calm", "mentions angry")])))
        self.assertEqual([a["noul"] for a in got], [0.9, 0.1])

    def test_cache_survives_the_process(self):
        rows = [noul(json.dumps({"id": i}), "the id is 7") for i in range(30)]
        first = self.answers(self.run_udf(chunk(rows)))
        sent = self.stats()["requests"]
        self.assertEqual(sent, 2)
        second = self.answers(self.run_udf(chunk(rows)))
        self.assertEqual(second, first)
        self.assertEqual(self.stats()["requests"], sent)

    def test_cache_is_per_question_and_model(self):
        rows = [noul(json.dumps({"id": 1}), "the id is 1")]
        self.answers(self.run_udf(chunk(rows)))
        self.answers(self.run_udf(chunk([noul(json.dumps({"id": 1}), "the id is 2")])))
        self.assertEqual(self.stats()["requests"], 2)
        self.write_config(model="jev-other")
        self.answers(self.run_udf(chunk(rows)))
        self.assertEqual(self.stats()["requests"], 3)

    def test_duplicate_rows_are_sent_once(self):
        rows = [noul(json.dumps({"id": i % 4}), "the id is 3") for i in range(40)]
        got = self.answers(self.run_udf(chunk(rows)))
        self.assertEqual([a["noul"] for a in got], [0.9 if i % 4 == 3 else 0.1 for i in range(40)])
        self.assertEqual(self.stats()["rows"], 4)

    def test_several_chunks_in_one_process(self):
        a = [noul(json.dumps({"id": i}), "the id is 2") for i in range(3)]
        b = [noul(json.dumps({"id": i}), "the id is 2") for i in range(3, 5)]
        proc = self.run_udf(chunk(a) + chunk(b))
        self.assertEqual([x["noul"] for x in self.answers(proc)], [0.1, 0.1, 0.9, 0.1, 0.1])
        self.assertEqual(sorted(self.stats()["batch_sizes"]), [2, 3])

    def test_choice_and_score(self):
        row = json.dumps({"id": 1})  # the mock picks index len(row JSON) % n
        k = len(json.dumps(json.loads(row), sort_keys=True))
        rows = [{"row": row, "question": "which team?", "kind": "choice", "options": ["billing", "sales", "tech"]},
                {"row": row, "question": "how urgent?", "kind": "score", "options": ["low", "mid", "high", "max"]}]
        choice, score = self.answers(self.run_udf(chunk(rows)))
        self.assertEqual(choice["choice"], ["billing", "sales", "tech"][k % 3])
        self.assertEqual(score["score"], float(k % 4))
        self.assertEqual(self.stats()["requests"], 2)

    def test_api_error_fails_the_chunk(self):
        proc = self.run_udf(chunk([noul("x", "please trigger422")]))
        self.assertEqual(proc.returncode, 1)
        self.assertIn("jev: API error 422", proc.stderr)
        self.assertEqual(proc.stdout, "")

    def test_daily_budget_refuses_before_sending(self):
        self.write_config(max_rows_per_day=10)
        ok = self.run_udf(chunk([noul(json.dumps({"id": i}), "the id is 1") for i in range(8)]))
        self.assertEqual(ok.returncode, 0, ok.stderr)
        sent = self.stats()["rows"]
        self.assertEqual(sent, 8)
        over = self.run_udf(chunk([noul(json.dumps({"id": i}), "the id is 1") for i in range(100, 105)]))
        self.assertEqual(over.returncode, 1)
        self.assertIn("above max_rows_per_day = 10", over.stderr)
        self.assertEqual(self.stats()["rows"], sent)

    def test_cached_rows_do_not_count_against_the_budget(self):
        self.write_config(max_rows_per_day=10)
        rows = [noul(json.dumps({"id": i}), "the id is 1") for i in range(8)]
        self.answers(self.run_udf(chunk(rows)))
        self.answers(self.run_udf(chunk(rows)))

    def test_failed_rows_are_given_back_to_the_budget(self):
        self.write_config(max_rows_per_day=10)
        failed = self.run_udf(chunk([noul(json.dumps({"id": i}), "please trigger422") for i in range(8)]))
        self.assertEqual(failed.returncode, 1)
        ok = self.run_udf(chunk([noul(json.dumps({"id": i}), "the id is 1") for i in range(8)]))
        self.assertEqual(ok.returncode, 0, ok.stderr)

    def test_unreachable_api_fails_fast(self):
        closed = socket.socket()
        closed.bind(("127.0.0.1", 0))  # bound but not listening: connections are refused
        self.addCleanup(closed.close)
        self.write_config(api_url="http://127.0.0.1:%d/v1/systemone" % closed.getsockname()[1], concurrency=2)
        rows = [noul(json.dumps({"id": i}), "the id is 1") for i in range(400)]  # 20 batches
        started = time.time()
        proc = self.run_udf(chunk(rows))
        elapsed = time.time() - started
        self.assertEqual(proc.returncode, 1)
        self.assertIn("jev: API unreachable after retries", proc.stderr)
        # One retry cycle is about 32 s of backoff; 20 batches on 2 threads without fail-fast would take minutes.
        self.assertLess(elapsed, 45)

    def test_missing_key_for_typesafe_host(self):
        self.write_config(api_key="", api_url="https://api.typesafe.ai/v1/systemone")
        proc = self.run_udf(chunk([noul("x", "y")]))
        self.assertEqual(proc.returncode, 1)
        self.assertIn("jev: no API key", proc.stderr)

    def test_invalid_key_is_rejected(self):
        self.write_config(api_key="wrong")
        proc = self.run_udf(chunk([noul("x", "y")]))
        self.assertEqual(proc.returncode, 1)
        self.assertIn("jev: API error 401", proc.stderr)

    def test_choice_without_options_is_rejected(self):
        proc = self.run_udf(chunk([{"row": "x", "question": "q", "kind": "choice", "options": []}]))
        self.assertEqual(proc.returncode, 1)
        self.assertIn("needs a non-empty options array", proc.stderr)
        self.assertEqual(self.stats()["requests"], 0)

    def test_stats_report_usage(self):
        self.answers(self.run_udf(chunk([noul(json.dumps({"id": i}), "the id is 1") for i in range(25)])))
        self.answers(self.run_udf(chunk([noul(json.dumps({"id": i}), "the id is 1") for i in range(25)])))
        proc = self.run_udf("", ["--stats", self.config_path])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        day = json.loads(proc.stdout.splitlines()[-1])
        self.assertEqual((day["rows_sent"], day["requests"], day["cache_hits"]), (25, 2, 25))
        self.assertGreater(day["input_tokens"], 0)


if __name__ == "__main__":
    unittest.main()
