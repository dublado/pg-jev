#!/usr/bin/env python3
"""Deterministic stand-in for https://api.typesafe.ai/v1/systemone.

Variant of test/mock_api.py for the ClickHouse UDF tests: same rules, plus a kernel-chosen port and GET /stats.

Rules (so expected output is stable):
  noul   -> 0.9 if the LAST word of `state.condition` appears (case-insensitively) in the row JSON, else 0.1
  score  -> level index = length of the row JSON modulo number of levels (one-hot probabilities)
  choice -> option index  = length of the row JSON modulo number of options
  A condition containing "trigger422" returns HTTP 422 (non-retryable error path).
  usage.input_tokens = len(request body) // 4
  Auth: "Bearer test-key" is required, except under /local/, which also accepts no Authorization header.
  GET /stats returns {"requests": n, "rows": n, "batch_sizes": [...]} for the requests answered so far.
Run: python3 test/clickhouse/mock_api.py [port] [host]   (default 8765 on 127.0.0.1; port 0 lets the kernel choose)
"""
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive, so connection reuse is exercised

    def log_message(self, *a):  # quiet
        pass

    def do_GET(self):
        with self.server.lock:
            stats = dict(self.server.stats)
        self._send(200, stats)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        auth = self.headers.get("Authorization")
        if auth != "Bearer test-key" and not (self.path.startswith("/local/") and auth is None):
            return self._send(401, {"error": "invalid api key"})
        req = json.loads(body)
        state, questions = req["state"], req["questions"]
        rows = state.get("rows", [])
        cond = state.get("condition", "")
        if "trigger422" in cond:
            return self._send(422, {"error": "mock validation failure"})
        needle = cond.split()[-1].lower() if cond.split() else ""
        answers = {}
        for qid, q in questions.items():
            i = int(qid[1:])
            row_json = json.dumps(rows[i], sort_keys=True)
            if q["type"] == "noul":
                answers[qid] = {"type": "noul", "noul": 0.9 if needle and needle in row_json.lower() else 0.1}
            elif q["type"] == "score":
                levels = q["criteria"]
                k = len(row_json) % len(levels)
                answers[qid] = {"type": "score", "score": float(k),
                                "legend": {str(j): l for j, l in enumerate(levels)},
                                "probabilities": {str(j): (1.0 if j == k else 0.0) for j in range(len(levels))},
                                "confidence": 1.0}
            elif q["type"] == "choice":
                opts = list(q["criteria"].keys())
                k = len(row_json) % len(opts)
                answers[qid] = {"type": "choice", "choice": opts[k],
                                "probabilities": {o: (1.0 if j == k else 0.0) for j, o in enumerate(opts)},
                                "confidence": 1.0}
        with self.server.lock:
            self.server.stats["requests"] += 1
            self.server.stats["rows"] += len(rows)
            self.server.stats["batch_sizes"] = self.server.stats["batch_sizes"] + [len(rows)]
        self._send(200, {"model": "jev-mock", "answers": answers,
                         "usage": {"input_tokens": len(body) // 4, "output_tokens": len(answers)}})

    def _send(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def make_server(port=0, host="127.0.0.1"):
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.lock = threading.Lock()
    server.stats = {"requests": 0, "rows": 0, "batch_sizes": []}
    return server


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    make_server(port, host).serve_forever()
