#!/usr/bin/env python3
"""ClickHouse executable UDF that judges rows with a Jev-compatible System One API.

A ClickHouse port of the request engine in sql/jev--*.sql. ClickHouse hands the
function whole blocks, so there is no read-ahead here: each chunk is deduplicated, looked up in the answer cache,
packed `batch_size` rows per request and judged over `concurrency` parallel keep-alive connections.

Protocol (executable_pool, format JSONEachRow, send_chunk_header = 1):
  stdin   <row count>\n, then one {"row": ..., "question": ..., "kind": ..., "options": [...]} per line
  stdout  one {"result": "<answer JSON>"} per input row, in input order

Usage:
  jev_udf.py [config.json]            serve chunks from stdin (how ClickHouse runs it)
  jev_udf.py --stats [config.json]    print usage per day from the cache database
"""
import hashlib
import http.client
import json
import os
import random
import sqlite3
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

VERSION = "0.1.0"
USD_PER_INPUT_TOKEN = 0.042 / 1_000_000  # jev-1.13 list price; output tokens are free
KINDS = ("noul", "score", "choice")
SQLITE_VARS = 500  # keys per IN (...) lookup

DEFAULTS = {
    "api_key": "",  # falls back to TYPESAFE_API_KEY in the server environment
    "api_url": "https://api.typesafe.ai/v1/systemone",
    "model": "jev-latest",
    "batch_size": 20,  # accuracy drops above ~20-25: the model finds rows[i] by position
    "concurrency": 16,
    "timeout": 30,
    "retries": 6,  # extra attempts per request on connection errors, 408, 429 and 5xx
    "keepalive": 600,
    "cache_path": "",  # SQLite file shared by the pool processes; empty = in memory, per process
    "cache_ttl": 0,  # seconds an answer stays valid; 0 = forever
    "max_rows_per_day": 0,  # spend guard on uncached rows sent to the API (UTC day); 0 = off
}


class JevError(Exception):
    pass


def load_config(path):
    cfg = dict(DEFAULTS)
    if path:
        try:
            with open(path) as f:
                loaded = json.load(f)
        except (OSError, ValueError) as e:
            raise JevError("jev: cannot read config %s: %s" % (path, e))
        unknown = sorted(set(loaded) - set(DEFAULTS))
        if unknown:
            raise JevError("jev: unknown config keys: %s" % ", ".join(unknown))
        cfg.update(loaded)
    url = urlsplit(cfg["api_url"])
    host = (url.hostname or "").lower()
    cfg["api_key"] = cfg["api_key"] or os.environ.get("TYPESAFE_API_KEY") or ""
    if not cfg["api_key"] and (host == "typesafe.ai" or host.endswith(".typesafe.ai")):
        raise JevError("jev: no API key. Set api_key in the config file or TYPESAFE_API_KEY in the server environment.")
    if url.scheme not in ("http", "https") or not host:
        raise JevError("jev: api_url must be an http(s) URL, got %r" % cfg["api_url"])
    cfg["batch_size"] = max(1, int(cfg["batch_size"]))
    cfg["concurrency"] = max(1, int(cfg["concurrency"]))
    cfg["timeout"] = float(cfg["timeout"])
    cfg["retries"] = max(0, int(cfg["retries"]))
    cfg["keepalive"] = float(cfg["keepalive"])
    cfg["cache_ttl"] = float(cfg["cache_ttl"])
    cfg["max_rows_per_day"] = int(cfg["max_rows_per_day"])
    cfg["scheme"], cfg["host"], cfg["port"] = url.scheme, url.hostname, url.port
    cfg["path"] = (url.path or "/") + ("?" + url.query if url.query else "")
    return cfg


# ---------------------------------------------------------------- answer cache and usage counters (main thread only)
class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path or ":memory:", timeout=30, isolation_level=None)
        if path:
            self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS answers (key TEXT PRIMARY KEY, answer TEXT NOT NULL,"
                        " created_at REAL NOT NULL) WITHOUT ROWID")
        self.db.execute("CREATE TABLE IF NOT EXISTS usage (day TEXT PRIMARY KEY, rows INTEGER NOT NULL DEFAULT 0,"
                        " requests INTEGER NOT NULL DEFAULT 0, input_tokens INTEGER NOT NULL DEFAULT 0,"
                        " cache_hits INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID")

    def get_many(self, keys, ttl):
        found, keys = {}, list(keys)
        oldest = time.time() - ttl if ttl > 0 else 0
        for i in range(0, len(keys), SQLITE_VARS):
            part = keys[i:i + SQLITE_VARS]
            sql = "SELECT key, answer FROM answers WHERE created_at >= ? AND key IN (%s)" % ",".join("?" * len(part))
            found.update(self.db.execute(sql, [oldest] + part).fetchall())
        return found

    def put_many(self, answers):
        now = time.time()
        self.db.execute("BEGIN IMMEDIATE")
        self.db.executemany("INSERT OR REPLACE INTO answers (key, answer, created_at) VALUES (?, ?, ?)",
                            [(k, a, now) for k, a in answers.items()])
        self.db.execute("COMMIT")

    def reserve(self, day, rows, limit):
        """Count `rows` against today's budget before sending them; refuse when that would pass the limit."""
        self.db.execute("BEGIN IMMEDIATE")
        used = self.db.execute("SELECT rows FROM usage WHERE day = ?", (day,)).fetchone()
        used = used[0] if used else 0
        if limit and used + rows > limit:
            self.db.execute("ROLLBACK")
            raise JevError("jev: this chunk would send %d rows to the API, %d already sent today,"
                           " above max_rows_per_day = %d" % (rows, used, limit))
        self.db.execute("INSERT INTO usage (day, rows) VALUES (?, ?) ON CONFLICT(day) DO UPDATE SET rows = rows + ?",
                        (day, rows, rows))
        self.db.execute("COMMIT")

    def release(self, day, rows):
        """Give back rows that were reserved but never answered."""
        self.db.execute("UPDATE usage SET rows = max(rows - ?, 0) WHERE day = ?", (rows, day))

    def record(self, day, requests, input_tokens, cache_hits):
        self.db.execute("INSERT INTO usage (day, requests, input_tokens, cache_hits) VALUES (?, ?, ?, ?)"
                        " ON CONFLICT(day) DO UPDATE SET requests = requests + ?, input_tokens = input_tokens + ?,"
                        " cache_hits = cache_hits + ?",
                        (day, requests, input_tokens, cache_hits, requests, input_tokens, cache_hits))

    def usage(self):
        return self.db.execute("SELECT day, rows, requests, input_tokens, cache_hits FROM usage ORDER BY day").fetchall()


# ---------------------------------------------------------------- request building (same contract as pg-jev)
def build_question(kind, question, options, i):
    ref = "rows[%d]" % i
    if kind == "noul":
        return {"type": "noul",
                "instructions": "Does the record `%s` satisfy the condition stated in `condition`?" % ref}
    if kind == "score":
        return {"type": "score", "instructions": "Rate the record `%s`: %s" % (ref, question), "criteria": options}
    return {"type": "choice", "instructions": "For the record `%s`: %s" % (ref, question),
            "criteria": {o: None for o in options}}


def request_body(cfg, kind, question, options, rows):
    state = {"condition": question, "rows": rows} if kind == "noul" else {"rows": rows}
    questions = {("r%d" % i): build_question(kind, question, options, i) for i in range(len(rows))}
    return json.dumps({"model": cfg["model"], "state": state, "questions": questions}).encode()


def parse_row(text):
    """Send the row as structured JSON when it is JSON (formatRowNoNewline, toJSONString), else as plain text."""
    try:
        return json.loads(text)
    except ValueError:
        return text


# ---------------------------------------------------------------- HTTP: one keep-alive connection per worker thread
_tls = threading.local()
_ssl_lock = threading.Lock()
_ssl_ctx = None


def ssl_context():
    global _ssl_ctx
    with _ssl_lock:
        if _ssl_ctx is None:
            _ssl_ctx = ssl.create_default_context()  # load the CA bundle once per process, not per connection
        return _ssl_ctx


def drop_connection():
    conn = getattr(_tls, "conn", None)
    if conn is not None:
        conn.close()
    _tls.conn = None


def connection(cfg):
    """Return (connection, reused). A connection idle past `keepalive` or opened for another endpoint is replaced."""
    endpoint = (cfg["scheme"], cfg["host"], cfg["port"])
    conn = getattr(_tls, "conn", None)
    if conn is not None and (_tls.endpoint != endpoint or time.time() - _tls.last_used >= cfg["keepalive"]):
        drop_connection()
        conn = None
    if conn is not None:
        return conn, True
    if cfg["scheme"] == "https":
        conn = http.client.HTTPSConnection(cfg["host"], cfg["port"], timeout=cfg["timeout"], context=ssl_context())
    else:
        conn = http.client.HTTPConnection(cfg["host"], cfg["port"], timeout=cfg["timeout"])
    _tls.conn, _tls.endpoint = conn, endpoint
    return conn, False


def retry_after_seconds(resp):
    v = resp.getheader("retry-after-ms")
    if v and v.strip().isdigit():
        return int(v) / 1000.0
    v = resp.getheader("retry-after")
    if v:
        try:
            return float(v)
        except ValueError:
            pass
    return None


def call_api(cfg, body, abort):
    headers = {"Content-Type": "application/json", "User-Agent": "clickhouse-jev/" + VERSION}
    if cfg["api_key"]:
        headers["Authorization"] = "Bearer " + cfg["api_key"]
    delay, last = 0.5, None
    for attempt in range(cfg["retries"] + 1):
        if abort.is_set():
            raise JevError("jev: skipped, another request of this chunk already failed")
        conn, reused = connection(cfg)
        try:
            conn.request("POST", cfg["path"], body=body, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
        except (http.client.HTTPException, OSError) as e:
            drop_connection()
            last = "%s: %s" % (type(e).__name__, e)
            if reused and attempt == 0:
                continue  # a keep-alive connection went stale: retry at once on a fresh one
            time.sleep(delay + random.random() * 0.25)
            delay = min(delay * 2, 8)
            continue
        _tls.last_used = time.time()
        if resp.will_close:
            drop_connection()
        if resp.status == 200:
            return json.loads(raw.decode())
        last = "%s %s" % (resp.status, raw.decode(errors="replace")[:300])
        if resp.status in (408, 429, 529) or resp.status >= 500:
            wait = retry_after_seconds(resp)
            time.sleep(min(wait if wait is not None else delay, 30) + random.random() * 0.25)
            delay = min(delay * 2, 8)
            continue
        raise JevError("jev: API error " + last)
    raise JevError("jev: API unreachable after retries: " + str(last))


def judge_batch(cfg, group, batch, abort):
    """Worker thread: judge one batch. Returns ({cache key: answer JSON}, input tokens). Touches no shared state."""
    kind, question, options = group
    body = request_body(cfg, kind, question, list(options), [parse_row(text) for _, text in batch])
    data = call_api(cfg, body, abort)
    answers = data.get("answers") or {}
    if any(("r%d" % i) not in answers for i in range(len(batch))):
        raise JevError("jev: API response is missing answers (%d of %d)" % (len(answers), len(batch)))
    judged = {key: json.dumps(answers["r%d" % i]) for i, (key, _) in enumerate(batch)}
    return judged, (data.get("usage") or {}).get("input_tokens", 0)


# ---------------------------------------------------------------- one chunk
def cache_key(cfg, kind, question, options, row):
    payload = json.dumps([cfg["model"], kind, question, options, row])
    return hashlib.sha1(payload.encode()).hexdigest()


def process_chunk(cfg, store, pool, rows):
    keys, pending = [], {}
    for r in rows:
        kind, question, options = r.get("kind") or "noul", r["question"], list(r.get("options") or [])
        if kind not in KINDS:
            raise JevError("jev: unknown kind %r (expected noul, score or choice)" % kind)
        if kind != "noul" and not options:
            raise JevError("jev: kind %r needs a non-empty options array" % kind)
        key = cache_key(cfg, kind, question, options, r["row"])
        keys.append(key)
        pending.setdefault((kind, question, tuple(options)), {})[key] = r["row"]

    result = store.get_many(set(keys), cfg["cache_ttl"])
    hits = sum(1 for k in keys if k in result)
    day = time.strftime("%Y-%m-%d", time.gmtime())
    todo = {g: [(k, t) for k, t in items.items() if k not in result] for g, items in pending.items()}
    n_todo = sum(len(items) for items in todo.values())
    if n_todo:
        store.reserve(day, n_todo, cfg["max_rows_per_day"])

    futures, abort = [], threading.Event()
    for group, items in todo.items():
        for i in range(0, len(items), cfg["batch_size"]):
            futures.append(pool.submit(judge_batch, cfg, group, items[i:i + cfg["batch_size"]], abort))
    fresh, tokens, requests, error = {}, 0, 0, None
    for fut in futures:
        if fut.cancelled():
            continue
        try:
            judged, used = fut.result()
        except Exception as e:  # keep what the other requests answered, so a retry only pays for the failed part
            if error is None:
                # Fail fast: without this every remaining batch would run its own full retry cycle first.
                error = e
                abort.set()
                for pending_fut in futures:
                    pending_fut.cancel()
            continue
        fresh.update(judged)
        tokens += used
        requests += 1
    if fresh:
        store.put_many(fresh)
    store.record(day, requests, tokens, hits)
    if error is not None:
        store.release(day, n_todo - len(fresh))
        raise error
    result.update(fresh)
    return [result[k] for k in keys]


# ---------------------------------------------------------------- entry points
class Runtime:
    """Config, cache and thread pool, rebuilt when the config file changes."""

    def __init__(self, config_path):
        self.config_path, self.mtime, self.cfg, self.store, self.pool = config_path, None, None, None, None

    def refresh(self):
        mtime = os.stat(self.config_path).st_mtime if self.config_path and os.path.exists(self.config_path) else 0
        if self.cfg is not None and mtime == self.mtime:
            return
        old = self.cfg
        self.cfg, self.mtime = load_config(self.config_path), mtime
        if old is None or old["cache_path"] != self.cfg["cache_path"]:
            self.store = Store(self.cfg["cache_path"])
        if old is None or old["concurrency"] != self.cfg["concurrency"]:
            if self.pool is not None:
                self.pool.shutdown(wait=False)
            self.pool = ThreadPoolExecutor(max_workers=self.cfg["concurrency"], thread_name_prefix="jev")


def serve(config_path):
    runtime, stdin = Runtime(config_path), sys.stdin.buffer
    while True:
        header = stdin.readline()
        if not header.strip():
            return 0
        rows = [json.loads(stdin.readline()) for _ in range(int(header))]
        runtime.refresh()
        answers = process_chunk(runtime.cfg, runtime.store, runtime.pool, rows)
        sys.stdout.write("".join(json.dumps({"result": a}) + "\n" for a in answers))
        sys.stdout.flush()


def print_stats(config_path):
    store = Store(load_config(config_path)["cache_path"])
    for day, rows, requests, input_tokens, cache_hits in store.usage():
        print(json.dumps({"day": day, "rows_sent": rows, "requests": requests, "input_tokens": input_tokens,
                          "cache_hits": cache_hits,
                          "estimated_cost_usd": round(input_tokens * USD_PER_INPUT_TOKEN, 6)}))
    return 0


def main(argv):
    args = [a for a in argv[1:] if a != "--stats"]
    config_path = args[0] if args else os.environ.get("JEV_CONFIG", "")
    try:
        code = print_stats(config_path) if "--stats" in argv else serve(config_path)
    except JevError as e:
        sys.stderr.write(str(e) + "\n")
        code = 1
    except Exception as e:
        sys.stderr.write("jev: %s: %s\n" % (type(e).__name__, e))
        code = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)  # worker threads may still be sleeping in a retry; do not wait for them


if __name__ == "__main__":
    main(sys.argv)
