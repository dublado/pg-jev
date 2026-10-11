# jev for ClickHouse

The same plain-language predicates, for ClickHouse. PostgreSQL extensions cannot be loaded into ClickHouse, so
this is a port of the request engine as an [executable UDF](https://clickhouse.com/docs/sql-reference/functions/udf):
persistent Python processes that the server feeds with blocks of rows.

```sql
SELECT id FROM tickets
WHERE jev(formatRowNoNewline('JSONEachRow', subject, body), 'the customer is furious');

SELECT jev_choice(body, 'which team should handle this?', ['billing', 'technical', 'sales']) AS team, count()
FROM tickets GROUP BY team;
```

## What changed from the PostgreSQL extension

| PostgreSQL | ClickHouse |
| --- | --- |
| `plpython3u` function inside the backend | `executable_pool` UDF: a pool of persistent Python processes |
| read-ahead of the table by TID | none needed: ClickHouse already hands the function whole blocks |
| answer cache in memory, per session | SQLite cache shared by the pool, kept across queries and restarts |
| `jev.*` GUCs per session | a `jev.json` file on the server, reread when it changes |
| `jev.max_rows_per_statement` | `max_rows_per_day`: a daily budget of rows sent to the API |

Unchanged: the API contract, 20 rows per request, 16 parallel requests over keep-alive connections, retries
that honour `Retry-After`, and the function names.

## Functions

| Function | Returns | Purpose |
| --- | --- | --- |
| `jev(row, condition)` | Bool | `WHERE` predicate at the 0.5 threshold |
| `jev_prob(row, condition)` | Float64 | Probability 0..1; for another threshold write `jev_prob(...) >= 0.7` |
| `jev_choice(row, question, options)` | String | The most likely option |
| `jev_score(row, question, levels)` | Float64 | Probability-weighted position on ordered levels (0 .. n-1) |
| `jev_score_norm(row, question, levels)` | Float64 | Same, normalised to 0..1 |
| `jev_confidence(row, question, kind, options)` | Float64 | Confidence of a `choice`/`score` answer |
| `jev_eval(row, question, kind, options)` | String | Raw answer JSON. This is the executable UDF; the others are SQL on top |

`row` is a String: `formatRowNoNewline('JSONEachRow', col1, col2)` for named columns, or a text column on its
own. Send only the columns the judgment needs.

## Install

Requires access to the server host; managed services that do not allow executable UDFs cannot run it.

1. `python3` on the ClickHouse host (standard library only; the official Docker image ships without Python).
2. Outbound HTTPS from the host to `api.typesafe.ai:443` and a TypeSafe API key, or a local Jev-compatible
   server in `api_url`.
3. Copy `jev_udf.py` to the `user_scripts` directory (default `/var/lib/clickhouse/user_scripts/`), executable
   by the `clickhouse` user.
4. Create `/etc/clickhouse-server/jev.json` from `jev.example.json`, readable only by `clickhouse`.
5. Copy `jev_function.xml` to `/etc/clickhouse-server/`. No restart is needed: the server picks it up within
   seconds, and `SYSTEM RELOAD FUNCTIONS` forces it.
6. Run `jev_wrappers.sql` once.

`jev_udf.py --stats /etc/clickhouse-server/jev.json` prints rows sent, requests, tokens and estimated cost per
day.

## Settings (`jev.json`)

| Key | Default | Meaning |
| --- | --- | --- |
| `api_key` | env `TYPESAFE_API_KEY` | Required only for `*.typesafe.ai` hosts |
| `api_url` | `https://api.typesafe.ai/v1/systemone` | Endpoint |
| `model` | `jev-latest` | Model name or pinned version. Part of the cache key |
| `batch_size` | `20` | Rows per request |
| `concurrency` | `16` | Parallel requests per pool process |
| `timeout` | `30` | Seconds per request |
| `retries` | `6` | Extra attempts on connection errors, 408, 429 and 5xx |
| `keepalive` | `600` | Seconds a connection may idle before it is reopened |
| `cache_path` | empty | SQLite file for the answer cache; empty keeps it in memory, per process |
| `cache_ttl` | `0` | Seconds an answer stays valid; 0 is forever |
| `max_rows_per_day` | `0` (off) | Refuse a chunk that would pass this many uncached rows in a UTC day |

## Caveats

- **`LIMIT` does not bound the cost.** A block is answered as a whole: `LIMIT 3` over `numbers()` with the
  default block size sent 65,409 rows; with `max_block_size = 100` it sent 100. A 1,000-row MergeTree table was
  sent whole even with `max_block_size = 100`. To sample, put the `LIMIT` in a subquery before `jev()` and set
  `max_rows_per_day`.
- **Never in an insert-triggered materialized view.** It would run on every inserted block, and an API outage
  would block ingestion. Use a scheduled `INSERT ... SELECT` or a refreshable materialized view, and store the
  result: a stored answer is paid for once.
- Every user with `SELECT` can call the functions, and the daily budget is per server, not per user.
- With `model = jev-latest` cached answers outlive a model release. Pin the model or set `cache_ttl`.
- Not tested on a cluster: each node would hold its own cache and its own daily budget.
- Row contents are sent to a third-party API, as with the PostgreSQL extension.

## Measured

Against the live API (`jev-latest`, 2026-10-10) inside ClickHouse 26.1.12, with synthetic texts: 2,000 distinct
rows judged in 2.4 s over 100 requests, 2,000 of 2,000 correct on a yes/no condition; the second run took
0.09 s. Two questions over those rows used 291,900 input tokens (73 per row, short texts), about $0.012 at list
price. `test/clickhouse/real_api_check.py` scored 20 of 20 on a yes/no condition and 24 of 24 on a six-way
classification in Portuguese and English, and held with shuffled rows and with the condition inverted. A
language-detection question was wrong on 83 of 600 Portuguese rows that carried an English prefix.

## Tests

```bash
python3 -m unittest discover -s test/clickhouse -v   # the UDF protocol against a mock API, no Docker
test/clickhouse/e2e.sh                               # a real clickhouse-server in Docker (26.1 by default)
python3 test/clickhouse/real_api_check.py            # live API, needs TYPESAFE_API_KEY in .env
```
