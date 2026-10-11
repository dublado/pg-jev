#!/usr/bin/env bash
# End-to-end check: a real clickhouse-server calling jev_udf.py, with the mock API inside the same container.
# Usage: test/clickhouse/e2e.sh [clickhouse version]   (default 26.1)
set -euo pipefail

cd "$(dirname "$0")/../.."
CH_VERSION="${1:-26.1}"
IMAGE="ch-jev-test:${CH_VERSION}"
CONTAINER="ch-jev-e2e-$$"

docker build --network=host -q --build-arg "CH_VERSION=${CH_VERSION}" -t "$IMAGE" -f test/clickhouse/Dockerfile . >/dev/null
docker run -d --name "$CONTAINER" "$IMAGE" >/dev/null
trap 'docker rm -f "$CONTAINER" >/dev/null 2>&1' EXIT
docker exec -d "$CONTAINER" python3 /opt/jev/mock_api.py 8765

query() { docker exec "$CONTAINER" clickhouse-client -q "$1"; }
mock_stat() {
    docker exec "$CONTAINER" python3 -c \
        "import json, sys, urllib.request; print(json.load(urllib.request.urlopen('http://127.0.0.1:8765/stats'))[sys.argv[1]])" "$1"
}
failures=0
check() { # name, expected, actual
    if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: expected '$2', got '$3'"; failures=$((failures + 1)); fi
}

for _ in $(seq 1 60); do
    if query 'SELECT 1' >/dev/null 2>&1; then break; fi
    sleep 1
done
echo "clickhouse $(query 'SELECT version()')"
docker exec "$CONTAINER" clickhouse-client --multiquery --queries-file /opt/jev/jev_wrappers.sql

query "CREATE TABLE tickets (id UInt32, subject String, body String) ENGINE = MergeTree ORDER BY id"
query "INSERT INTO tickets SELECT number, concat('ticket ', toString(number)),
       if(number % 100 = 0, 'I am furious, refund now', 'thanks, all good') FROM numbers(1000)"
ROW="formatRowNoNewline('JSONEachRow', id, body)"

check "jev() as a WHERE predicate" "0,100,200,300,400,500,600,700,800,900" \
    "$(query "SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (SELECT id FROM tickets WHERE jev($ROW, 'the customer is furious') ORDER BY id)")"
check "every distinct row was sent once" "1000" "$(mock_stat rows)"

before="$(mock_stat requests)"
check "second run returns the same rows" "10" "$(query "SELECT count() FROM tickets WHERE jev($ROW, 'the customer is furious')")"
check "second run is served from the cache" "$before" "$(mock_stat requests)"

before="$(mock_stat rows)"
query "SELECT count() FROM tickets WHERE subject LIKE '%7' AND jev($ROW, 'new question: furious')" >/dev/null
check "rows rejected by a cheaper predicate are not sent" "100" "$(( $(mock_stat rows) - before ))"

before="$(mock_stat rows)"
query "SELECT count() FROM (SELECT number FROM numbers(100000) WHERE jev(concat('n ', toString(number)), 'contains 7') LIMIT 3
       SETTINGS max_block_size = 100, max_threads = 1)" >/dev/null
check "LIMIT stops after the first block" "100" "$(( $(mock_stat rows) - before ))"

check "jev_choice classifies every row" "1000" \
    "$(query "SELECT sum(c) FROM (SELECT jev_choice(body, 'which team?', ['billing', 'support']) AS team, count() AS c FROM tickets GROUP BY team HAVING team IN ('billing', 'support'))")"
check "jev_score_norm stays within 0..1" "1" \
    "$(query "SELECT min(s) >= 0 AND max(s) <= 1 FROM (SELECT jev_score_norm(body, 'how angry?', ['calm', 'annoyed', 'furious']) AS s FROM tickets)")"
check "jev_confidence reads the answer" "1" \
    "$(query "SELECT min(jev_confidence(body, 'how angry?', 'score', ['calm', 'annoyed', 'furious'])) FROM tickets")"

error="$(query "SELECT jev(body, 'please trigger422') FROM tickets LIMIT 1" 2>&1 || true)"
check "an API error fails the query" "1" "$(grep -c 'jev: API error 422' <<<"$error" || true)"
check "the function still works after an error" "10" "$(query "SELECT count() FROM tickets WHERE jev($ROW, 'the customer is furious')")"

query "CREATE TABLE gold_furious_tickets (id UInt32, p Float64) ENGINE = MergeTree ORDER BY id"
query "INSERT INTO gold_furious_tickets SELECT id, jev_prob($ROW, 'the customer is furious') AS p FROM tickets WHERE p >= 0.5"
check "INSERT ... SELECT materialises the judgment" "10" "$(query 'SELECT count() FROM gold_furious_tickets')"

stats="$(docker exec -u clickhouse "$CONTAINER" /var/lib/clickhouse/user_scripts/jev_udf.py --stats /etc/clickhouse-server/jev.json | tail -1)"
check "--stats reports today's usage" "1" "$(grep -c '"requests": [1-9]' <<<"$stats" || true)"

if [ "$failures" -ne 0 ]; then
    echo "$failures check(s) failed"
    exit 1
fi
echo "all checks passed"
