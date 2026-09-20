#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/121231
# Disambiguating `(from <operator> ...)` from a FROM-first subquery used to parse the
# parenthesized contents a second time in the caller, so parse time grew exponentially
# with nesting depth. At depth 20 the query below could not be parsed at all (depth 13
# already took ~30 s); it must now finish well within the client timeout.
query="1"
for _ in $(seq 1 20); do query="(from IN ($query))"; done
echo "EXPLAIN SYNTAX SELECT $query" | ${CLICKHOUSE_CURL} -sS --max-time 30 "${CLICKHOUSE_URL}" -d @- 2>&1 | grep -o 'from' | head -1

# A parenthesized expression over a column named `from` still parses.
echo 'EXPLAIN SYNTAX SELECT (from IN (1))' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- 2>&1 | grep -o 'from' | head -1

# A FROM-first subquery still parses as a subquery.
echo 'EXPLAIN SYNTAX SELECT * FROM (FROM numbers(1))' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- 2>&1 | grep -o 'numbers' | head -1

# A relation named after an operator still reads as one.
echo 'EXPLAIN SYNTAX SELECT 1 IN (FROM in)' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- 2>&1 | grep -o 'in' | head -1

# A quoted `from` identifier is unaffected.
echo 'EXPLAIN SYNTAX SELECT (`from` IN (1))' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- 2>&1 | grep -o 'from' | head -1

# `(expr) -> ...` still parses as a lambda over a single-element tuple.
echo 'EXPLAIN SYNTAX SELECT (from IN (1)) -> 1' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- 2>&1 | grep -o 'from' | head -1
