#!/usr/bin/env bash
# Regression test for a SIGSEGV on INSERT after ALTER: AlterCommands::apply
# re-derived the persisted sorting/primary key types in the session context of
# the ALTER, so `cast_keep_nullable = 1` turned `CAST(x, 'UInt8')` into
# `Nullable(UInt8)` in primary_key.data_types while INSERT resolves the key
# expression in the global context and produces a plain `UInt8` column.
# Serializing it through SerializationNullable then crashed the server.
# Persisted key/index/projection types are now re-derived in the global
# context, the same context a fresh reload resolves them in.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05234 SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05234 (k UInt32, x Nullable(Int32), v String) ENGINE = MergeTree ORDER BY (k, CAST(x, 'UInt8'))"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05234 VALUES (1, 2, 'a')"

# Before a reload the mis-resolved Nullable key used to refuse a valid ALTER
# with ILLEGAL_COLUMN; it must succeed and record the reload-time type.
${CLICKHOUSE_CLIENT} --cast_keep_nullable 1 -q "ALTER TABLE t_05234 ADD COLUMN w UInt32"

# After a metadata reload the allow_nullable_key guard is gone, so the same
# ALTER used to record Nullable(UInt8) and crash the next INSERT.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_05234"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_05234"
${CLICKHOUSE_CLIENT} --cast_keep_nullable 1 -q "ALTER TABLE t_05234 ADD COLUMN y UInt32"

# Must not SIGSEGV: the recorded key type is UInt8 again.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05234 VALUES (2, 3, 'b')"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05234"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_05234 ORDER BY k"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_05234 SYNC"
