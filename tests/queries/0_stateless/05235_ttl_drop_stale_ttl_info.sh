#!/usr/bin/env bash
# Tags: long, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A part may carry TTL info of a family the table does not declare — a column TTL
# inherited via ATTACH PARTITION, or an entry left behind by REMOVE TTL with
# materialize_ttl_after_modify = 0. Such entries raise part_max_ttl, which used to
# tag the merge TTLDrop and let the empty-part short-circuit drop every row even
# though the table's rows TTL is not expired.
#
# Only the background selector assigns MergeType::TTLDrop; OPTIMIZE TABLE FINAL
# assigns MergeType::Regular. Both cases therefore drive a background TTL merge
# and wait for the parts to merge into one.

function wait_for_merge()
{
    local table=$1
    local deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do
        local part_count
        part_count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active")
        if [ "$part_count" -le 1 ]; then
            return
        fi
        sleep 0.2
    done
    echo "timed out waiting for the merge on $table"
}

# -------------------------------------------------------------------
# Case 1: column TTL carried over by ATTACH PARTITION
# -------------------------------------------------------------------
echo "-- Case 1: column TTL carried over by ATTACH PARTITION"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_ttl_drop_stale_src;
    DROP TABLE IF EXISTS t_ttl_drop_stale_dst;

    CREATE TABLE t_ttl_drop_stale_src
    (
        d DateTime,
        x UInt64,
        s String TTL d + INTERVAL 1 HOUR
    )
    ENGINE = MergeTree
    ORDER BY x
    PARTITION BY tuple()
    SETTINGS merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 1;

    CREATE TABLE t_ttl_drop_stale_dst
    (
        d DateTime,
        x UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY x
    PARTITION BY tuple()
    TTL d + INTERVAL 50 YEAR
    SETTINGS merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_stale_src;
    SYSTEM STOP MERGES t_ttl_drop_stale_dst;

    INSERT INTO t_ttl_drop_stale_src SELECT now() - INTERVAL 10 DAY, number, 'keep' FROM numbers(100);
    INSERT INTO t_ttl_drop_stale_src SELECT now() - INTERVAL 10 DAY, number + 100, 'keep' FROM numbers(100);

    ALTER TABLE t_ttl_drop_stale_dst ATTACH PARTITION tuple() FROM t_ttl_drop_stale_src;
    SYSTEM START MERGES t_ttl_drop_stale_dst;
"

wait_for_merge t_ttl_drop_stale_dst

# 200 rows in 1 part. Before the fix this printed "0 0" — the merge emptied the part.
${CLICKHOUSE_CLIENT} -q "SELECT count(), uniqExact(_part) FROM t_ttl_drop_stale_dst"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_ttl_drop_stale_src;
    DROP TABLE t_ttl_drop_stale_dst;
"

# -------------------------------------------------------------------
# Case 2: column TTL orphaned by REMOVE TTL
# -------------------------------------------------------------------
echo "-- Case 2: column TTL orphaned by REMOVE TTL"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_orphaned
    (
        d DateTime,
        x UInt64,
        s String TTL d + INTERVAL 1 HOUR
    )
    ENGINE = MergeTree
    ORDER BY x
    PARTITION BY tuple()
    SETTINGS merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 1, materialize_ttl_after_modify = 0;

    SYSTEM STOP MERGES t_ttl_drop_orphaned;

    INSERT INTO t_ttl_drop_orphaned SELECT now() - INTERVAL 10 DAY, number, 'keep' FROM numbers(100);
    INSERT INTO t_ttl_drop_orphaned SELECT now() - INTERVAL 10 DAY, number + 100, 'keep' FROM numbers(100);

    ALTER TABLE t_ttl_drop_orphaned MODIFY COLUMN s REMOVE TTL;
    ALTER TABLE t_ttl_drop_orphaned MODIFY TTL d + INTERVAL 50 YEAR;
    SYSTEM START MERGES t_ttl_drop_orphaned;
"

wait_for_merge t_ttl_drop_orphaned

${CLICKHOUSE_CLIENT} -q "SELECT count(), uniqExact(_part) FROM t_ttl_drop_orphaned"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_orphaned"
