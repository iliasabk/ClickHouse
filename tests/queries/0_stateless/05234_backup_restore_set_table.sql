-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/121176
-- BACKUP of an Engine = Set table used to store only the table metadata and silently
-- dropped all data: RESTORE reported success but produced an empty set. The same
-- fall-through made Engine = Join lose its data as well
-- (https://github.com/ClickHouse/ClickHouse/issues/62255).

-- A DROP of a table that holds data on disk is silently ignored when this probability is
-- non-zero, which the stress runner sets, and the following RESTORE would then take the
-- already-exists path instead.
SET ignore_drop_queries_probability = 0;

DROP TABLE IF EXISTS 05234_set SYNC;

CREATE TABLE 05234_set (k UInt64) ENGINE = Set;
INSERT INTO 05234_set SELECT number FROM numbers(100);

SELECT count() FROM numbers(200) WHERE number IN 05234_set;

BACKUP TABLE 05234_set TO Memory('05234_set_backup') FORMAT Null;

DROP TABLE 05234_set SYNC;
RESTORE TABLE 05234_set FROM Memory('05234_set_backup') FORMAT Null;

-- Data must survive the round-trip.
SELECT count() FROM numbers(200) WHERE number IN 05234_set;

-- The restored set keeps its files on disk again, so new inserts extend it.
INSERT INTO 05234_set SELECT number + 100 FROM numbers(50);
SELECT count() FROM numbers(200) WHERE number IN 05234_set;

DROP TABLE 05234_set SYNC;

-- Engine = Join shares the same storage base and its rows must round-trip too.
DROP TABLE IF EXISTS 05234_join SYNC;
DROP TABLE IF EXISTS 05234_join_left SYNC;

CREATE TABLE 05234_join (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO 05234_join SELECT number, 'v_' || toString(number) FROM numbers(50);

CREATE TABLE 05234_join_left (k UInt64) ENGINE = Memory;
INSERT INTO 05234_join_left SELECT number FROM numbers(100);

SELECT countIf(v != '') FROM 05234_join_left l ANY LEFT JOIN 05234_join j ON l.k = j.k;

BACKUP TABLE 05234_join TO Memory('05234_join_backup') FORMAT Null;
DROP TABLE 05234_join SYNC;
RESTORE TABLE 05234_join FROM Memory('05234_join_backup') FORMAT Null;

SELECT countIf(v != '') FROM 05234_join_left l ANY LEFT JOIN 05234_join j ON l.k = j.k;

DROP TABLE 05234_join SYNC;
DROP TABLE 05234_join_left SYNC;

-- An empty table backs up and restores without error.
DROP TABLE IF EXISTS 05234_set_empty SYNC;
CREATE TABLE 05234_set_empty (k UInt64) ENGINE = Set;
BACKUP TABLE 05234_set_empty TO Memory('05234_set_empty_backup') FORMAT Null;
DROP TABLE 05234_set_empty SYNC;
RESTORE TABLE 05234_set_empty FROM Memory('05234_set_empty_backup') FORMAT Null;
SELECT count() FROM numbers(10) WHERE number IN 05234_set_empty;
DROP TABLE 05234_set_empty SYNC;

-- A structure_only backup carries no data marker; restoring it as data must fail instead
-- of silently recreating an empty table (that silent empty restore is exactly the
-- #121176 data loss).
DROP TABLE IF EXISTS 05234_set_meta SYNC;
CREATE TABLE 05234_set_meta (k UInt64) ENGINE = Set;
INSERT INTO 05234_set_meta VALUES (1);
BACKUP TABLE 05234_set_meta TO Memory('05234_set_meta_backup') SETTINGS structure_only = true FORMAT Null;
DROP TABLE 05234_set_meta SYNC;
RESTORE TABLE 05234_set_meta FROM Memory('05234_set_meta_backup') FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }
RESTORE TABLE 05234_set_meta FROM Memory('05234_set_meta_backup') SETTINGS structure_only = true FORMAT Null;
SELECT count() FROM numbers(10) WHERE number IN 05234_set_meta;
DROP TABLE 05234_set_meta SYNC;
