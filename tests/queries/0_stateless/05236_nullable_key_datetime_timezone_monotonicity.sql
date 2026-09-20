-- Index analysis over a `Nullable` key column that names its own time zone must not prune rows
-- that match a date-part predicate. The monotonicity check used to resolve the factor time zone
-- from the outer `Nullable` wrapper, which carries no zone, and fell back to the session zone;
-- the function itself runs in the nested `DateTime('TZ')` zone. A key range that spans two days
-- in the column's zone can collapse into one day in the session zone, so `toHour`/`toDayOfMonth`
-- were wrongly declared monotonic and the mapped range dropped matching rows.

DROP TABLE IF EXISTS t_dt_tz_nullable;
DROP TABLE IF EXISTS t_dt_tz_plain;
SET session_timezone = 'UTC';

CREATE TABLE t_dt_tz_nullable (dt Nullable(DateTime('America/New_York'))) ENGINE = MergeTree ORDER BY dt SETTINGS allow_nullable_key = 1;
CREATE TABLE t_dt_tz_plain (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt;

-- Two different days in the key's zone, one and the same day in the session zone.
INSERT INTO t_dt_tz_nullable VALUES ('2026-08-31 23:00:00'), ('2026-09-01 01:00:00');
INSERT INTO t_dt_tz_plain VALUES ('2026-08-31 23:00:00'), ('2026-09-01 01:00:00');

SELECT (SELECT count() FROM t_dt_tz_nullable WHERE toHour(dt) = 23) AS pruned,
       (SELECT countIf(toHour(dt) = 23) FROM t_dt_tz_nullable) AS honest;

SELECT (SELECT count() FROM t_dt_tz_nullable WHERE toDayOfMonth(dt) = 31) AS pruned,
       (SELECT countIf(toDayOfMonth(dt) = 31) FROM t_dt_tz_nullable) AS honest;

SELECT (SELECT count() FROM t_dt_tz_plain WHERE toHour(dt) = 23) AS pruned,
       (SELECT countIf(toHour(dt) = 23) FROM t_dt_tz_plain) AS honest;

DROP TABLE IF EXISTS t_dt_tz_nullable;
DROP TABLE IF EXISTS t_dt_tz_plain;
