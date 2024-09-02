CREATE TEMPORARY TABLE t1 (i INT, t TEXT) USING memoryam;

INSERT INTO t1 VALUES (1, 'hello');

-- should be one entry
SELECT * FROM t1;

BEGIN;
INSERT into t1 VALUES (2, 'world');
-- should be two entries
SELECT * FROM t1;
ROLLBACK;

-- should be back to one entry
SELECT * FROM t1;

BEGIN;
-- should be two entries
INSERT INTO t1 VALUES (2, 'world');
SELECT * FROM t1;
COMMIT;

-- should still be two entries
SELECT * FROM t1;

BEGIN;
DELETE FROM t1 WHERE i = 2;
-- should be one entry
SELECT * FROM t1;
ROLLBACK;

-- should be back to two entries
SELECT * FROM t1;

BEGIN;
DELETE FROM t1;
-- should be no entries
SELECT * FROM t1;
COMMIT;

-- should still be no entries
SELECT * FROM t1;

DROP TABLE t1;
