CREATE TEMPORARY TABLE t1 (i INT, t TEXT) USING memoryam;

INSERT INTO t1 VALUES (1, 'hello');
INSERT INTO t1 VALUES (2, 'world');

SELECT * FROM t1;

UPDATE t1 SET t = 'foo' WHERE i = 2;

SELECT * FROM t1;

UPDATE t1 SET t = 'bar', i = 3 WHERE i = 1;

SELECT * FROM t1;

DROP TABLE t1;

CREATE TEMPORARY TABLE upsert_test (
  i INT,
  t TEXT
) USING memoryam;

CREATE UNIQUE INDEX ON upsert_test (t);

INSERT INTO upsert_test (i, t) VALUES (1, 'hello');
INSERT INTO upsert_test (i, t) VALUES (2, 'world');

-- should work, then roll back
BEGIN;
INSERT INTO upsert_test (t) VALUES ( 'hello' ) ON CONFLICT (t) DO UPDATE SET t = 'foo';
SELECT * FROM upsert_test;
ROLLBACK;

SELECT * FROM upsert_test;

-- should take affect immediately
INSERT INTO upsert_test (t) VALUES ( 'hello' ) ON CONFLICT (t) DO UPDATE SET t = 'bar';
SELECT * FROM upsert_test;

-- should work as expected
BEGIN;
INSERT INTO upsert_test (t) VALUES ( 'world' ) ON CONFLICT (t) DO UPDATE SET t = 'foo';
SELECT * FROM upsert_test;
COMMIT;

-- should also work as expected, a select can mask a bug
DELETE FROM upsert_test;
BEGIN;
INSERT INTO upsert_test (i, t) VALUES (1, 'hello');
INSERT INTO upsert_test (t) VALUES ( 'hello' ) ON CONFLICT (t) DO UPDATE SET t = 'bar';
COMMIT;

SELECT * FROM upsert_test;

DROP TABLE upsert_test;
