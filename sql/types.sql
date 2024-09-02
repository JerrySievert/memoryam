CREATE TEMPORARY TABLE t1 (i1 INT, t1 TEXT) using memoryam;

INSERT INTO t1 VALUES (1, 'missy');
INSERT INTO t1 VALUES (2, 'kitten');
INSERT INTO t1 VALUES (NULL, NULL);

SELECT * FROM t1;

DROP TABLE t1;

CREATE TEMPORARY TABLE t1 (f REAL, d DOUBLE PRECISION) using memoryam;

INSERT INTO t1 VALUES (1.1, 2.4);
INSERT INTO t1 VALUES (2.3, 23.6);

SELECT * FROM t1;

DROP TABLE t1;