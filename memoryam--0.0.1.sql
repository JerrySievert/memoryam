CREATE OR REPLACE FUNCTION memoryam_tableam_handler(internal)
RETURNS table_am_handler
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'memoryam_tableam_handler';

CREATE ACCESS METHOD memoryam TYPE TABLE HANDLER memoryam_tableam_handler;

CREATE OR REPLACE FUNCTION memoryam_relation_details(
  IN regclass,
  OUT row_number bigint,
  OUT xmin integer,
  OUT xmax integer,
  OUT is_deleted bool
) RETURNS SETOF record
LANGUAGE c
AS 'MODULE_PATHNAME', 'memoryam_relation_details';

CREATE OR REPLACE FUNCTION memoryam_storage_details(
  OUT table_name text,
  OUT row_count bigint,
  OUT deleted_count bigint,
  OUT transaction_count bigint
) RETURNS SETOF record
LANGUAGE c
AS 'MODULE_PATHNAME', 'memoryam_storage_details';
