-- SQL script for pg_duckdb_translator extension

CREATE OR REPLACE FUNCTION pgsql_to_ducklogical(text) RETURNS text
    AS 'MODULE_PATHNAME', 'pgsql_to_ducklogical'
    LANGUAGE C STRICT;

COMMENT ON FUNCTION pgsql_to_ducklogical(text) IS 'Translate a Postgres Query (text) to a DuckDB logical plan string via in-kernel translator.';

create or replace function ducksql_to_ducklogical(text) returns text
    as 'MODULE_PATHNAME', 'ducksql_to_ducklogical'
    language c strict;

comment on function ducksql_to_ducklogical(text) is 'Translate a DuckDB SQL Query (text) to a DuckDB logical plan string via in-kernel translator.';

CREATE FUNCTION pg_duckdb_explain(sql text)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_duckdb_explain'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_duckdb_explain(text)
IS 'Translate a Postgres PlannedStmt into a DuckDB physical plan string (EXPLAIN-like output).';
