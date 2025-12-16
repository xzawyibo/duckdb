/*
 * pg_duckdb_translator.h
 *
 * C API header for translating PostgreSQL PlannedStmt into DuckDB plans.
 * This is a lightweight, forward-declaration-only header to avoid
 * including heavy Postgres headers in early prototyping.
 */
#ifndef PG_DUCKDB_TRANSLATOR_H
#define PG_DUCKDB_TRANSLATOR_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque pointers used to avoid direct Postgres/DuckDB type dependencies
 * at header level. Implementations live in C++ files and should cast
 * these to the real types when building inside the Postgres tree.
 */
typedef void PGPlannedStmt; /* actually: PlannedStmt* */
typedef void PGSnapshot;    /* actually: Snapshot */
typedef void DuckDBLogicalPlan;  /* actually: duckdb::LogicalOperator* */

/* Translate a Postgres PlannedStmt to a DuckDB logical plan.
 * - stmt, snapshot: opaque pointers from Postgres runtime (may be NULL in tests)
 * - out_logical_ptr: on success points to an allocated (opaque) logical plan
 * - error_msg: on failure points to a malloc'd NUL-terminated error string
 * Returns true on success.
 */
bool pg_to_duckdb_logical_plan(PGPlannedStmt *stmt,
                              PGSnapshot *snapshot,
                              DuckDBLogicalPlan **out_logical_ptr,
                              char **error_msg);

/* Extract DuckDB's own LogicalPlan for a SQL string using DuckDB's planner.
 * - sql: NUL-terminated SQL string to parse/plan inside DuckDB
 * - out_logical_ptr: on success points to an allocated (opaque) logical plan
 * - error_msg: on failure points to a malloc'd NUL-terminated error string
 * Returns true on success.
 */
bool duckdb_extract_logical_plan_from_sql(const char *sql,
                                          DuckDBLogicalPlan **out_logical_ptr,
                                          char **error_msg);

/* Serialize a DuckDB logical plan into a NUL-terminated string (caller must
 * free with free()). Uses LogicalOperator::ToString() as a stable-ish
 * representation for structural comparison.
 */
bool duckdb_logical_serialize(DuckDBLogicalPlan *logical_ptr,
                             char **out_str,
                             char **error_msg);

/* Free plan objects created by the translator. */
void duckdb_plan_free(void *plan_ptr);

/* Diagnostic helper: map and log each entry in a Postgres Query's targetList.
 * This is intended for debugging; it exercises the internal `map_pg_expr`
 * logic and writes human-readable entries to `/tmp/pg_const_map.log`.
 */
void pg_duckdb_log_targetlist_nodes(void *stmt);

bool pg_to_duckdb_physical_plan(void *stmt_ptr, void **out_plan_ptr, char **error_msg);
void duckdb_physical_plan_free(void *plan_ptr);
bool duckdb_physical_serialize(void *plan_ptr, char **out_str, char **error_msg);

#ifdef __cplusplus
}
#endif

#endif /* PG_DUCKDB_TRANSLATOR_H */
