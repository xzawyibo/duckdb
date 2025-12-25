#ifndef PG_DUCKDB_MAPPER_H
#define PG_DUCKDB_MAPPER_H

#include <memory>
#include "duckdb/common/unique_ptr.hpp"
#include <string>
#include <vector>
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp" 


// Forward declare global caches (defined in pg_duckdb_translator.cc)
extern std::vector<std::string> g_current_colnames;
extern std::vector<duckdb::LogicalType> g_current_types;
// Map Postgres attribute number (1-based) -> column index in g_current_colnames/g_current_types
extern std::vector<int> g_current_attnum_map;

// Map a Postgres expression node to a DuckDB Expression
// node is passed as a void* to avoid forcing Postgres headers into
// every translation unit that includes this header.
duckdb::unique_ptr<duckdb::Expression> map_pg_expr(void *node);

// 专门的 PG Const -> DuckDB BoundConstantExpression
duckdb::unique_ptr<duckdb::BoundConstantExpression> PgConstToDuckConst(void *pg_const_node);
#endif // PG_DUCKDB_MAPPER_H
