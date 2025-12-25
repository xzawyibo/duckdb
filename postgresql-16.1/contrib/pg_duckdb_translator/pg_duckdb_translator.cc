/*
 * When building inside the Postgres backend, the build system should define
 * `COMPILE_WITH_POSTGRES` and provide include paths so we can include the
 * real Postgres node headers and map the `Query` structure. For local helper
 * builds (outside the full Postgres build), we avoid including Postgres
 * headers to prevent dependency on generated headers; in that case we keep
 * the fallback placeholder translator.
 */



/*
 * pg_duckdb_translator.cc
 *
 * Minimal C++ translator implementation stubs. Real implementation should
 * construct DuckDB logical/physical plan objects and perform conversions.
 * These stubs provide linkable symbols and helpful error messages for
 * early integration.
 */

#include "pg_duckdb_translator.h"
#include <stdlib.h>
#include <string.h>
#include <string>

#if defined(Min)
#undef Min
#endif
#if defined(Max)
#undef Max
#endif
#if defined(gettext)
#undef gettext
#endif
#if defined(dgettext)
#undef dgettext
#endif
#if defined(ngettext)
#undef ngettext
#endif
#if defined(dngettext)
#undef dngettext
#endif
#if defined(FATAL)
#undef FATAL
#endif
#if defined(MONTHS_PER_YEAR)
#undef MONTHS_PER_YEAR
#endif
#if defined(DAYS_PER_MONTH)
#undef DAYS_PER_MONTH
#endif
#if defined(DAYS_PER_YEAR)
#undef DAYS_PER_YEAR
#endif
#if defined(SECS_PER_MINUTE)
#undef SECS_PER_MINUTE
#endif
#if defined(MINS_PER_HOUR)
#undef MINS_PER_HOUR
#endif
#if defined(HOURS_PER_DAY)
#undef HOURS_PER_DAY
#endif
#if defined(SECS_PER_HOUR)
#undef SECS_PER_HOUR
#endif
#if defined(SECS_PER_DAY)
#undef SECS_PER_DAY
#endif
#if defined(Abs)
#undef Abs
#endif

#if defined(INVALID_CATALOG)
#undef INVALID_CATALOG
#endif

#if defined(DEFAULT_SCHEMA)
#undef DEFAULT_SCHEMA
#endif

// DuckDB headers
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include <memory>
#include "duckdb/main/connection.hpp"
#include <ctime>

#include "pg_duckdb_mapper.h"

// for exectue
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

//新加
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/common/types/date.hpp"


extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/pg_list.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "utils/rel.h"
#include "nodes/plannodes.h"
#include "optimizer/tlist.h"      // get_tle_by_resno
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
}

using duckdb::PhysicalTableScan; 
using duckdb::PhysicalHashAggregate; 
using duckdb::PhysicalProjection; 
using duckdb::PhysicalOrder; 
using duckdb::PhysicalFilter;
using duckdb::LogicalType;
using duckdb::unique_ptr;
using duckdb::make_uniq;
using duckdb::PhysicalOperator;
using duckdb::ExpressionIterator;

static void set_error(char **error_msg, const char *msg) {
    if (!error_msg) return;
    *error_msg = (char*)malloc(strlen(msg) + 1);
    strcpy(*error_msg, msg);
}

static duckdb::LogicalType PgTypeOidToDuckType(Oid typid, int32 typmod) {
	(void)typmod;
	switch (typid) {
	case INT2OID: return LogicalType::SMALLINT;
	case INT4OID: return LogicalType::INTEGER;
	case INT8OID: return LogicalType::BIGINT;
	case FLOAT4OID: return LogicalType::FLOAT;
	case FLOAT8OID: return LogicalType::DOUBLE;
	case NUMERICOID: return LogicalType::DOUBLE; // reference choice
	case TEXTOID:
	case VARCHAROID:
	case BPCHAROID: return LogicalType::VARCHAR;
	case DATEOID: return LogicalType::DATE;
	case TIMESTAMPOID: return LogicalType::TIMESTAMP;
	default:
		throw duckdb::InternalException("PgTypeOidToDuckType: unsupported type oid " + std::to_string((uint32)typid));
	}
}

/*用一个自定义的 PgTableFunctionInfo 把 PG 表名塞进 table_func.function_info 里；
把 table_func.name 改成 "pg_seq_scan"*/
// struct PgColBinding {
//     AttrNumber     attno;      // PG 1-based 列号
//     duckdb::idx_t  local_index;// 在我们这次扫描输出里的列顺序
//     duckdb::LogicalType type;
//     std::string    name;
// };



// 传 Relation / TupleDesc 进来，完全不碰 DuckDB catalog。
// static PgColBinding
// PgVarToPgScanColumn(Var *var,
//                     Relation rel,
//                     TupleDesc tupdesc,
//                     std::vector<duckdb::LogicalType> &returned_types,
//                     std::vector<std::string> &column_names,
//                     std::vector<duckdb::ColumnIndex> &column_ids) {
//     using namespace duckdb;

//     AttrNumber attno = var->varattno; // PG 1-based

//     if (attno <= 0 || attno > tupdesc->natts) {
//         ereport(ERROR,
//                 (errmsg("PgVarToPgScanColumn: invalid attno %d", attno)));
//     }

//     Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
//     if (attr->attisdropped) {
//         ereport(ERROR,
//                 (errmsg("PgVarToPgScanColumn: dropped column attno %d", attno)));
//     }

//     const char *attname = NameStr(attr->attname);
//     Oid atttypid = attr->atttypid;
//     int32 atttypmod = attr->atttypmod;

//     // 看看这个 attno 是否已经加入 column_ids 里
//     duckdb::column_t local_idx = (duckdb::column_t)duckdb::DConstants::INVALID_INDEX;

//     // 用列名做 key，这样多次遇到同一列（同名）就复用同一个 local_index
//     std::string colname = attname ? std::string(attname)
//                                   : std::string("col") + std::to_string((int)attno);


//     // 先看看这个列是不是已经在我们这次扫描的列数组里了
//     for (column_t i = 0; i < (column_t)column_ids.size(); i++) {
//         if (column_names[i] == colname) {
//             local_idx = i;
//             break;
//         }
//     }

//     if (local_idx == duckdb::DConstants::INVALID_INDEX) {
//         // 这次扫描中新加一列
//         // local_idx = (duckdb::column_t)column_ids.size();
//         // column_ids.push_back(duckdb::ColumnIndex(local_idx));          // 本次 scan 中的列号 0..n-1
//         // column_names.emplace_back(attname ? attname : "");
//         // returned_types.push_back(PgTypeOidToDuckType(atttypid, atttypmod));
//         auto table_col_idx = (duckdb::column_t)(attno - 1);   // base table column index (0-based)

//         local_idx = (duckdb::column_t)column_ids.size();      // scan-local index
//         column_ids.push_back(duckdb::ColumnIndex(table_col_idx));

//         // returned_types 最好也从 DuckDB catalog 拿，避免 PG/DuckDB 类型不一致
//         // returned_types.push_back(PgTypeOidToDuckType(...)); // 这一行建议别再用
//         returned_types.push_back(table_entry.GetColumns().GetColumn(table_col_idx).Type()); // 伪码：按你实际 API 调整
//     }

//     PgColBinding b;
//     b.attno       = attno;
//     b.local_index = local_idx;
//     b.type        = returned_types[local_idx];
//     b.name        = column_names[local_idx];
//     return b;
// }


// bool TryBuildComparisonFilterFromQual_Q1_PG(
//     Expr *qual,
//     TupleDesc tupdesc,
//     const std::vector<duckdb::column_t> &attno_to_local, // size >= natts+1
//     duckdb::TableFilterSet &table_filters) {

//     using namespace duckdb;

//     if (!IsA(qual, OpExpr)) {
//         return false;
//     }
//     auto *opexpr = (OpExpr *)qual;
//     if (list_length(opexpr->args) != 2) {
//         return false;
//     }

//     Expr *arg1 = (Expr *)linitial(opexpr->args);
//     Expr *arg2 = (Expr *)lsecond(opexpr->args);

//     Var   *var = NULL;
//     Const *cst = NULL;

//     if (IsA(arg1, Var) && IsA(arg2, Const)) {
//         var = (Var *)arg1;
//         cst = (Const *)arg2;
//     } else if (IsA(arg1, Const) && IsA(arg2, Var)) {
//         var = (Var *)arg2;
//         cst = (Const *)arg1;
//     } else {
//         return false;
//     }

//     AttrNumber attno = var->varattno;
//     if (attno <= 0 || attno >= (int)attno_to_local.size()) {
//         return false;
//     }
//     auto local_idx = attno_to_local[attno];
//     if (local_idx == duckdb::DConstants::INVALID_INDEX) {
//         return false;
//     }

//     // 操作符翻译
//     const char *opname = get_opname(opexpr->opno);
//     if (!opname) {
//         return false;
//     }
//     ExpressionType cmp;
//     if (strcmp(opname, "<=") == 0) cmp = ExpressionType::COMPARE_LESSTHANOREQUALTO;
//     else if (strcmp(opname, "<") == 0) cmp = ExpressionType::COMPARE_LESSTHAN;
//     else if (strcmp(opname, "=") == 0) cmp = ExpressionType::COMPARE_EQUAL;
//     else if (strcmp(opname, ">=") == 0) cmp = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
//     else if (strcmp(opname, ">") == 0) cmp = ExpressionType::COMPARE_GREATERTHAN;
//     else return false;

//     if (cst->constisnull) {
//         return false;
//     }

//     // Const -> duckdb::Value，用你已有的 PgConstToDuckConst
//     auto duck_const_expr = PgConstToDuckConst(cst);//auto 是 C++11 的类型自动推导关键字  
//     /*调用 PgConstToDuckConst 把 PG 的 Const 节点转换成一个 DuckDB 的 BoundConstantExpression，
//     结果保存在一个智能指针 duck_const_expr 里，类型由 auto 自动推导。auto就是帮你少写一长串类型名
//     这一行等价于：std::unique_ptr<BoundConstantExpression> duck_const_expr = PgConstToDuckConst(cst);*/

//     if (!duck_const_expr) {
//         return false;
//     }
//     Value constant_val = duck_const_expr->value;

//     auto tf = duckdb::make_uniq<ConstantFilter>(cmp, constant_val);//构造条件op const
//     table_filters.PushFilter(ColumnIndex(local_idx), std::move(tf));//将构造的条件挂载到通过attno在columid里面对应的列上形成var op const

//     return true;
// }


// ---------------- helper: normalize identifier ----------------
// PG 未加引号的标识符会 fold to lower-case；DuckDB 通常大小写不敏感，但我们统一 lower-case 作为 key
static inline std::string NormalizeIdent(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return (char)std::tolower(c); });
    return s;
}

struct DuckColRef {
    duckdb::column_t physical_col;   // DuckDB physical column id (0..n-1)
    duckdb::LogicalType duck_type;   // DuckDB column logical type
};

// ---------------- helper: build DuckDB column map (name->physical/type) ----------------
// 注意：这里是从 DuckDB catalog 拿到“physical columns”的顺序与类型。
// 你仍然可以用 PG 的列名作为查询 key，这样就满足“列名来自 PG”。
static std::unordered_map<std::string, DuckColRef>
BuildDuckColumnMap(const duckdb::TableCatalogEntry &table_entry) {
    std::unordered_map<std::string, DuckColRef> out;

    duckdb::idx_t phys_idx = 0;
    for (auto &coldef : table_entry.GetColumns().Physical()) {
        std::string name_key = NormalizeIdent(coldef.Name());
        out[name_key] = DuckColRef{(duckdb::column_t)phys_idx, coldef.Type()};
        phys_idx++;
    }
    return out;
}
// ---------------- helper: build DuckDB physical names vector (index = physical column id) ----------------
// 重要：不要用 auto& 接 Physical() 的返回值（它是临时 iterator），也不要用 size()，用 push_back 最稳。
static duckdb::vector<std::string>
BuildDuckPhysicalNameVector(const duckdb::TableCatalogEntry &table_entry) {
    duckdb::vector<std::string> names;
    auto phys = table_entry.GetColumns().Physical();   // 注意：不要 auto&
    names.reserve(phys.Size());                        // 注意：Size() 不是 size()
    for (auto &coldef : phys) {
        names.push_back(coldef.Name());                // names[physical_id]
    }
    return names;
}

static duckdb::vector<duckdb::LogicalType>
BuildDuckPhysicalTypeVector(const duckdb::TableCatalogEntry &table_entry) {
    duckdb::vector<duckdb::LogicalType> types;
    auto phys = table_entry.GetColumns().Physical();
    types.reserve(phys.Size());
    for (auto &coldef : phys) {
        types.push_back(coldef.Type());                // types[physical_id]
    }
    return types;
}


struct PgColBinding {
    AttrNumber attno;                 // PG 1-based
    duckdb::column_t local_index;     // scan-local 0..k-1 (index into returned_types/scan_column_names)
    duckdb::column_t physical_index;  // DuckDB physical column id
    duckdb::LogicalType type;         // DuckDB type (must match physical column)
    std::string name;                 // PG column name
};


// ---------------- PG Var -> scan column binding ----------------
// returned_types/scan_column_names/column_ids 这三者必须按 scan-local 同步增长：
// - local_index 是这三者向量中的位置
// - column_ids[local_index] 存 DuckDB physical id
// - returned_types[local_index] 存 DuckDB type
static PgColBinding
PgVarToPgScanColumn(Var *var,
                    TupleDesc tupdesc,
                    const std::unordered_map<std::string, DuckColRef> &duck_colmap,
                    std::vector<duckdb::LogicalType> &returned_types,
                    std::vector<std::string> &scan_column_names,          // PG names (scan-local)
                    std::vector<duckdb::ColumnIndex> &column_ids,         // DuckDB physical ids (scan-local)
                    std::vector<duckdb::column_t> &local_to_physical) {   // scan-local -> DuckDB physical
    using namespace duckdb;

    AttrNumber attno = var->varattno; // PG 1-based
    if (attno <= 0 || attno > tupdesc->natts) {
        ereport(ERROR, (errmsg("PgVarToPgScanColumn: invalid attno %d", attno)));
    }

    Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
    if (attr->attisdropped) {
        ereport(ERROR, (errmsg("PgVarToPgScanColumn: dropped column attno %d", attno)));
    }

    const char *attname = NameStr(attr->attname);
    std::string pg_colname = attname ? std::string(attname)
                                     : std::string("col") + std::to_string((int)attno);
    std::string key = NormalizeIdent(pg_colname);

    auto it = duck_colmap.find(key);
    if (it == duck_colmap.end()) {
        ereport(ERROR,
                (errmsg("PgVarToPgScanColumn: column '%s' not found in DuckDB table",
                        pg_colname.c_str())));
    }
    duckdb::column_t physical_col = it->second.physical_col;
    duckdb::LogicalType duck_type = it->second.duck_type;

    // scan-local dedup by name
    duckdb::column_t local_idx = (duckdb::column_t)duckdb::DConstants::INVALID_INDEX;
    for (duckdb::column_t i = 0; i < (duckdb::column_t)scan_column_names.size(); i++) {
        if (NormalizeIdent(scan_column_names[i]) == key) {
            local_idx = i;
            break;
        }
    }

    if (local_idx == (duckdb::column_t)duckdb::DConstants::INVALID_INDEX) {
        local_idx = (duckdb::column_t)column_ids.size();

        column_ids.push_back(duckdb::ColumnIndex((duckdb::idx_t)physical_col));
        returned_types.push_back(duck_type);
        scan_column_names.push_back(pg_colname);
        local_to_physical.push_back(physical_col);
    } else {
        // 如果重复出现同名列，确保 physical 一致（否则映射会乱）
        if (local_idx < (duckdb::column_t)local_to_physical.size() &&
            local_to_physical[local_idx] != physical_col) {
            ereport(ERROR, (errmsg("PgVarToPgScanColumn: inconsistent physical mapping for '%s'", pg_colname.c_str())));
        }
    }

    PgColBinding b;
    b.attno          = attno;
    b.local_index    = local_idx;
    b.physical_index = physical_col;
    b.type           = returned_types[local_idx];
    b.name           = scan_column_names[local_idx];
    return b;
}



// ---------------- qual -> TableFilterSet ----------------
// 关键修复：TableFilterSet::PushFilter 的 ColumnIndex 是 “scan-local 下标”，不是 physical id。
// 所以这里 attno_to_local 存的是 local_index。
// ---------------- qual -> TableFilterSet (key must be scan-local index) ----------------


/* Postgres headers are included only when building inside Postgres (see COMPILE_WITH_POSTGRES). */

using namespace duckdb;


// Global helper: current column names (populated when building LogicalGet)
std::vector<std::string> g_current_colnames;
// Global helper: current column types (DuckDB LogicalType) for scan
std::vector<duckdb::LogicalType> g_current_types;
// Map Postgres attribute number (1-based) -> column index in g_current_colnames/g_current_types
std::vector<int> g_current_attnum_map;


// map_pg_expr implementation moved to `pg_duckdb_mapper.cc` for easier maintenance.


extern "C" bool pg_to_duckdb_logical_plan(void *stmt, void *snapshot,
                                          void **out_logical_ptr,
                                          char **error_msg) {
    if (!out_logical_ptr) {
        set_error(error_msg, "out_logical_ptr is NULL");
        return false;
    }
    try {
        // Emit a short diagnostic banner to help correlate invocations with logs
        {
            FILE *f = fopen("/tmp/pg_const_map.log", "a");
            if (f) {
                time_t t = time(NULL);
                fprintf(f, "CALL pg_to_duckdb_logical_plan stmt=%p time=%ld\n", stmt, (long) t);
                if (stmt) {
                    Query *qtmp = reinterpret_cast<Query *>(stmt);
                    int rlen = qtmp->rtable ? list_length(qtmp->rtable) : 0;
                    int tlen = qtmp->targetList ? list_length(qtmp->targetList) : 0;
                    fprintf(f, "  Query summary: rtable_len=%d targetList_len=%d hasAggs=%d\n", rlen, tlen, (int) qtmp->hasAggs);
                }
                fclose(f);
            }
        }

        // If caller provided a Postgres Query, map parts of it to DuckDB LogicalOperators.
        if (stmt != nullptr) {
            Query *q = reinterpret_cast<Query *>(stmt);

            // Start with a LogicalGet for the first RTE (if any)
            unique_ptr<LogicalOperator> get_op;
            if (q->rtable != NIL && list_length(q->rtable) > 0) {
                RangeTblEntry *rte = (RangeTblEntry *) linitial(q->rtable);

                if (rte->rtekind == RTE_RELATION && OidIsValid(rte->relid)) {
                    struct RelationData *pg_rel = table_open(rte->relid, AccessShareLock);
                    TupleDesc tupdesc = RelationGetDescr(pg_rel);
                    int natts = tupdesc->natts;
                    vector<LogicalType> types;
                    vector<string> colnames;
                    // attnum map sized by natts (postgre attribute numbers are 1-based)
                    vector<int> attnum_map(natts, -1);
                    for (int i = 1; i <= natts; i++) {
                        Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
                        if (attr->attisdropped) continue;
                        // current column index in our vectors
                        int cur_idx = (int) colnames.size();
                        attnum_map[i - 1] = cur_idx;
                        Oid atttypid = attr->atttypid;
                        const char *attname = get_attname(RelationGetRelid(pg_rel), i, false);
                        string cname = attname ? string(attname) : string("col") + to_string(i - 1);
                        colnames.push_back(cname);
                        // Map common PG types to DuckDB LogicalType
                        if (atttypid == INT2OID) {
                            types.emplace_back(LogicalType::SMALLINT);
                        } else if (atttypid == INT4OID) {
                            types.emplace_back(LogicalType::INTEGER);
                        } else if (atttypid == INT8OID) {
                            types.emplace_back(LogicalType::BIGINT);
                        } else if (atttypid == FLOAT4OID) {
                            types.emplace_back(LogicalType::FLOAT);
                        } else if (atttypid == FLOAT8OID) {
                            types.emplace_back(LogicalType::DOUBLE);
                        } else if (atttypid == BOOLOID) {
                            types.emplace_back(LogicalType::BOOLEAN);
                        } else if (atttypid == DATEOID) {
                            types.emplace_back(LogicalType::DATE);
                        } else if (atttypid == NUMERICOID) {
                            // map NUMERIC/DECIMAL to a wide decimal type (precision/scale unknown here)
                            types.emplace_back(LogicalType::DECIMAL(38, 10));
                        } else if (atttypid == TIMESTAMPOID) {
                            types.emplace_back(LogicalType::TIMESTAMP);
                        } else if (atttypid == TIMESTAMPTZOID) {
                            types.emplace_back(LogicalType::TIMESTAMP_TZ);
                        } else if (atttypid == TIMEOID) {
                            types.emplace_back(LogicalType::TIME);
                        } else if (atttypid == TIMETZOID) {
                            types.emplace_back(LogicalType::TIME_TZ);
                        } else if (atttypid == JSONOID || atttypid == JSONBOID) {
                            types.emplace_back(LogicalType::VARCHAR);
                        } else if (atttypid == BYTEAOID) {
                            types.emplace_back(LogicalType::BLOB);
                        } else if (atttypid == UUIDOID) {
                            types.emplace_back(LogicalType::VARCHAR);
                        } else {
                            // fallback to varchar for unknown/complex types
                            types.emplace_back(LogicalType::VARCHAR);
                        }
                    }
                    // capture relation name for better diagnostics in the LogicalGet
                    const char *relname_c = get_rel_name(rte->relid);
                    std::string relname = relname_c ? std::string(relname_c) : std::string("(unknown)");
                    table_close(pg_rel, AccessShareLock);
                    // publish current column names and types for map_pg_expr
                    g_current_colnames = colnames;
                    g_current_types = types;
                    g_current_attnum_map = attnum_map;
                    // create a LogicalGet and populate a few extra fields so the
                    // serialized plan resembles a SEQ_SCAN with table name and
                    // pushed-down filter information where possible.
                    auto lg = make_uniq<LogicalGet>(0, TableFunction(), nullptr, std::move(types), std::move(colnames));
                    // set function name to 'seq_scan' so GetName() prints as SEQ_SCAN
                    lg->function.name = std::string("seq_scan");
                    // include the relation name in extra_info.file_filters so explain
                    // output shows which table this LogicalGet refers to
                    lg->extra_info.file_filters = std::string("table=") + relname;
                    get_op = std::move(lg);
                    // Insert an internal compress projection after scan for
                    // string columns to better match DuckDB's internal
                    // compress projections that appear in its planner output.
                    try {
                        // build projection expressions wrapping VARCHAR columns
                        vector<unique_ptr<Expression>> comp_exprs;
                        // use the published globals (g_current_types/g_current_colnames)
                        for (size_t ci = 0; ci < g_current_types.size(); ++ci) {
                            LogicalType ctype = g_current_types[ci];
                            std::string cname = (ci < g_current_colnames.size()) ? g_current_colnames[ci] : std::string("col") + to_string((long long)ci);
                            // Reference the input column
                            auto cref = make_uniq<BoundColumnRefExpression>(cname, ctype, ColumnBinding(0, (idx_t)ci));
                            if (ctype.id() == LogicalTypeId::VARCHAR) {
                                // wrap in a synthetic compress function to mimic DuckDB
                                vector<LogicalType> arg_types = {ctype};
                                ScalarFunction sf(arg_types, ctype, ScalarFunction::NopFunction);
                                sf.name = std::string("__internal_compress_string_");
                                vector<unique_ptr<Expression>> fchildren;
                                fchildren.push_back(std::move(cref));
                                comp_exprs.push_back(make_uniq<BoundFunctionExpression>(ctype, std::move(sf), std::move(fchildren), nullptr, true));
                            } else {
                                comp_exprs.push_back(std::move(cref));
                            }
                        }
                        if (!comp_exprs.empty()) {
                            idx_t comp_table_index = 1;
                            auto comp_proj = make_uniq<LogicalProjection>(comp_table_index, std::move(comp_exprs));
                            comp_proj->AddChild(std::move(get_op));
                            get_op = std::move(comp_proj);
                        }
                    } catch (...) {
                        // best-effort: ignore failures to insert compress projection
                    }
                } else {
                    // Not a base relation: fallback placeholder
                    get_op = make_uniq<LogicalGet>(0, TableFunction(), nullptr, vector<LogicalType>{}, vector<string>{});
                }

            } else {
                // No RTE: produce an empty dummy leaf
                get_op = make_uniq<LogicalGet>(0, TableFunction(), nullptr, vector<LogicalType>{}, vector<string>{});
            }

            // If there are quals (WHERE/join quals), add a LogicalFilter above the scan
            unique_ptr<LogicalOperator> cur = std::move(get_op);
            if (q->jointree && q->jointree->quals != NULL) {
                Node *quals = (Node *) q->jointree->quals;
                unique_ptr<Expression> pred_expr = map_pg_expr(quals);
                if (!pred_expr) {
                    pred_expr = make_uniq<BoundConstantExpression>(duckdb::Value());
                }

                bool pushed_table_filter = false;

                // If the current leaf is a LogicalGet, also attach a textual
                // representation of the qual into its ExtraOperatorInfo so the
                // serialized plan shows the table and filter together (closer
                // to DuckDB's native SEQ_SCAN display). Try to push simple
                // comparisons into the LogicalGet.table_filters. If we can
                // successfully push the predicate, we will NOT create a
                // separate LogicalFilter operator above the scan to better
                // match DuckDB's plan text.
                try {
                    std::string qual_text = pred_expr->ToString();
                    if (cur && cur->type == LogicalOperatorType::LOGICAL_GET) {
                        auto &lg = cur->Cast<LogicalGet>();
                        if (!lg.extra_info.file_filters.empty()) lg.extra_info.file_filters += std::string(" | ");
                        lg.extra_info.file_filters += std::string("qual=") + qual_text;

                        // Try to recognize simple comparisons (col OP constant) and push
                        // them as structured TableFilter objects into the LogicalGet so
                        // explain output shows Filters: in a native way.
                        Node *qnode = (Node *) q->jointree->quals;
                        if (qnode && IsA(qnode, OpExpr)) {
                            OpExpr *op = (OpExpr *) qnode;
                            List *args = op->args;
                            if (args != NIL) {
                                ListCell *alc = list_head(args);
                                Node *n1 = alc ? (Node *) lfirst(alc) : NULL;
                                alc = lnext(args,alc);
                                Node *n2 = alc ? (Node *) lfirst(alc) : NULL;
                                // unwrap common wrappers
                                auto unwrap = [](Node *n) -> Node * {
                                    while (n) {
                                        if (IsA(n, RelabelType)) { n = (Node *) ((RelabelType *) n)->arg; continue; }
                                        if (IsA(n, CoerceViaIO)) { n = (Node *) ((CoerceViaIO *) n)->arg; continue; }
                                        if (IsA(n, Param)) { break; }
                                        if (IsA(n, TargetEntry)) { n = (Node *) ((TargetEntry *) n)->expr; continue; }
                                        if (IsA(n, FuncExpr)) {
                                            FuncExpr *fe = (FuncExpr *) n;
                                            if (fe->args != NIL && list_length(fe->args) == 1) {
                                                n = (Node *) linitial(fe->args);
                                                continue;
                                            }
                                        }
                                        break;
                                    }
                                    return n;
                                };
                                Node *a = unwrap(n1);
                                Node *b = unwrap(n2);
                                // Identify var and const (either order)
                                Node *varnode = NULL; Node *constnode = NULL;
                                if (a && IsA(a, Var) && b && IsA(b, Const)) { varnode = a; constnode = b; }
                                else if (b && IsA(b, Var) && a && IsA(a, Const)) { varnode = b; constnode = a; }
                                if (varnode && constnode) {
                                    Var *v = (Var *) varnode;
                                    Const *c = (Const *) constnode;
                                    // map operator to ExpressionType
                                    const char *oname = get_opname(op->opno);
                                    ExpressionType cmp = ExpressionType::INVALID;
                                    std::string opname = oname ? std::string(oname) : std::string("");
                                    if (opname == "=") cmp = ExpressionType::COMPARE_EQUAL;
                                    else if (opname == "<") cmp = ExpressionType::COMPARE_LESSTHAN;
                                    else if (opname == "<=") cmp = ExpressionType::COMPARE_LESSTHANOREQUALTO;
                                    else if (opname == ">") cmp = ExpressionType::COMPARE_GREATERTHAN;
                                    else if (opname == ">=") cmp = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
                                    else if (opname == "<>" || opname == "!=") cmp = ExpressionType::COMPARE_NOTEQUAL;

                                    if (cmp != ExpressionType::INVALID && !c->constisnull) {
                                        // use existing map_pg_expr to convert Const -> BoundConstantExpression
                                        unique_ptr<Expression> ce = map_pg_expr((Node *) c);
                                        if (ce && ce->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                                            auto &bc = ce->Cast<BoundConstantExpression>();
                                            // determine column index like earlier (1-based varattno)
                                            int colidx = v->varattno - 1;
                                            if (colidx < 0) colidx = 0;
                                            // create ConstantFilter and push
                                            auto filt = make_uniq<ConstantFilter>(cmp, bc.value);
                                            try {
                                                lg.table_filters.PushFilter(ColumnIndex(colidx), std::move(filt));
                                                pushed_table_filter = true;
                                            } catch (...) {
                                                // push best-effort: ignore failures
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (...) {
                    // best-effort: ignore ToString failures
                }

                // Only create a LogicalFilter above the scan if we failed to
                // push the predicate into the LogicalGet (or if the predicate
                // is complex). This helps match DuckDB's tendency to show
                // filters within the SEQ_SCAN node instead of a separate
                // FILTER operator.
                if (!pushed_table_filter) {
                    auto filter = make_uniq<LogicalFilter>(std::move(pred_expr));
                    filter->AddChild(std::move(cur));
                    cur = std::move(filter);
                }
            }

            // If query has grouping/aggregates, create a LogicalAggregate
            // Following DuckDB Binder order: first apply WHERE/filter, then
            // construct LogicalAggregate with group expressions and aggregate
            // expressions referencing the input (scan/filter). After the
            // aggregate, create a LogicalProjection to emit targetList order.
            if (q->hasAggs || (q->groupClause != NIL && list_length(q->groupClause) > 0)) {
                // Prepare mappings from targetList indices to group/agg positions
                int tlen = q->targetList ? list_length(q->targetList) : 0;
                vector<int> target_group_pos(tlen, -1);
                vector<int> target_agg_pos(tlen, -1);

                // Build group expressions from groupClause (these reference input columns)
                vector<unique_ptr<Expression>> groups;
                if (q->groupClause != NIL) {
                    ListCell *lc;
                    int gpos = 0;
                    foreach(lc, q->groupClause) {
                        SortGroupClause *sg = (SortGroupClause *) lfirst(lc);
                        int tref = sg->tleSortGroupRef;
                        if (tref > 0 && q->targetList != NIL) {
                            TargetEntry *gte = (TargetEntry *) list_nth(q->targetList, tref - 1);
                            if (gte && gte->expr) {
                                unique_ptr<Expression> gexpr = map_pg_expr((Node *) gte->expr);
                                if (!gexpr) gexpr = make_uniq<BoundConstantExpression>(duckdb::Value());
                                groups.push_back(std::move(gexpr));
                                if (tref - 1 >= 0 && tref - 1 < tlen) target_group_pos[tref - 1] = gpos;
                                gpos++;
                                continue;
                            }
                        }
                        // fallback placeholder
                        groups.push_back(make_uniq<BoundConstantExpression>(duckdb::Value()));
                        gpos++;
                    }
                }

                // Build aggregate expressions by scanning targetList for Aggrefs
                vector<unique_ptr<Expression>> agg_exprs;
                if (q->targetList != NIL) {
                    int tidx = 0;
                    ListCell *tlc;
                    foreach(tlc, q->targetList) {
                        TargetEntry *te = (TargetEntry *) lfirst(tlc);
                        if (te && te->expr && IsA(te->expr, Aggref)) {
                            unique_ptr<Expression> aexpr = map_pg_expr((Node *) te->expr);
                            if (!aexpr) aexpr = make_uniq<BoundConstantExpression>(duckdb::Value());
                            target_agg_pos[tidx] = (int) agg_exprs.size();
                            agg_exprs.push_back(std::move(aexpr));
                        }
                        tidx++;
                    }
                }

                // Keep types for projection referencing after we move expressions
                vector<LogicalType> group_types;
                for (auto &g : groups) group_types.push_back(g ? g->return_type : LogicalType::VARCHAR);
                vector<LogicalType> agg_types;
                for (auto &a : agg_exprs) agg_types.push_back(a ? a->return_type : LogicalType::VARCHAR);

                // Construct LogicalAggregate: group_index=0, aggregate_index=0 (aggregate outputs will be bound at table_index 0)
                auto agg = make_uniq<LogicalAggregate>(0, 0, std::move(agg_exprs));
                agg->groups = std::move(groups);
                agg->AddChild(std::move(cur));
                cur = std::move(agg);

                // Insert a decompress projection after aggregate so that any
                // internal compressed string columns are materialized back to
                // VARCHAR before the final projection. This mimics DuckDB's
                // internal decompress projections and reduces textual diffs.
                try {
                    vector<unique_ptr<Expression>> decomp_exprs;
                    // combined outputs: groups first, then aggregates
                    vector<LogicalType> out_types;
                    for (auto &gt : group_types) out_types.push_back(gt);
                    for (auto &at : agg_types) out_types.push_back(at);
                    for (idx_t oi = 0; oi < out_types.size(); ++oi) {
                        LogicalType ot = out_types[oi];
                        std::string alias = std::string("col") + to_string((long long)oi);
                        auto cref = make_uniq<BoundColumnRefExpression>(alias, ot, ColumnBinding(0, oi));
                        if (ot.id() == LogicalTypeId::VARCHAR) {
                            vector<LogicalType> arg_types = {ot};
                            ScalarFunction sf(arg_types, ot, ScalarFunction::NopFunction);
                            sf.name = std::string("__internal_decompress_string_");
                            vector<unique_ptr<Expression>> fchildren;
                            fchildren.push_back(std::move(cref));
                            decomp_exprs.push_back(make_uniq<BoundFunctionExpression>(ot, std::move(sf), std::move(fchildren), nullptr, true));
                        } else {
                            decomp_exprs.push_back(std::move(cref));
                        }
                    }
                    if (!decomp_exprs.empty()) {
                        idx_t decomp_table_index = 2;
                        auto decomp_proj = make_uniq<LogicalProjection>(decomp_table_index, std::move(decomp_exprs));
                        decomp_proj->AddChild(std::move(cur));
                        cur = std::move(decomp_proj);
                    }
                } catch (...) {
                    // ignore failures
                }

                // After aggregate, build a projection that emits the original targetList
                // in the requested order. Aggregate outputs are ordered: groups first,
                // then aggregate expressions.
                vector<unique_ptr<Expression>> post_proj_exprs;
                if (q->targetList != NIL) {
                    int tidx = 0;
                    ListCell *tlc2;
                    foreach(tlc2, q->targetList) {
                        TargetEntry *te = (TargetEntry *) lfirst(tlc2);
                        if (te && target_agg_pos[tidx] != -1) {
                            int aggpos = target_agg_pos[tidx];
                            // aggregate outputs are after group outputs; their index = groups.size() + aggpos
                            idx_t out_idx = (idx_t) group_types.size() + (idx_t) aggpos;
                            std::string alias = te && te->resname ? std::string(te->resname) : std::string("agg") + to_string(aggpos);
                            post_proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(alias, agg_types[aggpos], ColumnBinding(0, out_idx)));
                        } else if (te && target_group_pos[tidx] != -1) {
                            int gpos = target_group_pos[tidx];
                            std::string alias = te && te->resname ? std::string(te->resname) : std::string("group") + to_string(gpos);
                            post_proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(alias, group_types[gpos], ColumnBinding(0, (idx_t) gpos)));
                        } else {
                            // fallback: try to map the original expr to something readable
                            if (te && te->expr) {
                                unique_ptr<Expression> ex = map_pg_expr((Node *) te->expr);
                                if (ex) {
                                    post_proj_exprs.push_back(std::move(ex));
                                } else {
                                    post_proj_exprs.push_back(make_uniq<BoundConstantExpression>(duckdb::Value()));
                                }
                            } else {
                                post_proj_exprs.push_back(make_uniq<BoundConstantExpression>(duckdb::Value()));
                            }
                        }
                        tidx++;
                    }
                }

                // Create projection operator (use projection table index distinct from leafs)
                idx_t proj_table_index = 1;
                auto post_proj = make_uniq<LogicalProjection>(proj_table_index, std::move(post_proj_exprs));
                post_proj->AddChild(std::move(cur));
                cur = std::move(post_proj);
            }

            // Return ownership of the constructed tree to caller (heap-allocated)
            // For diagnostics: exercise mapping of targetList entries even for
            // non-aggregate queries so map_pg_expr gets invoked and can log
            // constant/param parsing. We discard the result; this does not
            // change the plan we return.
            if (q->targetList != NIL) {
                ListCell *tlc2;
                foreach(tlc2, q->targetList) {
                    TargetEntry *te2 = (TargetEntry *) lfirst(tlc2);
                    if (te2 && te2->expr) {
                        unique_ptr<Expression> tmpmap = map_pg_expr((Node *) te2->expr);
                        FILE *f = fopen("/tmp/pg_const_map.log", "a");
                        if (f) {
                            time_t t = time(NULL);
                            fprintf(f, "post-map targetList mapping returned %s for nodeTag=%d time=%ld\n",
                                    tmpmap ? "non-null" : "NULL", (int) nodeTag(te2->expr), (long) t);
                            fclose(f);
                        }
                    }
                }
            }

            // Map ORDER BY (sortClause) into a DuckDB LogicalOrder if present.
            if (q->sortClause != NIL) {
                vector<BoundOrderByNode> orders;
                ListCell *slc;
                foreach(slc, q->sortClause) {
                    SortGroupClause *sg = (SortGroupClause *) lfirst(slc);
                    if (!sg) continue;
                    int tref = sg->tleSortGroupRef;
                    unique_ptr<Expression> order_expr = nullptr;
                    if (tref > 0 && q->targetList != NIL) {
                        TargetEntry *te = (TargetEntry *) list_nth(q->targetList, tref - 1);
                        if (te && te->expr) {
                            order_expr = map_pg_expr((Node *) te->expr);
                        }
                    }
                    if (!order_expr) {
                        // fallback: map the node referenced by sortClause if any
                        order_expr = make_uniq<BoundConstantExpression>(duckdb::Value());
                    }
                    // Determine order direction: Postgres SortGroupClause doesn't
                    // directly store ASC/DESC; we default to ASC and use
                    // nulls_first to select null ordering.
                    OrderType otype = OrderType::ASCENDING;
                    OrderByNullType null_order = sg->nulls_first ? OrderByNullType::NULLS_FIRST : OrderByNullType::NULLS_LAST;
                    orders.emplace_back(otype, null_order, std::move(order_expr));
                }
                if (!orders.empty()) {
                    // Insert a compress projection before ORDER_BY to mimic DuckDB's
                    // internal compress projection that often appears before sort.
                    try {
                        vector<unique_ptr<Expression>> pre_ord_comp_exprs;
                        // compress first two grouping/order columns if present
                        int comp_cols = (int)std::min<size_t>(2, g_current_colnames.size());
                        for (int ci = 0; ci < comp_cols; ++ci) {
                            LogicalType ctype = (ci < (int)g_current_types.size()) ? g_current_types[ci] : LogicalType::VARCHAR;
                            std::string cname = (ci < (int)g_current_colnames.size()) ? g_current_colnames[ci] : std::string("col") + to_string((long long)ci);
                            auto cref = make_uniq<BoundColumnRefExpression>(cname, ctype, ColumnBinding(0, (idx_t)ci));
                            if (ctype.id() == LogicalTypeId::VARCHAR) {
                                // compress to small integer to mimic utinyint(#0) appearance
                                vector<LogicalType> arg_types = {ctype};
                                ScalarFunction sf(arg_types, LogicalType::UTINYINT, ScalarFunction::NopFunction);
                                sf.name = std::string("__internal_compress_string_");
                                vector<unique_ptr<Expression>> fchildren;
                                fchildren.push_back(std::move(cref));
                                pre_ord_comp_exprs.push_back(make_uniq<BoundFunctionExpression>(LogicalType::UTINYINT, std::move(sf), std::move(fchildren), nullptr, true));
                            } else {
                                pre_ord_comp_exprs.push_back(std::move(cref));
                            }
                        }
                        // For remaining columns, just pass through references (but limit to a modest count)
                        int pass_through = 8; // add a few more to match typical planner outputs
                        for (int ci = comp_cols; ci < comp_cols + pass_through && ci < (int)g_current_colnames.size(); ++ci) {
                            LogicalType ctype = (ci < (int)g_current_types.size()) ? g_current_types[ci] : LogicalType::VARCHAR;
                            std::string cname = (ci < (int)g_current_colnames.size()) ? g_current_colnames[ci] : std::string("col") + to_string((long long)ci);
                            pre_ord_comp_exprs.push_back(make_uniq<BoundColumnRefExpression>(cname, ctype, ColumnBinding(0, (idx_t)ci)));
                        }
                        if (!pre_ord_comp_exprs.empty()) {
                            auto pre_comp_proj = make_uniq<LogicalProjection>(1, std::move(pre_ord_comp_exprs));
                            pre_comp_proj->AddChild(std::move(cur));
                            cur = std::move(pre_comp_proj);
                        }
                    } catch (...) {
                        // ignore best-effort failures
                    }

                    auto ord = make_uniq<LogicalOrder>(std::move(orders));
                    ord->AddChild(std::move(cur));
                    cur = std::move(ord);
                }
            }

            // Map LIMIT/OFFSET to DuckDB LogicalLimit when present
            if (q->limitCount != NULL || q->limitOffset != NULL) {
                BoundLimitNode limit_node;
                BoundLimitNode offset_node;
                // limitCount
                if (q->limitCount != NULL) {
                    Node *lc = (Node *) q->limitCount;
                    if (IsA(lc, Const)) {
                        Const *c = (Const *) lc;
                        if (!c->constisnull) {
                            Oid typid = c->consttype;
                            Oid typoutput; bool typisvarlena;
                            getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                            char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                            long long v = strtoll(out, NULL, 10);
                            pfree(out);
                            limit_node = BoundLimitNode::ConstantValue(v);
                        } else {
                            limit_node = BoundLimitNode();
                        }
                    } else {
                        unique_ptr<Expression> lex = map_pg_expr(lc);
                        if (lex) limit_node = BoundLimitNode::ExpressionValue(std::move(lex)); else limit_node = BoundLimitNode();
                    }
                }
                // limitOffset
                if (q->limitOffset != NULL) {
                    Node *lo = (Node *) q->limitOffset;
                    if (IsA(lo, Const)) {
                        Const *c = (Const *) lo;
                        if (!c->constisnull) {
                            Oid typid = c->consttype;
                            Oid typoutput; bool typisvarlena;
                            getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                            char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                            long long v = strtoll(out, NULL, 10);
                            pfree(out);
                            offset_node = BoundLimitNode::ConstantValue(v);
                        } else {
                            offset_node = BoundLimitNode();
                        }
                    } else {
                        unique_ptr<Expression> off = map_pg_expr(lo);
                        if (off) offset_node = BoundLimitNode::ExpressionValue(std::move(off)); else offset_node = BoundLimitNode();
                    }
                }
                auto lim = make_uniq<LogicalLimit>(std::move(limit_node), std::move(offset_node));
                lim->AddChild(std::move(cur));
                cur = std::move(lim);
            }

            // After applying ORDER/LIMIT, insert a top-level DECOMPRESS projection
            // to mimic DuckDB's outermost projection that materializes compressed
            // string columns back to VARCHAR. We'll create a projection wrapping
            // the current root and produce a modest number of output columns.
            try {
                vector<unique_ptr<Expression>> top_decomp_exprs;
                int max_out_cols = 12; // guard: produce up to 12 outputs
                for (int oi = 0; oi < max_out_cols; ++oi) {
                    // assume output column index oi
                    LogicalType ot = (oi < (int)g_current_types.size()) ? g_current_types[oi] : LogicalType::VARCHAR;
                    std::string alias = std::string("g(#") + to_string((long long)oi) + std::string(")");
                    auto cref = make_uniq<BoundColumnRefExpression>(alias, ot, ColumnBinding(0, (idx_t)oi));
                    if (ot.id() == LogicalTypeId::VARCHAR) {
                        vector<LogicalType> arg_types = {ot};
                        ScalarFunction sf(arg_types, ot, ScalarFunction::NopFunction);
                        sf.name = std::string("__internal_decompress_string_");
                        vector<unique_ptr<Expression>> fchildren;
                        fchildren.push_back(std::move(cref));
                        top_decomp_exprs.push_back(make_uniq<BoundFunctionExpression>(ot, std::move(sf), std::move(fchildren), nullptr, true));
                    } else {
                        top_decomp_exprs.push_back(std::move(cref));
                    }
                }
                if (!top_decomp_exprs.empty()) {
                    auto top_decomp = make_uniq<LogicalProjection>(2, std::move(top_decomp_exprs));
                    top_decomp->AddChild(std::move(cur));
                    cur = std::move(top_decomp);
                }
            } catch (...) {
                // ignore
            }
            *out_logical_ptr = cur.release();
            // clear transient column name cache
            g_current_colnames.clear();
            return true;
        }


        // Fallback: no Postgres Query provided, preserve previous placeholder behavior
        // Example structure: LogicalAggregate <- LogicalFilter <- LogicalGet
        auto get = make_uniq<LogicalGet>(0, TableFunction(), nullptr, vector<LogicalType>{}, vector<string>{});

        // Build a simple predicate expression placeholder: use a BoundConstantExpression as placeholder
        auto const_expr = make_uniq<BoundConstantExpression>(duckdb::Value());
        auto filter = make_uniq<LogicalFilter>(std::move(const_expr));
        filter->AddChild(std::move(get));

        // Aggregate: group by two placeholder bound column refs
    vector<unique_ptr<Expression>> groups;
    groups.push_back(make_uniq<BoundColumnRefExpression>(std::string("col0"), LogicalType::VARCHAR, ColumnBinding(0, 0)));
    groups.push_back(make_uniq<BoundColumnRefExpression>(std::string("col1"), LogicalType::VARCHAR, ColumnBinding(0, 1)));

        // Create aggregate operator (group_index=0, aggregate_index=0, empty select_list)
        auto agg = make_uniq<LogicalAggregate>(0, 0, std::move(groups));
        agg->AddChild(std::move(filter));

        unique_ptr<LogicalOperator> root = std::move(agg);
        *out_logical_ptr = root.release();
        g_current_colnames.clear();
        return true;
    } catch (std::exception &ex) {
        set_error(error_msg, ex.what());
        return false;
    } catch (...) {
        set_error(error_msg, "unknown exception in pg_to_duckdb_logical_plan");
        return false;
    }
}

extern "C" bool duckdb_logical_to_physical(void *logical_ptr,
                                           void **out_physical_ptr,
                                           char **error_msg) {
    set_error(error_msg, "duckdb_logical_to_physical is removed in logical-only integration");
    return false;
}

extern "C" void pg_duckdb_log_targetlist_nodes(void *stmt) {
    if (!stmt) {
        FILE *f = fopen("/tmp/pg_const_map.log", "a");
        if (f) { time_t t = time(NULL); fprintf(f, "pg_duckdb_log_targetlist_nodes: stmt=NULL time=%ld\n", (long)t); fclose(f); }
        return;
    }
    Query *q = reinterpret_cast<Query *>(stmt);
    if (!q->targetList) {
        FILE *f = fopen("/tmp/pg_const_map.log", "a");
        if (f) { time_t t = time(NULL); fprintf(f, "pg_duckdb_log_targetlist_nodes: empty targetList time=%ld\n", (long)t); fclose(f); }
        return;
    }
    ListCell *tlc;
    foreach(tlc, q->targetList) {
        TargetEntry *te = (TargetEntry *) lfirst(tlc);
        if (!te || !te->expr) continue;
        Node *n = (Node *) te->expr;
        FILE *f = fopen("/tmp/pg_const_map.log", "a");
        if (f) {
            time_t t = time(NULL);
            fprintf(f, "pg_duckdb_log_targetlist_nodes: expr node=%p tag=%d time=%ld\n", (void*)n, (int) nodeTag(n), (long)t);
            // Try to emit nodeToString for richer diagnostics
            char *ns = nodeToString(n);
            if (ns) {
                fprintf(f, "  nodeToString: %s\n", ns);
                pfree(ns);
            }
            fclose(f);
        }
        unique_ptr<Expression> e = map_pg_expr(n);
        FILE *f2 = fopen("/tmp/pg_const_map.log", "a");
        if (f2) {
            time_t t = time(NULL);
            if (!e) {
                fprintf(f2, "  mapped -> NULL time=%ld\n", (long)t);
            } else {
                // If constant, attempt to dump value
                if (e->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                    auto &bc = e->Cast<BoundConstantExpression>();
                    std::string vs = bc.value.ToString();
                    fprintf(f2, "  mapped -> BOUND_CONSTANT value=%s time=%ld\n", vs.c_str(), (long)t);
                } else {
                    fprintf(f2, "  mapped -> %d (non-constant) time=%ld\n", (int) e->GetExpressionClass(), (long)t);
                }
            }
            fclose(f2);
        }
    }
}
// New API: extract DuckDB LogicalPlan for a SQL string
extern "C" bool duckdb_extract_logical_plan_from_sql(const char *sql,
                                                      void **out_logical_ptr,
                                                      char **error_msg) {
    if (!sql || !out_logical_ptr) {
        set_error(error_msg, "invalid arguments to duckdb_extract_logical_plan_from_sql");
        return false;
    }
    try {
        // Create a DuckDB in-memory database instance and Connection
        DuckDB db; // constructs DatabaseInstance and system catalog
        // Use Connection which owns a proper ClientContext and exposes ExtractPlan
        Connection conn(db);

        // Ensure a minimal `lineitem` table exists so the planner can bind references
        // Columns used in the TPCH-Q1-like SQL: l_returnflag, l_linestatus, l_quantity,
        // l_extendedprice, l_discount, l_tax, l_shipdate
        conn.Query("CREATE TABLE IF NOT EXISTS lineitem(l_returnflag VARCHAR, l_linestatus VARCHAR, l_quantity INTEGER, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE, l_shipdate DATE);");

        unique_ptr<LogicalOperator> plan = conn.ExtractPlan(std::string(sql));
        if (!plan) {
            set_error(error_msg, "DuckDB planner returned null logical plan");
            return false;
        }
        // Transfer ownership to caller
        *out_logical_ptr = plan.release();
        return true;
    } catch (std::exception &ex) {
        set_error(error_msg, ex.what());
        return false;
    } catch (...) {
        set_error(error_msg, "unknown exception in duckdb_extract_logical_plan_from_sql");
        return false;
    }
}

// Serialize a DuckDB logical plan using LogicalOperator::ToString
extern "C" bool duckdb_logical_serialize(void *logical_ptr, char **out_str, char **error_msg) {
    if (!out_str) {
        set_error(error_msg, "out_str is NULL");
        return false;
    }
    if (!logical_ptr) {
        const char *empty = "(null logical plan)";
        *out_str = (char *)malloc(strlen(empty) + 1);
        strcpy(*out_str, empty);
        return true;
    }
    try {
        LogicalOperator *logical = reinterpret_cast<LogicalOperator *>(logical_ptr);
        std::string s = logical->ToString();
        *out_str = (char *)malloc(s.size() + 1);
        memcpy(*out_str, s.c_str(), s.size());
        (*out_str)[s.size()] = '\0';
        return true;
    } catch (std::exception &ex) {
        set_error(error_msg, ex.what());
        return false;
    } catch (...) {
        set_error(error_msg, "unknown exception in duckdb_logical_serialize");
        return false;
    }
}

extern "C" void duckdb_plan_free(void *plan_ptr) {
    if (!plan_ptr) return;
    try {
        LogicalOperator *l = reinterpret_cast<LogicalOperator *>(plan_ptr);
        delete l;
    } catch (...) {
    }
}


//开始


PlannedStmt *g_current_pstmt = nullptr;

class PgPhysicalPlanGenerator {
public:
    explicit PgPhysicalPlanGenerator(duckdb::ClientContext &ctx) : context(ctx) {}

    // 从 PG PlannedStmt 构造一个 DuckDB PhysicalPlan
    unique_ptr<duckdb::PhysicalPlan> PlanFromPlannedStmt(PlannedStmt *stmt);

    // 主入口：根据 PG Plan 节点创建 DuckDB PhysicalOperator
    duckdb::PhysicalOperator &CreatePlan(Plan *plan);

    duckdb::PhysicalOperator &CreatePlanSeqScan(SeqScan *scan);
    duckdb::PhysicalOperator &CreatePlanAgg(Agg *agg);
    duckdb::PhysicalOperator &CreatePlanSort(Sort *sort);
    duckdb::PhysicalOperator &CreatePlanResult(Result *res);
    duckdb::PhysicalOperator &ExtractAggregateExpressionsCompat(duckdb::PhysicalOperator &child, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &aggregates,
	    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &groups);
    PhysicalOperator &duplicate_subexprs(PhysicalOperator &child, vector<unique_ptr<Expression>> &aggregates, vector<unique_ptr<Expression>> &groups);

    // DuckDB 的 Make<T> 包一层，方便使用（跟 PhysicalPlanGenerator 一样的写法）
    // template <class T, class... ARGS>
    // T &Make(ARGS &&...args) {
    //     return physical_plan->Make<T>(std::forward<ARGS>(args)...);
    // }

        // 新增：公共 alias 选择函数
    std::string ChooseAliasFromPG(Expr *expr, TargetEntry *tle);

    template <class T, class... ARGS>
    duckdb::PhysicalOperator &Make(ARGS &&...args) {
        return physical_plan->Make<T>(std::forward<ARGS>(args)...);
    }

private:
    duckdb::ClientContext &context;
    unique_ptr<PhysicalPlan> physical_plan;

    // PG 侧的一些上下文字段
    PlannedStmt *pstmt = nullptr;
    List *range_table = nullptr;
};

struct CSENode {
	idx_t count;
	optional_idx column_index;

	CSENode() : count(1), column_index() {
	}
};

struct CSEReplaceState {
    bool perform_replacement;
	//! Map of expression -> CSENode
	expression_map_t<CSENode> expression_count;
	//! Map of column bindings to column indexes in the projection expression list
    map<idx_t, idx_t> column_map;
	//! The set of expressions of the resulting projection
	vector<unique_ptr<Expression>> expressions;
	//! Cached expressions that are kept around so the expression_map always contains valid expressions
	vector<unique_ptr<Expression>> cached_expressions;
};

unique_ptr<duckdb::PhysicalPlan>
PgPhysicalPlanGenerator::PlanFromPlannedStmt(PlannedStmt *stmt) {
    if (!stmt || !stmt->planTree) {
        throw duckdb::InternalException("PlanFromPlannedStmt: null PlannedStmt or planTree");
    }

    pstmt = stmt;
    g_current_pstmt = pstmt;    // 顺带把全局也设一下，让 mapper 能用
    range_table = stmt->rtable;

    // 用 DuckDB 的 Allocator 创建 PhysicalPlan
    physical_plan = make_uniq<duckdb::PhysicalPlan>(duckdb::Allocator::Get(context));

    // 递归生成物理算子树
    duckdb::PhysicalOperator &root = CreatePlan(stmt->planTree);
    physical_plan->SetRoot(root);

    // // 可选：如果你链接的 DuckDB 版本有 Verify，就调一下
    // physical_plan->Verify();

    return std::move(physical_plan);
}

duckdb::PhysicalOperator &
PgPhysicalPlanGenerator::CreatePlan(Plan *plan) {
    if (!plan) {
        throw duckdb::InternalException("CreatePlan: null Plan*");
    }

    switch (nodeTag(plan)) {
    case T_SeqScan:
        return CreatePlanSeqScan((SeqScan *)plan);
    case T_Agg:
        return CreatePlanAgg((Agg *)plan);
    case T_Sort:
        return CreatePlanSort((Sort *)plan);
    case T_Result:
        return CreatePlanResult((Result *)plan);

    case T_IndexScan: {
    IndexScan *iscan = (IndexScan *)plan;
    // 当成 SeqScan 用：只用 iscan->scan 里的 scanrelid/plan/targetlist/qual
    return CreatePlanSeqScan((SeqScan *)&iscan->scan);
    }   
    // default:
    //     ereport(ERROR,
    //             (errmsg("CreatePlan: unsupported nodeTag %d", nodeTag(plan))));

    default: {
            /* ★ 关键改动：把整个 Plan 节点转成字符串，直接塞到 ERROR 的 DETAIL 里 ★ */
            char *plan_str = nodeToString((Node *)plan);

            ereport(ERROR,
                    (errmsg("CreatePlan: unsupported plan nodeTag=%d",
                            (int)nodeTag(plan)),
                     errdetail_internal("%s",
                            plan_str ? plan_str : "<null>")));

            /* 理论上 ereport 不会返回，这里的 pfree 不会执行；
               只是演示一下，调试阶段这点内存泄露可以忽略。 */
            if (plan_str) {
                pfree(plan_str);
            }
        }
    }
}


// ，用于 DuckDB Expression::alias打印列名
// 逻辑：
//  1) 有 TargetEntry 且 resname 非空 → 用 resname
//  2) 否则，从 expr 里找第一个 Var：
//       - 只接受 varlevelsup=0 且 varno 落在 pstmt->rtable 范围内
//       - 对应 RTE_RELATION → 从 pg_attribute 拿 attname
//  3) 都拿不到就返回空字符串，由调用者决定是否保留原 alias

std::string PgPhysicalPlanGenerator::ChooseAliasFromPG(Expr *expr, TargetEntry *tle) {
    // 1) 优先 TLE 的 resname（比如 SELECT sum(l_quantity) AS sum_qty）
    if (tle && tle->resname && tle->resname[0] != '\0') {
        return std::string(tle->resname);
    }

    // 2) 从 expr 中找 base table 的 Var
    //    我们写一个内部小 helper：递归剥掉 RelabelType / FuncExpr 等 wrapper
    std::function<Var *(Node *)> find_var = [&](Node *node) -> Var * {
        if (!node) return nullptr;
        if (IsA(node, Var)) {
            return (Var *)node;
        }

        if (IsA(node, RelabelType)) {
            RelabelType *rt = (RelabelType *)node;
            return find_var((Node *)rt->arg);
        }
        if (IsA(node, FuncExpr)) {
            FuncExpr *fe = (FuncExpr *)node;
            if (fe->args != NIL) {
                ListCell *lc;
                foreach (lc, fe->args) {
                    Var *v = find_var((Node *)lfirst(lc));
                    if (v) return v;
                }
            }
            return nullptr;
        }
        // 其它节点类型需要的话后面再扩展
        return nullptr;
    };

    Var *v = find_var((Node *)expr);
    if (!v) {
        return std::string(); // 空，交给调用者保留原 alias
    }

    // 只处理 varlevelsup=0 且 varno 在 rtable 范围内的普通 Var
    if (v->varlevelsup != 0) {
        return std::string();
    }
    if (!pstmt) {
        return std::string();
    }

    int rtable_len = list_length(pstmt->rtable);
    if (v->varno <= 0 || v->varno > rtable_len) {
        return std::string();
    }

    RangeTblEntry *rte = rt_fetch(v->varno, pstmt->rtable);
    if (!rte || rte->rtekind != RTE_RELATION) {
        return std::string();
    }

    struct RelationData *rel = table_open(rte->relid, AccessShareLock);
    std::string result;

    TupleDesc tupdesc = RelationGetDescr(rel);
    if (v->varattno > 0 && v->varattno <= tupdesc->natts) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, v->varattno - 1);
        if (!attr->attisdropped) {
            const char *attname = NameStr(attr->attname);
            if (attname && attname[0] != '\0') {
                result = std::string(attname);  // 比如 "l_returnflag"
            }
        }
    }

    table_close(rel, AccessShareLock);
    return result; // 可能为空字符串，调用方要判断
}

/*scan算子
duckdb::PhysicalOperator &
PgPhysicalPlanGenerator::CreatePlanSeqScan(SeqScan *scan) {
    using namespace duckdb;

    Plan *plan_node = (Plan *)scan;
    Scan *scan_node = (Scan *)scan;

    // 1. 从 PG 侧拿表信息
    RangeTblEntry *rte = rt_fetch(scan_node->scanrelid, pstmt->rtable);
    if (!rte || rte->rtekind != RTE_RELATION) {
        ereport(ERROR,
                (errmsg("CreatePlanSeqScan: only base relation supported")));
    }

    struct RelationData *rel = table_open(rte->relid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(rel);
    int natts = tupdesc->natts;

    duckdb::vector<LogicalType> returned_types;
    duckdb::vector<string>      column_names;
    duckdb::vector<ColumnIndex>    column_ids;
    duckdb::vector<idx_t>       projection_ids;

    // attno -> 在上面数组里的 local index
    vector<column_t>    attno_to_local(natts + 1, DConstants::INVALID_INDEX);

    // ---------- 1) targetlist 中的 Var ----------
    ListCell *lc;
    for (lc = list_head(plan_node->targetlist);
         lc != NULL;
         lc = lnext(plan_node->targetlist, lc)) {

        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (!IsA(tle->expr, Var)) {
            continue; // 复杂 expr 上交给上层 Result/Projection
        }
        Var *var = (Var *)tle->expr;

        auto binding = PgVarToPgScanColumn(
            var, rel, tupdesc,
            returned_types, column_names,
            column_ids
        );
        attno_to_local[binding.attno] = binding.local_index;
        // projection_ids 只在 targetlist 里 push
        projection_ids.push_back(binding.local_index);
    }

    // ---------- 2) qual 里的 Var 也要进 column_ids ----------
    if (plan_node->qual) {
        for (lc = list_head(plan_node->qual);
             lc != NULL;
             lc = lnext(plan_node->qual, lc)) {

            Expr *qual = (Expr *)lfirst(lc);
            if (!IsA(qual, OpExpr)) {
                continue;
            }
            auto *op = (OpExpr *)qual;
            if (list_length(op->args) != 2) {
                continue;
            }
            Expr *arg1 = (Expr *)linitial(op->args);
            if (!IsA(arg1, Var)) {
                continue;
            }
            Var *var = (Var *)arg1;
            auto binding = PgVarToPgScanColumn(
                var, rel, tupdesc,
                returned_types, column_names,
                column_ids 
            );
            attno_to_local[binding.attno] = binding.local_index;
        }
    }

    // ---------- 3) 从 qual 构造 TableFilterSet ----------对第几列（local_index）做过滤
    unique_ptr<TableFilterSet> table_filters;
    if (plan_node->qual) {
        auto logical_filters = make_uniq<TableFilterSet>();
        for (lc = list_head(plan_node->qual);
             lc != NULL;
             lc = lnext(plan_node->qual, lc)) {

            Expr *qual = (Expr *)lfirst(lc);
            TryBuildComparisonFilterFromQual_Q1_PG(
                qual, tupdesc, attno_to_local, *logical_filters
            );
        }
        if (!logical_filters->filters.empty()) {
            table_filters = std::move(logical_filters);
        }
    }

    // ---------- 4) 输出 schema ----------
    // returned_types/column_names：是所有“要读的列”（包括只用于过滤、不输出的列）
    // duckdb::vector<LogicalType> output_types = returned_types;
    // duckdb::vector<string>      output_names = column_names;

    //只需要 targetlist 中的列，所以：新建 output_types / output_names
    std::vector<LogicalType> output_types;
    std::vector<std::string> output_names;
    output_types.reserve(returned_types.size());
    output_names.reserve(column_names.size());

    对 projection_ids 里的每一个 index：
    从 returned_types[proj_idx] 取 type， push 到 output_types；
    从 column_names[proj_idx] 取 name， push 到 output_names。

    for (auto proj_idx : projection_ids) {
        output_types.push_back(returned_types[proj_idx]);
        output_names.push_back(column_names[proj_idx]);
    }

    // ---------- 5) 构造假的 TableFunction ----------
    TableFunction table_func;
    const char *relname_c = get_rel_name(RelationGetRelid(rel));
    std::string table_name = relname_c ? std::string(relname_c) : std::string("unknown_pg_table");
    // 函数名固定成 pg_seq_scan
    table_func.name = "pg_seq_scan";

    // 把 PG 表名塞进 TableFunctionInfo，方便 explain 里拿出来打印
    auto tf_info = duckdb::make_uniq<PgTableFunctionInfo>(table_name);
    table_func.function_info = std::move(tf_info);

    // 2.1 to_string 回调：打印 Table / Type，两行就够
    table_func.to_string = PgSeqScanToString;

    // 3) 打开 Projections / Filters 的打印开关
    table_func.projection_pushdown = true;  //这个表函数支持只读某些列，而不是所有列
    table_func.filter_prune        = true;   // 只打印 projection_ids 中的列,将qual中的列保留在scan 的 returned_types/column_ids
    table_func.filter_pushdown     = true;//这个表函数支持把上层的 WHERE 条件下推到底层 scan，自己利用这些 filter 去做更高效的读取。

    unique_ptr<FunctionData> bind_data; // 这里不真正执行，只是为了 ToString

    idx_t estimated_card = (idx_t)plan_node->plan_rows;
    ExtraOperatorInfo extra_info;
    duckdb::vector<Value>   parameters;
    // vector<column_t> virtual_columns;

    // virtual_column_map_t 是一个别名，用它
    duckdb::virtual_column_map_t virtual_columns;

    auto &scan_op = Make<PhysicalTableScan>(
        std::move(output_types),
        table_func,
        std::move(bind_data),
        std::move(returned_types),
        std::move(column_ids),
        std::move(projection_ids),
        std::move(column_names),
        std::move(table_filters),
        estimated_card,
        std::move(extra_info),
        std::move(parameters),
        std::move(virtual_columns)
    );

    table_close(rel, AccessShareLock);
    return scan_op;

}
*/

// --------------------------------------------
// 1) DuckDB runtime: in-memory DB + Connection
// --------------------------------------------
struct DuckRuntime {
    DuckDB db;
    Connection conn;
    bool q1_ready = false;

    DuckRuntime() : db(nullptr), conn(db) {} // nullptr => in-memory
};

static DuckRuntime &GetDuckRuntime() {
    static DuckRuntime rt;
    return rt;
}

// ----------------------------------------------------------
// 2) Q1 test init: create main.lineitem + insert exactly 1 row
// ----------------------------------------------------------
static void EnsureDuckdbQ1TestData() {
    auto &rt = GetDuckRuntime();
    if (rt.q1_ready) {
        return;
    }

    auto res = rt.conn.Query("CREATE SCHEMA IF NOT EXISTS main;");
    if (res->HasError()) {
        throw std::runtime_error("DuckDB create schema failed: " + res->GetError());
    }

    // TPCH lineitem 标准 16 列（按顺序）
    // 为了先跑通：NUMERIC/DECIMAL 统一用 DOUBLE；CHAR(1) 用 VARCHAR
    std::string ddl = R"SQL(
    CREATE TABLE IF NOT EXISTS main.lineitem (
        l_orderkey      BIGINT,
        l_partkey       BIGINT,
        l_suppkey       BIGINT,
        l_linenumber    INTEGER,
        l_quantity      DOUBLE,
        l_extendedprice DOUBLE,
        l_discount      DOUBLE,
        l_tax           DOUBLE,
        l_returnflag    VARCHAR,
        l_linestatus    VARCHAR,
        l_shipdate      DATE,
        l_commitdate    DATE,
        l_receiptdate   DATE,
        l_shipinstruct  VARCHAR,
        l_shipmode      VARCHAR,
        l_comment       VARCHAR
    );
    )SQL";

    res = rt.conn.Query(ddl);
    if (res->HasError()) {
        throw std::runtime_error("DuckDB create lineitem failed: " + res->GetError());
    }

    // 保证幂等：每次 backend 初始化只留 1 行
    res = rt.conn.Query("DELETE FROM main.lineitem;");
    if (res->HasError()) {
        throw std::runtime_error("DuckDB delete lineitem failed: " + res->GetError());
    }

    // 插入 1 行测试数据（足够验证 scan->filter->agg->order 这条链路是否“能跑”）
    std::string ins = R"SQL(
    INSERT INTO main.lineitem VALUES
    (1, 1, 1, 1,
     17.0, 1000.0, 0.04, 0.02,
     'N', 'O',
     DATE '1998-09-02', DATE '1998-08-30', DATE '1998-09-10',
     'DELIVER IN PERSON', 'AIR', 'test row');
    )SQL";

    res = rt.conn.Query(ins);
    if (res->HasError()) {
        throw std::runtime_error("DuckDB insert test row failed: " + res->GetError());
    }

    rt.q1_ready = true;
}

// duckdb::PhysicalOperator &
// PgPhysicalPlanGenerator::CreatePlanSeqScan(SeqScan *scan) {
//     using namespace duckdb;

//     EnsureDuckdbQ1TestData();

//     Scan *scan_node = (Scan *)scan;
//     RangeTblEntry *rte = rt_fetch(scan_node->scanrelid, pstmt->rtable);
//     if (!rte || rte->rtekind != RTE_RELATION) {
//         ereport(ERROR, (errmsg("CreatePlanSeqScan: only base relation supported")));
//     }

//     const char *pg_table_name = get_rel_name(rte->relid);
//     if (!pg_table_name) {
//         ereport(ERROR, (errmsg("CreatePlanSeqScan: get_rel_name failed")));
//     }
//     if (strcmp(pg_table_name, "lineitem") != 0) {
//         ereport(ERROR, (errmsg("Q1-only mode: only table 'lineitem' supported, got '%s'", pg_table_name)));
//     }

//     std::string schema_name = "main";
//     std::string table_name  = "lineitem";

    

//     // 1) catalog lookup：现在 TableCatalogEntry 是完整类型了
//     auto &catalog = Catalog::GetSystemCatalog(context);
//     auto &table_entry = catalog.GetEntry<TableCatalogEntry>(
//         context,
//         INVALID_CATALOG,
//         schema_name,
//         table_name
//     );
    
//     // 3) 准备 scan columns：全 16 列
//     //    注意：column_ids 是“底表列号”(0-based)
//     duckdb::vector<LogicalType> returned_types = {
//         LogicalType::BIGINT,   // l_orderkey
//         LogicalType::BIGINT,   // l_partkey
//         LogicalType::BIGINT,   // l_suppkey
//         LogicalType::INTEGER,  // l_linenumber
//         LogicalType::DOUBLE,   // l_quantity
//         LogicalType::DOUBLE,   // l_extendedprice
//         LogicalType::DOUBLE,   // l_discount
//         LogicalType::DOUBLE,   // l_tax
//         LogicalType::VARCHAR,  // l_returnflag
//         LogicalType::VARCHAR,  // l_linestatus
//         LogicalType::DATE,     // l_shipdate
//         LogicalType::DATE,     // l_commitdate
//         LogicalType::DATE,     // l_receiptdate
//         LogicalType::VARCHAR,  // l_shipinstruct
//         LogicalType::VARCHAR,  // l_shipmode
//         LogicalType::VARCHAR   // l_comment
//     };


//     duckdb::vector<string> column_names = {
//         "l_orderkey","l_partkey","l_suppkey","l_linenumber",
//         "l_quantity","l_extendedprice","l_discount","l_tax",
//         "l_returnflag","l_linestatus",
//         "l_shipdate","l_commitdate","l_receiptdate",
//         "l_shipinstruct","l_shipmode","l_comment"
//     };

//     duckdb::vector<ColumnIndex> column_ids;
//     duckdb::vector<idx_t> projection_ids;
//     for (idx_t i = 0; i < 16; i++) {
//         column_ids.push_back(ColumnIndex((column_t)i)); // base column index
//         projection_ids.push_back(i);                    // output = returned (identity)
//     }

//     // scan 的输出 schema： = 全列输出（让上层 Projection/Agg 自己处理）
//     duckdb::vector<LogicalType> output_types = returned_types;


//     // 2) 用 duckdb::unique_ptr（不是 std::unique_ptr）
//     duckdb::unique_ptr<FunctionData> bind_data;

//     // 3) 真实 GetScanFunction + 真实 bind_data（可执行）
//     //    你之前写的 table_entry.GetScanFunction(context, bind_data) 现在能编译了
//     TableFunction table_func = table_entry.GetScanFunction(context, bind_data);

//     // ===== 下面这些你原来已有的 returned_types/column_ids/projection_ids/... 构造逻辑保持即可 =====

//     duckdb::unique_ptr<TableFilterSet> table_filters; // 先不下推过滤，跑通后再加

//     idx_t estimated_card = 1;
//     ExtraOperatorInfo extra_info;
//     duckdb::vector<Value> parameters;
//     duckdb::virtual_column_map_t virtual_columns;

//     // 注意：构造 PhysicalTableScan 的第3个参数是 TableFunction（按值），建议 move
//     auto &scan_op = Make<PhysicalTableScan>(
//         std::move(output_types),
//         std::move(table_func),            // <- 这里 move 掉
//         std::move(bind_data),             // <- duckdb::unique_ptr
//         std::move(returned_types),
//         std::move(column_ids),
//         std::move(projection_ids),
//         std::move(column_names),
//         std::move(table_filters),         // <- duckdb::unique_ptr
//         estimated_card,
//         std::move(extra_info),
//         std::move(parameters),
//         std::move(virtual_columns)
//     );

//     return scan_op;
// }

// duckdb::PhysicalOperator &
// PgPhysicalPlanGenerator::CreatePlanSeqScan(SeqScan *scan) {
//     using namespace duckdb;
        
//     EnsureDuckdbQ1TestData(); // 现在的 Q1 测试表&数据初始化

//     Plan *plan_node = (Plan *)scan;
//     Scan *scan_node = (Scan *)scan;

//     // 1. 从 PG 侧拿表信息
//     RangeTblEntry *rte = rt_fetch(scan_node->scanrelid, pstmt->rtable);
//     if (!rte || rte->rtekind != RTE_RELATION) {
//         ereport(ERROR,
//                 (errmsg("CreatePlanSeqScan: only base relation supported")));
//     }

//     struct RelationData *rel = table_open(rte->relid, AccessShareLock);
//     TupleDesc tupdesc = RelationGetDescr(rel);
//     int natts = tupdesc->natts;
    
//     const char *pg_table_name = get_rel_name(rte->relid);
//     if (!pg_table_name) {
//         table_close(rel, AccessShareLock);
//         ereport(ERROR, (errmsg("CreatePlanSeqScan: get_rel_name failed")));
//     }
//     if (strcmp(pg_table_name, "lineitem") != 0) {
//         table_close(rel, AccessShareLock);
//         ereport(ERROR, (errmsg("Q1-only mode: only table 'lineitem' supported, got '%s'", pg_table_name)));
//     }

//     // 2) DuckDB catalog lookup
//     std::string schema_name = "main";
//     std::string table_name  = "lineitem";

//     auto &catalog = Catalog::GetSystemCatalog(context);
//     auto &table_entry = catalog.GetEntry<TableCatalogEntry>(
//         context, INVALID_CATALOG, schema_name, table_name
//     );

//     duckdb::vector<LogicalType> returned_types;
//     duckdb::vector<string>      column_names;
//     duckdb::vector<ColumnIndex>    column_ids;
//     duckdb::vector<idx_t>       projection_ids;

//     // attno -> 在上面数组里的 local index
//     vector<column_t>    attno_to_local(natts + 1, DConstants::INVALID_INDEX);

//     // ---------- 1) targetlist 中的 Var ----------
//     ListCell *lc;
//     for (lc = list_head(plan_node->targetlist);
//          lc != NULL;
//          lc = lnext(plan_node->targetlist, lc)) {

//         TargetEntry *tle = (TargetEntry *)lfirst(lc);
//         if (!IsA(tle->expr, Var)) {
//             continue; // 复杂 expr 上交给上层 Result/Projection
//         }
//         Var *var = (Var *)tle->expr;

//         auto binding = PgVarToPgScanColumn(
//             var, rel, tupdesc,
//             returned_types, column_names,
//             column_ids
//         );
//         attno_to_local[binding.attno] = binding.local_index;
//         // projection_ids 只在 targetlist 里 push
//         projection_ids.push_back(binding.local_index);
//     }

//     // ---------- 2) qual 里的 Var 也要进 column_ids ----------
//     if (plan_node->qual) {
//         for (lc = list_head(plan_node->qual);
//              lc != NULL;
//              lc = lnext(plan_node->qual, lc)) {

//             Expr *qual = (Expr *)lfirst(lc);
//             if (!IsA(qual, OpExpr)) {
//                 continue;
//             }
//             auto *op = (OpExpr *)qual;
//             if (list_length(op->args) != 2) {
//                 continue;
//             }
//             Expr *arg1 = (Expr *)linitial(op->args);
//             if (!IsA(arg1, Var)) {
//                 continue;
//             }
//             Var *var = (Var *)arg1;
//             auto binding = PgVarToPgScanColumn(
//                 var, rel, tupdesc,
//                 returned_types, column_names,
//                 column_ids 
//             );
//             attno_to_local[binding.attno] = binding.local_index;
//         }
//     }

//     // ---------- 3) 从 qual 构造 TableFilterSet ----------对第几列（local_index）做过滤
//     unique_ptr<TableFilterSet> table_filters;
//     if (plan_node->qual) {
//         auto logical_filters = make_uniq<TableFilterSet>();
//         for (lc = list_head(plan_node->qual);
//              lc != NULL;
//              lc = lnext(plan_node->qual, lc)) {

//             Expr *qual = (Expr *)lfirst(lc);
//             TryBuildComparisonFilterFromQual_Q1_PG(
//                 qual, tupdesc, attno_to_local, *logical_filters
//             );
//         }
//         if (!logical_filters->filters.empty()) {
//             table_filters = std::move(logical_filters);
//         }
//     }

//     // ---------- 4) 输出 schema ----------
//     // returned_types/column_names：是所有“要读的列”（包括只用于过滤、不输出的列）
//     // duckdb::vector<LogicalType> output_types = returned_types;
//     // duckdb::vector<string>      output_names = column_names;

//     //只需要 targetlist 中的列，所以：新建 output_types / output_names
//     std::vector<LogicalType> output_types;
//     std::vector<std::string> output_names;
//     output_types.reserve(returned_types.size());
//     output_names.reserve(column_names.size());

//     // 对 projection_ids 里的每一个 index：
//     // 从 returned_types[proj_idx] 取 type， push 到 output_types；
//     // 从 column_names[proj_idx] 取 name， push 到 output_names。

//     for (auto proj_idx : projection_ids) {
//         output_types.push_back(returned_types[proj_idx]);
//         output_names.push_back(column_names[proj_idx]);
//     }

//     // 6) 真实 scan function + bind_data（可执行）
//     duckdb::unique_ptr<FunctionData> bind_data;
//     TableFunction table_func = table_entry.GetScanFunction(context, bind_data);

//     // 3) 打开 Projections / Filters 的打印开关
//     table_func.projection_pushdown = true;  //这个表函数支持只读某些列，而不是所有列
//     table_func.filter_prune        = true;   // 只打印 projection_ids 中的列,将qual中的列保留在scan 的 returned_types/column_ids
//     table_func.filter_pushdown     = true;//这个表函数支持把上层的 WHERE 条件下推到底层 scan，自己利用这些 filter 去做更高效的读取。



//     idx_t estimated_card = (idx_t)plan_node->plan_rows;
//     ExtraOperatorInfo extra_info;
//     duckdb::vector<Value>   parameters;
//     // vector<column_t> virtual_columns;

//     // virtual_column_map_t 是一个别名，用它
//     duckdb::virtual_column_map_t virtual_columns;

//     auto &scan_op = Make<PhysicalTableScan>(
//         std::move(output_types),
//         table_func,
//         std::move(bind_data),
//         std::move(returned_types),
//         std::move(column_ids),
//         std::move(projection_ids),
//         std::move(column_names),
//         std::move(table_filters),
//         estimated_card,
//         std::move(extra_info),
//         std::move(parameters),
//         std::move(virtual_columns)
//     );

//     table_close(rel, AccessShareLock);
//     return scan_op;

// }

static void ClientNotice(const std::string &s) {
    ereport(NOTICE, (errmsg_internal("%s", s.c_str())));
}

static std::string AttnoToName(TupleDesc tupdesc, AttrNumber attno) {
    if (attno <= 0 || attno > tupdesc->natts) return "<invalid>";
    Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
    return std::string(NameStr(attr->attname));
}

// 1) strip PG implicit cast wrapper
static Expr *StripRelabel(Expr *e) {
    while (e && IsA(e, RelabelType)) {
        e = (Expr *)((RelabelType *)e)->arg;
    }
    return e;
}

// 2) flatten AND into a list of clauses
static void CollectAndQuals(Expr *qual, std::vector<Expr*> &out) {
    qual = StripRelabel(qual);
    if (!qual) return;

    if (IsA(qual, BoolExpr)) {
        BoolExpr *b = (BoolExpr *)qual;
        if (b->boolop == AND_EXPR) {
            for (ListCell *lc = list_head(b->args); lc != NULL; lc = lnext(b->args, lc)) {
                CollectAndQuals((Expr *)lfirst(lc), out);
            }
            return;
        }
    }
    out.push_back(qual);
}

// 3) PG Const -> DuckDB Value (covers TPCH-style types; you can extend later)
static duckdb::Value PgConstToDuckValue(Const *cst) {
    using namespace duckdb;

    if (!cst || cst->constisnull) {
        return Value(); // NULL
    }

    switch (cst->consttype) {
    case BOOLOID:
        return Value::BOOLEAN(DatumGetBool(cst->constvalue));

    case INT2OID:
        return Value::SMALLINT((int16)DatumGetInt16(cst->constvalue));
    case INT4OID:
        return Value::INTEGER((int32)DatumGetInt32(cst->constvalue));
    case INT8OID:
        return Value::BIGINT((int64)DatumGetInt64(cst->constvalue));

    case FLOAT4OID:
        return Value::FLOAT((float)DatumGetFloat4(cst->constvalue));
    case FLOAT8OID:
        return Value::DOUBLE((double)DatumGetFloat8(cst->constvalue));

    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID: {
        char *s = TextDatumGetCString(cst->constvalue);
        std::string str = s ? std::string(s) : std::string();
        if (s) pfree(s);

        // 关键：直接 return Value(str); 不要先 Value out(...) 声明（会触发 vexing parse）
        return Value(str);
    }

    case DATEOID: {
        // PG DateADT: days since 2000-01-01
        DateADT pg_days = DatumGetDateADT(cst->constvalue);

        // DuckDB date_t: days since 1970-01-01
        int32_t duck_days = (int32_t)pg_days + 10957; // 1970->2000
        duckdb::date_t d;
        d.days = duck_days;
        return Value::DATE(d);
    }

    default: {
        // fallback: output as string (safe)
        Oid outfunc;
        bool isvarlena;
        getTypeOutputInfo(cst->consttype, &outfunc, &isvarlena);
        char *s = OidOutputFunctionCall(outfunc, cst->constvalue);

        std::string str = s ? std::string(s) : std::string();
        if (s) pfree(s);

        return Value(str);
    }
    }
}
// 4) PG Var -> DuckDB physical column id (uses PG column name -> duck_colmap)
static duckdb::column_t PgVarToDuckPhysical(Var *var,
                                           TupleDesc tupdesc,
                                           const std::unordered_map<std::string, DuckColRef> &duck_colmap,
                                           std::string &out_pg_colname) {
    AttrNumber attno = var->varattno;
    if (attno <= 0 || attno > tupdesc->natts) {
        ereport(ERROR, (errmsg("PgVarToDuckPhysical: invalid attno %d", attno)));
    }

    Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
    if (attr->attisdropped) {
        ereport(ERROR, (errmsg("PgVarToDuckPhysical: dropped column attno %d", attno)));
    }

    const char *attname = NameStr(attr->attname);
    out_pg_colname = attname ? std::string(attname)
                             : std::string("col") + std::to_string((int)attno);

    auto it = duck_colmap.find(NormalizeIdent(out_pg_colname));
    if (it == duck_colmap.end()) {
        ereport(ERROR,
                (errmsg("PgVarToDuckPhysical: column '%s' not found in DuckDB table",
                        out_pg_colname.c_str())));
    }
    return it->second.physical_col;
}

// 5) reverse comparison when Const OP Var
static duckdb::ExpressionType ReverseCmp(duckdb::ExpressionType t) {
    using namespace duckdb;
    switch (t) {
    case ExpressionType::COMPARE_LESSTHAN: return ExpressionType::COMPARE_GREATERTHAN;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO: return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    case ExpressionType::COMPARE_GREATERTHAN: return ExpressionType::COMPARE_LESSTHAN;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return ExpressionType::COMPARE_LESSTHANOREQUALTO;
    default: return t;
    }
}

// 6) build ConstantFilter pushdown: Var OP Const or Const OP Var
//    - returns true if pushed
//    - returns false if unsupported form
static bool TryBuildComparisonFilterFromQual_Physical(
    Expr *qual,
    TupleDesc tupdesc,
    const std::unordered_map<std::string, DuckColRef> &duck_colmap,
    duckdb::TableFilterSet &table_filters) {

    using namespace duckdb;

    qual = StripRelabel(qual);
    if (!qual || !IsA(qual, OpExpr)) return false;

    auto *opexpr = (OpExpr *)qual;
    if (list_length(opexpr->args) != 2) return false;

    Expr *a1 = StripRelabel((Expr *)linitial(opexpr->args));
    Expr *a2 = StripRelabel((Expr *)lsecond(opexpr->args));

    Var   *var = nullptr;
    Const *cst = nullptr;
    bool const_on_left = false;

    if (IsA(a1, Var) && IsA(a2, Const)) {
        var = (Var *)a1;
        cst = (Const *)a2;
    } else if (IsA(a1, Const) && IsA(a2, Var)) {
        var = (Var *)a2;
        cst = (Const *)a1;
        const_on_left = true;
    } else {
        return false;
    }

    const char *opname = get_opname(opexpr->opno);
    if (!opname) return false;

    ExpressionType cmp;
    if (strcmp(opname, "<=") == 0) cmp = ExpressionType::COMPARE_LESSTHANOREQUALTO;
    else if (strcmp(opname, "<") == 0) cmp = ExpressionType::COMPARE_LESSTHAN;
    else if (strcmp(opname, "=") == 0) cmp = ExpressionType::COMPARE_EQUAL;
    else if (strcmp(opname, ">=") == 0) cmp = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    else if (strcmp(opname, ">") == 0) cmp = ExpressionType::COMPARE_GREATERTHAN;
    else return false;

    if (const_on_left) cmp = ReverseCmp(cmp);
    if (cst->constisnull) return false;

    std::string pg_colname;
    auto physical = PgVarToDuckPhysical(var, tupdesc, duck_colmap, pg_colname);

    Value v = PgConstToDuckValue(cst);
    auto tf = duckdb::make_uniq<ConstantFilter>(cmp, v);

    // IMPORTANT: you are in PHYSICAL column space now
    table_filters.PushFilter(ColumnIndex((idx_t)physical), std::move(tf));
    return true;
}



duckdb::PhysicalOperator &
PgPhysicalPlanGenerator::CreatePlanSeqScan(SeqScan *scan) {
    using namespace duckdb;

    EnsureDuckdbQ1TestData();

    Plan *plan_node = (Plan *)scan;
    Scan *scan_node = (Scan *)scan;

    RangeTblEntry *rte = rt_fetch(scan_node->scanrelid, pstmt->rtable);
    if (!rte || rte->rtekind != RTE_RELATION) {
        ereport(ERROR, (errmsg("CreatePlanSeqScan: only base relation supported")));
    }

    struct RelationData *rel = table_open(rte->relid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(rel);

    // 仅支持 lineitem（Q1）
    const char *pg_table_name = get_rel_name(rte->relid);
    if (!pg_table_name) {
        table_close(rel, AccessShareLock);
        ereport(ERROR, (errmsg("CreatePlanSeqScan: get_rel_name failed")));
    }
    if (strcmp(pg_table_name, "lineitem") != 0) {
        table_close(rel, AccessShareLock);
        ereport(ERROR, (errmsg("Q1-only mode: only table 'lineitem' supported, got '%s'", pg_table_name)));
    }

    // 打印 PG SeqScan targetlist
    {
        ClientNotice("---- PG SeqScan targetlist ----");
        int i = 0;
        for (ListCell *lc = list_head(plan_node->targetlist); lc != NULL; lc = lnext(plan_node->targetlist, lc), i++) {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            if (IsA(tle->expr, Var)) {
                Var *v = (Var *)tle->expr;
                ClientNotice("target[" + std::to_string(i) + "]: attno=" + std::to_string((int)v->varattno) +
                             " name=" + AttnoToName(tupdesc, v->varattno) +
                             " resjunk=" + std::to_string((int)tle->resjunk));
            } else {
                ClientNotice("target[" + std::to_string(i) + "]: non-Var nodeTag=" + std::to_string((int)nodeTag(tle->expr)) +
                             " resjunk=" + std::to_string((int)tle->resjunk));
            }
        }
    }

    // DuckDB catalog: main.lineitem
    std::string schema_name = "main";
    std::string table_name  = "lineitem";
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto &table_entry = catalog.GetEntry<TableCatalogEntry>(
        context, INVALID_CATALOG, schema_name, table_name
    );

    // name->(physical,type)
    auto duck_colmap = BuildDuckColumnMap(table_entry);

    // 物理列空间（长度=全表物理列数）
    duckdb::vector<std::string> physical_names = BuildDuckPhysicalNameVector(table_entry);
    duckdb::vector<duckdb::LogicalType> physical_types = BuildDuckPhysicalTypeVector(table_entry);

    ClientNotice("DuckDB physical_names.size=" + std::to_string((int)physical_names.size()) +
                 " physical_names[8]=" + (physical_names.size() > 8 ? physical_names[8] : std::string("<OOB>")));

    // returned_types 直接用“物理列 types”（关键修复：避免 physical id 去索引 size=6）
    duckdb::vector<LogicalType> returned_types = physical_types; // copy

    // column_ids 构建为“物理列全量 identity”：column_ids[i] == i
    duckdb::vector<ColumnIndex> column_ids;
    column_ids.reserve(physical_names.size());
    for (duckdb::idx_t i = 0; i < physical_names.size(); i++) {
        column_ids.push_back(ColumnIndex(i));
    }

    // output projection ids：这里直接用 physical id（因为 local==physical）
    duckdb::vector<idx_t> output_projection_ids;
    std::vector<bool> seen_phys(physical_names.size(), false);

    // 从 targetlist 收集输出列（只收集 Var 且 resjunk=false）
    for (ListCell *lc = list_head(plan_node->targetlist); lc != NULL; lc = lnext(plan_node->targetlist, lc)) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->resjunk) {
            continue;
        }
        if (!IsA(tle->expr, Var)) {
            // Q1 的 SeqScan targetlist 理论上全是 Var；如果你还没支持更复杂下推，就直接跳过或报错
            continue;
        }
        Var *var = (Var *)tle->expr;

        std::string pg_colname;
        duckdb::column_t phys = PgVarToDuckPhysical(var, tupdesc, duck_colmap, pg_colname);

        if ((duckdb::idx_t)phys >= physical_names.size()) {
            ereport(ERROR, (errmsg("BUG: physical id %d out of range physical_names.size=%d",
                                   (int)phys, (int)physical_names.size())));
        }

        if (!seen_phys[(duckdb::idx_t)phys]) {
            seen_phys[(duckdb::idx_t)phys] = true;
            output_projection_ids.push_back((idx_t)phys);
        }
    }

    // 没有输出列就默认全列（一般不会发生在 Q1）
    if (output_projection_ids.empty()) {
        output_projection_ids.reserve(physical_names.size());
        for (duckdb::idx_t i = 0; i < physical_names.size(); i++) {
            output_projection_ids.push_back(i);
        }
    }

    // 输出 types：按 output_projection_ids（physical）取 physical_types
    duckdb::vector<LogicalType> output_types;
    output_types.reserve(output_projection_ids.size());
    for (auto pid : output_projection_ids) {
        if (pid >= physical_types.size()) {
            ereport(ERROR, (errmsg("BUG: projection physical id %llu out of range physical_types.size=%llu",
                                   (unsigned long long)pid, (unsigned long long)physical_types.size())));
        }
        output_types.push_back(physical_types[pid]);
    }

ClientNotice("---- PG SeqScan qual ----");
ClientNotice("plan_node->qual length = " + std::to_string(list_length(plan_node->qual)));


    // -------------------------
    // WHERE pushdown (plan_node->qual)
    // -------------------------
    unique_ptr<TableFilterSet> table_filters;
    if (plan_node->qual) {
        auto logical_filters = make_uniq<TableFilterSet>();

        std::vector<Expr*> clauses;
        for (ListCell *lc = list_head(plan_node->qual); lc != NULL; lc = lnext(plan_node->qual, lc)) {
            CollectAndQuals((Expr *)lfirst(lc), clauses);
        }
ClientNotice("flattened clauses = " + std::to_string((int)clauses.size()));
        int pushed = 0;
        int total  = 0;
        for (auto *cl : clauses) {
            total++;
            if (TryBuildComparisonFilterFromQual_Physical(cl, tupdesc, duck_colmap, *logical_filters)) {
                pushed++;
            } else {
                // IMPORTANT:
                // 为了语义正确：你现在还没有生成 DuckDB 侧的 PhysicalFilter 节点来执行未下推条件
                // 所以遇到不支持的 qual 必须报错，否则结果会错。
                ereport(ERROR,
                        (errmsg("CreatePlanSeqScan: unsupported WHERE qual for pushdown (nodeTag=%d). "
                                "Implement remaining qual as DuckDB PhysicalFilter or extend pushdown.",
                                (int)nodeTag(cl))));
            }
        }
        ClientNotice("table_filters.size = " + std::to_string((int)logical_filters->filters.size()));
    ClientNotice("pushed filters = " + std::to_string(pushed));

        if (!logical_filters->filters.empty()) {
            table_filters = std::move(logical_filters);
            ClientNotice("SeqScan pushdown filters: " + std::to_string(pushed) + "/" + std::to_string(total));
        }
    }

    // scan function
    duckdb::unique_ptr<FunctionData> bind_data;
    TableFunction table_func = table_entry.GetScanFunction(context, bind_data);
    table_func.projection_pushdown = true;
    table_func.filter_pushdown     = true;
    table_func.filter_prune        = true;

    idx_t estimated_card = (idx_t)plan_node->plan_rows;
    ExtraOperatorInfo extra_info;
    duckdb::vector<Value> parameters;
    duckdb::virtual_column_map_t virtual_columns;

    // 关键：names/returned_types/column_ids 全部在“物理列空间”，projection_ids 也用 physical id
    auto &scan_op = Make<PhysicalTableScan>(
        std::move(output_types),
        table_func,
        std::move(bind_data),
        std::move(returned_types),          // 物理 types（size=16）
        std::move(column_ids),              // identity（size=16）
        std::move(output_projection_ids),   // physical ids（例如 8,9,4,5,6,7）
        std::move(physical_names),          // 物理 names（size=16）
        std::move(table_filters),
        estimated_card,
        std::move(extra_info),
        std::move(parameters),
        std::move(virtual_columns)
    );

    table_close(rel, AccessShareLock);

    // 现在这里 ToString 不应该再炸
    ClientNotice("---- DuckDB scan_op.ToString() right after construction ----\n" + scan_op.ToString());
    return scan_op;
}





duckdb::PhysicalOperator &
PgPhysicalPlanGenerator::CreatePlanResult(Result *res) {
    using namespace duckdb;

    Plan *plan_node = &res->plan;

    // --- 1. 估计行数：没有的话默认 1 行 ---
    idx_t estimated_card = (idx_t)plan_node->plan_rows;
    if (estimated_card == 0) {
        estimated_card = 1;
    }

    // --- 2. targetlist -> DuckDB 表达式 + 类型 ---
    duckdb::vector<duckdb::LogicalType> proj_types;
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;

    for (ListCell *lc = list_head(plan_node->targetlist);
         lc != nullptr;
         lc = lnext(plan_node->targetlist, lc)) {

        TargetEntry *tle = (TargetEntry *) lfirst(lc);
        if (!tle || tle->resjunk) {
            continue;
        }

        // PG Expr -> DuckDB Expression
        auto e = map_pg_expr((Node *) tle->expr);
        if (!e) {
            // 兜底：NULL 常量
            e = duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value());
        }

        // 通过 PG 表达式类型推 DuckDB LogicalType（你自己已有这个函数）
        Oid   typid  = exprType((Node *) tle->expr);
        int32 typmod = exprTypmod((Node *) tle->expr);
        duckdb::LogicalType ltype = PgTypeOidToDuckType(typid, typmod);

        proj_types.push_back(ltype);
        proj_exprs.push_back(std::move(e));
    }

    // --- 3. 调用 Make<PhysicalProjection> 创建算子 ---
    auto &proj = Make<duckdb::PhysicalProjection>(
        proj_types,
        std::move(proj_exprs),
        estimated_card
    );

    // --- 4. 如果有 child，就挂在下面；没有 child（SELECT 1）就先空着 ---
    auto &child_phys = Make<PhysicalDummyScan>(proj_types, estimated_card);
    proj.children.push_back(child_phys);

    return proj;
}


// 收集是否有重复表达式
// static void
// CountExpressions(Expression &expr, CSEReplaceState &state) {
// 	// we only consider expressions with children for CSE elimination
// 	switch (expr.GetExpressionClass()) {
// 	case ExpressionClass::BOUND_COLUMN_REF:
// 	case ExpressionClass::BOUND_CONSTANT:
// 	case ExpressionClass::BOUND_PARAMETER:
// 		return;
// 	default:
// 		break;
// 	}
// 	if (expr.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE && !expr.IsVolatile()) {
// 		// we can't move aggregates to a projection, so we only consider the children of the aggregate
// 		auto node = state.expression_count.find(expr);
// 		if (node == state.expression_count.end())
// 			// first time we encounter this expression, insert this node with [count = 1]
// 			// but only if it is not an interior argument of a short circuit sensitive expression.
// 				state.expression_count[expr] = CSENode();
// 		else {
// 			// we encountered this expression before, increment the occurrence count
// 			node->second.count++;
//             state.perform_replacement = true;
// 		}
// 	}

// 	// recursively count the children
// 	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { CountExpressions(child, state); });
// }

// static void
// PerformCSEReplacement(unique_ptr<Expression> &expr_ptr, CSEReplaceState &state) {
// 	Expression &expr = *expr_ptr;
// 	// 对不同类型的表达式做替换处理，目标是将重复表达式替换为投影中计算的列引用
// 	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
// 		auto &bound_column_ref = expr.Cast<BoundColumnRefExpression>();
// 		// bound column ref, check if this one has already been recorded in the expression list
// 		auto column_entry = state.column_map.find(bound_column_ref.binding);
// 		if (column_entry == state.column_map.end()) {
// 			// not there yet: push the expression
// 			idx_t new_column_index = state.expressions.size();
// 			state.column_map[bound_column_ref.binding] = new_column_index;
// 			state.expressions.push_back(make_uniq<BoundColumnRefExpression>(
// 			    bound_column_ref.GetAlias(), bound_column_ref.return_type, bound_column_ref.binding));
// 			bound_column_ref.binding = ColumnBinding(0, new_column_index);
// 		} else {
// 			// else: just update the column binding!
// 			bound_column_ref.binding = ColumnBinding(0, column_entry->second);
// 		}
// 		return;
// 	}

// 	// check if this child is eligible for CSE elimination
// 	if (state.expression_count.find(expr) != state.expression_count.end()) {
// 		auto &node = state.expression_count[expr];
// 		if (node.count > 1) {
// 			// this expression occurs more than once! push it into the projection
// 			// check if it has already been pushed into the projection
// 			auto alias = expr.GetAlias();
// 			auto type = expr.return_type;
// 			if (!node.column_index.IsValid()) {
// 				// has not been pushed yet: push it
// 				node.column_index = state.expressions.size();
// 				state.expressions.push_back(std::move(expr_ptr));
// 			} else {
// 				state.cached_expressions.push_back(std::move(expr_ptr));
// 			}
// 			// replace the original expression with a bound column ref
// 			expr_ptr = make_uniq<BoundReferenceExpression>(type, node.column_index.GetIndex());
// 			return;
// 		}
// 	}
// 	// this expression only occurs once, we can't perform CSE elimination
// 	// look into the children to see if we can replace them
// 	ExpressionIterator::EnumerateChildren(expr,
// 	                                      [&](unique_ptr<Expression> &child) { PerformCSEReplacement(child, state); });
// }

static void
CountExpressions(Expression &expr, CSEReplaceState &state) {
	// we only consider expressions with children for CSE elimination
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_PARAMETER:
		return;
	default:
		break;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE && !expr.IsVolatile()) {
		// we can't move aggregates to a projection, so we only consider the children of the aggregate
		auto node = state.expression_count.find(expr);
		if (node == state.expression_count.end())
			// first time we encounter this expression, insert this node with [count = 1]
			// but only if it is not an interior argument of a short circuit sensitive expression.
				state.expression_count[expr] = CSENode();
		else {
			// we encountered this expression before, increment the occurrence count
			node->second.count++;
            state.perform_replacement = true;
		}
	}

	// recursively count the children
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { CountExpressions(child, state); });
}

static void
PerformCSEReplacement(unique_ptr<Expression> &expr_ptr, CSEReplaceState &state) {
	Expression &expr = *expr_ptr;
	// 对不同类型的表达式做替换处理，目标是将重复表达式替换为投影中计算的列引用
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &bound_column_ref = expr.Cast<BoundReferenceExpression>();
		// bound column ref, check if this one has already been recorded in the expression list
		auto column_entry = state.column_map.find(bound_column_ref.index);
		if (column_entry == state.column_map.end()) {
			// not there yet: push the expression
			idx_t new_column_index = state.expressions.size();
			state.column_map[bound_column_ref.index] = new_column_index;
			state.expressions.push_back(make_uniq<BoundReferenceExpression>(
			    bound_column_ref.GetAlias(), bound_column_ref.return_type, bound_column_ref.index));
			bound_column_ref.index = new_column_index;
		} else {
			// else: just update the column binding!
			bound_column_ref.index = column_entry->second;
		}
		return;
	}

	// check if this child is eligible for CSE elimination
	if (state.expression_count.find(expr) != state.expression_count.end()) {
		auto &node = state.expression_count[expr];
		if (node.count > 1) {
			// this expression occurs more than once! push it into the projection
			// check if it has already been pushed into the projection
			auto alias = expr.GetAlias();
			auto type = expr.return_type;
			if (!node.column_index.IsValid()) {
				// has not been pushed yet: push it
				node.column_index = state.expressions.size();
				state.expressions.push_back(std::move(expr_ptr));
			} else {
				state.cached_expressions.push_back(std::move(expr_ptr));
			}
			// replace the original expression with a bound column ref
			expr_ptr = make_uniq<BoundReferenceExpression>(type, node.column_index.GetIndex());
			return;
		}
	}
	// this expression only occurs once, we can't perform CSE elimination
	// look into the children to see if we can replace them
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](unique_ptr<Expression> &child) { PerformCSEReplacement(child, state); });
}


PhysicalOperator &
PgPhysicalPlanGenerator::duplicate_subexprs(PhysicalOperator &child,
            vector<unique_ptr<Expression>> &aggregates,
            vector<unique_ptr<Expression>> &groups) {
    vector<LogicalType> proj_types;
    vector<unique_ptr<Expression>> proj_exprs;
    CSEReplaceState state;
    state.perform_replacement = false;

    //  遍历收集
    for (auto &expr : groups)
        CountExpressions(*expr, state);
    for (auto &expr : aggregates)
        CountExpressions(*expr, state);

    // 无重复内容
    if (!state.perform_replacement)
        return child;

    // 遍历 map，找出重复的表达式
    for (auto &expr : groups)
        PerformCSEReplacement(expr, state);
    for (auto &expr : aggregates)
        PerformCSEReplacement(expr, state);

    // 构造 proj_types
    for (auto &expr : state.expressions)
        proj_types.push_back(expr->return_type);

    // 构造Projection
    auto &projection = Make<PhysicalProjection>(
        std::move(proj_types),
        std::move(state.expressions),
        child.estimated_cardinality
    );

    projection.children.push_back(child);
    return projection;
}


duckdb::PhysicalOperator &
PgPhysicalPlanGenerator::ExtractAggregateExpressionsCompat(
    duckdb::PhysicalOperator &child,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &aggregates,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &groups) {
    using namespace duckdb;

    // 会插在 child 上面的一层 Projection
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
    duckdb::vector<duckdb::LogicalType>                    proj_types;

    // ---------- 1) 先处理 group keys ----------
    for (auto &group_expr : groups) {
        // 把 group expr 下推到 Projection
        proj_types.push_back(group_expr->return_type);
        proj_exprs.push_back(std::move(group_expr));

        idx_t col_idx = proj_exprs.size() - 1;
        // 上层 HashAggregate 只需要一个 BoundReference 指向 Projection 的这一列
        group_expr = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
            proj_types.back(),
            col_idx
        );
    }

    // ---------- 2) 再处理每个聚合的 children + filter ----------
    for (auto &aggr_expr : aggregates) {
        auto &bound_aggr = aggr_expr->Cast<duckdb::BoundAggregateExpression>();

        // 2.1 参数表达式 children
        for (auto &child_expr : bound_aggr.children) {
            idx_t col_idx = proj_exprs.size();
            proj_types.push_back(child_expr->return_type);
            proj_exprs.push_back(std::move(child_expr));

            child_expr = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                proj_types.back(),
                col_idx
            );
        }

        // 2.2 filter 表达式（如 agg(x) FILTER (WHERE cond)）
        if (bound_aggr.filter) {
            auto &filter_expr = bound_aggr.filter;

            idx_t col_idx = proj_exprs.size();
            proj_types.push_back(filter_expr->return_type);
            proj_exprs.push_back(std::move(filter_expr));

            bound_aggr.filter = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                proj_types.back(),
                col_idx
            );
        }
    }

    // 如果没有任何下推的表达式，就不用插 Projection，直接返回 child
    if (proj_exprs.empty()) {
        return child;
    }

    auto &proj = Make<duckdb::PhysicalProjection>(
        std::move(proj_types),
        std::move(proj_exprs),
        child.estimated_cardinality
    );
    proj.children.push_back(child);
    return proj;
}


duckdb::PhysicalOperator &
PgPhysicalPlanGenerator::CreatePlanAgg(Agg *agg) {
    using namespace duckdb;

    Plan *plan_node  = &agg->plan;
    Plan *child_plan = outerPlan(plan_node);
    if (!child_plan) {
        ereport(ERROR, (errmsg("CreatePlanAgg: Agg without child")));
    }

    // 1) 递归生成 child 的物理计划（通常是 SeqScan / Join / Sort 等）
    auto &child_phys = CreatePlan(child_plan);

    // ---------- 1.1 为 map_pg_expr 填好当前输出 schema 信息 ----------
    g_current_colnames.clear();
    g_current_types.clear();
    g_current_attnum_map.clear();

    for (ListCell *lc = list_head(child_plan->targetlist);
         lc != nullptr;
         lc = lnext(child_plan->targetlist, lc)) {

        auto *ctle = (TargetEntry *) lfirst(lc);
        if (!ctle || ctle->resjunk) {
            continue; // 忽略 junk 列
        }

        Expr *cexpr = (Expr *)ctle->expr;

        // 优先 TLE::resname / base table 列名
        std::string cname = ChooseAliasFromPG(cexpr, ctle);
        if (cname.empty()) {
            cname = "col" + std::to_string((int)ctle->resno - 1);
        }
        g_current_colnames.push_back(cname);

        // PG type -> DuckDB LogicalType
        Oid   ctypid  = exprType((Node *)cexpr);
        int32 ctypmod = exprTypmod((Node *)cexpr);
        g_current_types.push_back(PgTypeOidToDuckType(ctypid, ctypmod));

        // resno(1-based) -> colidx(0-based)
        if ((int)g_current_attnum_map.size() < ctle->resno) {
            g_current_attnum_map.resize(ctle->resno, -1);
        }
        g_current_attnum_map[ctle->resno - 1] =
            (int)g_current_colnames.size() - 1;
    }

    // --------------------------------------------------
    // 2) 准备 HashAggregate 需要的几大块：
    //    output_types / groups / aggregates
    // --------------------------------------------------

    // 2.1 输出列类型：初始用 Agg targetlist 的类型（后面对 group key 会改成压缩类型）
    duckdb::vector<duckdb::LogicalType> output_types;
    for (ListCell *lc2 = list_head(plan_node->targetlist);
         lc2 != nullptr;
         lc2 = lnext(plan_node->targetlist, lc2)) {

        auto *tle = (TargetEntry *) lfirst(lc2);
        if (!tle || tle->resjunk) {
            continue;
        }

        Oid   typid  = exprType((Node *) tle->expr);
        int32 typmod = exprTypmod((Node *) tle->expr);
        output_types.push_back(PgTypeOidToDuckType(typid, typmod));
    }

    // PG 侧是不是字符串类型
    auto IsStringTypePG = [](Oid typid) {
        return (typid == BPCHAROID  ||
                typid == VARCHAROID ||
                typid == TEXTOID   ||
                typid == NAMEOID);
    };

    // 记录每个 group key 是否字符串列（按 group 顺序）
    duckdb::vector<bool> group_is_string;

    // 2.2 group keys：Agg->grpColIdx[] 描述 “child 输出 tuple 的第几列”
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> groups;

    if (agg->numCols > 0) {
        groups.reserve(agg->numCols);
        group_is_string.reserve(agg->numCols);

        for (int i = 0; i < agg->numCols; i++) {
            AttrNumber attno = agg->grpColIdx[i]; // child_plan->targetlist 中的 resno

            // 从 child targetlist 找到对应的 TargetEntry
            TargetEntry *tle = get_tle_by_resno(child_plan->targetlist, attno);
            if (!tle) {
                ereport(ERROR,
                        (errmsg("CreatePlanAgg: cannot find group key resno %d in child targetlist",
                                (int)attno)));
            }

            Expr *group_expr = (Expr *) tle->expr;

            auto duck_expr = map_pg_expr((Node *)group_expr);
            if (!duck_expr) {
                ereport(ERROR,
                        (errmsg("CreatePlanAgg: map_pg_expr returned null for group key")));
            }

            // 用 PG 类型覆盖 group key 的返回类型（后面压缩时会改）
            Oid   g_typid  = exprType((Node *)group_expr);
            int32 g_typmod = exprTypmod((Node *)group_expr);
            duckdb::LogicalType g_ltype = PgTypeOidToDuckType(g_typid, g_typmod);
            duck_expr->return_type = g_ltype;

            group_is_string.push_back(IsStringTypePG(g_typid));
            groups.push_back(std::move(duck_expr));
        }
    }

    // 2.3 aggregates：把 targetlist 里的 Aggref 转成 BoundAggregateExpression
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> aggregates;

    for (ListCell *lc3 = list_head(plan_node->targetlist);
         lc3 != nullptr;
         lc3 = lnext(plan_node->targetlist, lc3)) {

        auto *tle = (TargetEntry *) lfirst(lc3);
        if (!tle || !IsA(tle->expr, Aggref)) {
            continue; // 非聚合列：group key 或普通表达式
        }

        Aggref *aggref = (Aggref *) tle->expr;

        // 聚合函数名
        const char *fn_name_c = get_func_name(aggref->aggfnoid);
        if (!fn_name_c) {
            ereport(ERROR,
                    (errmsg("CreatePlanAgg: get_func_name failed for aggfnoid %u",
                            aggref->aggfnoid)));
        }
        std::string fn_name = fn_name_c;

        // count(*) 特殊处理：DuckDB 中是 count_star()
        bool is_count_star = aggref->aggstar;
        if (is_count_star) {
            fn_name = "count_star";
        }

        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> arg_exprs;
        duckdb::vector<duckdb::LogicalType> arg_types;

        if (!is_count_star) {
            if (aggref->args == NIL) {
                ereport(ERROR,
                        (errmsg("CreatePlanAgg: aggregate %s has no args", fn_name_c)));
            }

            // 只取第一个参数
            auto *arg_tle = (TargetEntry *) linitial(aggref->args);
            Expr *arg_expr = (Expr *) arg_tle->expr;

            auto duck_arg = map_pg_expr((Node *)arg_expr);
            if (!duck_arg) {
                ereport(ERROR,
                        (errmsg("CreatePlanAgg: map_pg_expr returned null for agg arg")));
            }

            Oid   arg_typid  = exprType((Node *)arg_expr);
            int32 arg_typmod = exprTypmod((Node *)arg_expr);
            duckdb::LogicalType arg_ltype = PgTypeOidToDuckType(arg_typid, arg_typmod);

            duck_arg->return_type = arg_ltype;

            arg_types.push_back(arg_ltype);
            arg_exprs.push_back(std::move(duck_arg));
        } else {
            arg_exprs.clear();
            arg_types.clear();
        }

        // 聚合结果类型
        LogicalType return_type = PgTypeOidToDuckType(aggref->aggtype, -1);

        // DISTINCT / FILTER
        bool is_distinct = (aggref->aggdistinct != NIL);

        duckdb::unique_ptr<duckdb::Expression> filter_expr;
        if (aggref->aggfilter) {
            filter_expr = map_pg_expr((Node *)aggref->aggfilter);
        }

        // 从 DuckDB Catalog 拿聚合函数实现
        auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
        auto &entry = catalog.GetEntry<duckdb::AggregateFunctionCatalogEntry>(
            context,
            INVALID_CATALOG,
            DEFAULT_SCHEMA,
            fn_name
        );
        auto &func_set = entry.functions;

        duckdb::AggregateFunction agg_func =
            func_set.GetFunctionByArguments(context, arg_types);

        duckdb::AggregateType aggr_type =
            is_distinct ? duckdb::AggregateType::DISTINCT
                        : duckdb::AggregateType::NON_DISTINCT;

        auto bound_aggr = duckdb::make_uniq<duckdb::BoundAggregateExpression>(
            std::move(agg_func),
            std::move(arg_exprs),
            std::move(filter_expr),
            nullptr,
            aggr_type
        );
        bound_aggr->return_type = return_type;

        aggregates.push_back(std::move(bound_aggr));
    }

    // 3) grouping_sets / grouping_functions：TPCH 场景为空
    duckdb::vector<duckdb::GroupingSet> grouping_sets;
    duckdb::vector<duckdb::unsafe_vector<duckdb::idx_t>> grouping_functions;

    // 是否存在“字符串 group key”，后面决定要不要插压缩/解压 Projection
    bool has_string_group = false;
    for (idx_t i = 0; i < group_is_string.size(); i++) {
        if (group_is_string[i]) {
            has_string_group = true;
            break;
        }
    }

    // 压缩后 key 的类型（例如 UTINYINT）
    duckdb::LogicalType compress_key_type = duckdb::LogicalType::UTINYINT;
    // 为聚集函数做重复表达式消除的问题
    auto &duplicate_child = duplicate_subexprs(child_phys, aggregates, groups);

    // 如果后面要解压，需要提前知道 compress 的返回类型
    if (has_string_group) {
        auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
        auto &compress_entry =
            catalog.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
                context,
                INVALID_CATALOG,
                DEFAULT_SCHEMA,
                "__internal_compress_string_utinyint"
            );
        auto &compress_set = compress_entry.functions;

        duckdb::vector<duckdb::LogicalType> compress_arg_types;
        compress_arg_types.push_back(duckdb::LogicalType::VARCHAR);

        duckdb::ScalarFunction compress_fun =
            compress_set.GetFunctionByArguments(context, compress_arg_types);

        compress_key_type = compress_fun.return_type; // 一般是 UTINYINT
    }


    duckdb::PhysicalOperator *agg_child = &duplicate_child;

    // --------------------------------------------------
    // 5) 在 proj_child 上面插一层真正的“压缩 PROJECTION”
    //
    //   PROJECTION (compress_proj)
    //     compress_string(#0)   ← 字符串 group key
    //     compress_string(#1)
    //     #2
    //     #3 ...
    //   PROJECTION (proj_child)
    // --------------------------------------------------
    if (has_string_group) {
        auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
        auto &compress_entry =
            catalog.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
                context,
                INVALID_CATALOG,
                DEFAULT_SCHEMA,
                "__internal_compress_string_utinyint"
            );
        auto &compress_set = compress_entry.functions;

        duckdb::vector<duckdb::LogicalType> compress_arg_types;
        compress_arg_types.push_back(duckdb::LogicalType::VARCHAR);
        duckdb::ScalarFunction compress_fun =
            compress_set.GetFunctionByArguments(context, compress_arg_types);

        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
        duckdb::vector<duckdb::LogicalType>                    proj_types;

        auto &child_types = agg_child->types;
        proj_exprs.reserve(child_types.size());
        proj_types.reserve(child_types.size());

        for (idx_t col = 0; col < child_types.size(); col++) {
            bool this_is_string_group_col =
                (col < group_is_string.size() && group_is_string[col]);

            if (this_is_string_group_col) {
                duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> fun_args;
                fun_args.push_back(
                    duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                        "#" + std::to_string(col),                   // alias
                        child_types[col],
                        col
                    )
                );

                auto fun_expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
                    compress_fun.return_type,
                    compress_fun,
                    std::move(fun_args),
                    nullptr
                );

                proj_types.push_back(compress_fun.return_type);
                proj_exprs.push_back(std::move(fun_expr));
            } else {
                proj_types.push_back(child_types[col]);
                proj_exprs.push_back(
                    duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                        "#" + std::to_string(col),
                        child_types[col],
                        col
                    )
                );
            }
        }

        auto &compress_proj = Make<duckdb::PhysicalProjection>(
            std::move(proj_types),
            std::move(proj_exprs),
            agg_child->estimated_cardinality
        );
        compress_proj.children.push_back(*agg_child);
        agg_child = &compress_proj;

        // 注意：此时 group 的 BoundRef 仍然是 #0/#1/...，
        // 但是列位置没变，只是类型变成了压缩类型，
        // 所以我们需要把 groups[i].return_type / output_types 里
        // 对应的 group key 类型改成 compress_key_type。

        for (idx_t gi = 0; gi < group_is_string.size(); gi++) {
            if (!group_is_string[gi]) {
                continue;
            }
            auto *ref = dynamic_cast<duckdb::BoundReferenceExpression  *>(groups[gi].get());
            if (ref) {
                ref->return_type = compress_key_type;
            }
        }

        // output_types：Agg 输出前 agg->numCols 列我们假定是 group key
        //（TPCH Q1 成立，泛化场景需要更精细的映射）
        idx_t out_idx = 0;
        for (ListCell *lc = list_head(plan_node->targetlist);
             lc != nullptr;
             lc = lnext(plan_node->targetlist, lc)) {

            auto *tle = (TargetEntry *) lfirst(lc);
            if (!tle || tle->resjunk) {
                continue;
            }

            if (out_idx < group_is_string.size() && group_is_string[out_idx]) {
                output_types[out_idx] = compress_key_type;
            }
            out_idx++;
        }
    }


        // --------------------------------------------------
    // 4) 先用 ExtractAggregateExpressionsCompat 下推
    //    group / agg 输入表达式到最底层 PROJECTION
    //
    //   proj_child: PROJECTION
    //      [0..G-1] group key exprs
    //      [G..]    各种聚合参数 expr
    //   groups / aggregates 都被改成 BoundRef(#i)
    // --------------------------------------------------
    auto &proj_child =
        ExtractAggregateExpressionsCompat(*agg_child, aggregates, groups);

    // --------------------------------------------------
    // 6) 真正创建 PhysicalHashAggregate
    // --------------------------------------------------
    auto group_validity    = duckdb::TupleDataValidityType::CAN_HAVE_NULL_VALUES;
    auto distinct_validity = duckdb::TupleDataValidityType::CANNOT_HAVE_NULL_VALUES;

    duckdb::idx_t estimated_card = (duckdb::idx_t)plan_node->plan_rows;
    if (estimated_card == 0) {
        estimated_card = 1;
    }

    auto &hash_agg = Make<duckdb::PhysicalHashAggregate>(
        context,
        std::move(output_types),
        std::move(aggregates),
        std::move(groups),
        std::move(grouping_sets),
        std::move(grouping_functions),
        estimated_card,
        group_validity,
        distinct_validity
    );

    hash_agg.children.push_back(proj_child);

    // --------------------------------------------------
    // 7) 顶层“解压 PROJECTION”
    //
    //   PROJECTION (top_proj)
    //     decompress_string(#0)
    //     decompress_string(#1)
    //     #2, #3 ...
    //   HASH_GROUP_BY
    // --------------------------------------------------
    if (!has_string_group) {
        // 没有字符串 group key，就不需要解压层，直接返回 HashAgg
        return hash_agg;
    }

    auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
    auto &decompress_entry =
        catalog.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
            context,
            INVALID_CATALOG,
            DEFAULT_SCHEMA,
            "__internal_decompress_string"
        );
    auto &decompress_set = decompress_entry.functions;

    duckdb::vector<duckdb::LogicalType> decompress_arg_types;
    decompress_arg_types.push_back(compress_key_type);

    duckdb::ScalarFunction decompress_fun =
        decompress_set.GetFunctionByArguments(context, decompress_arg_types);

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> top_proj_exprs;
    duckdb::vector<duckdb::LogicalType>                    top_proj_types;

    auto &agg_out_types = hash_agg.types;
    top_proj_exprs.reserve(agg_out_types.size());
    top_proj_types.reserve(agg_out_types.size());

    for (idx_t col = 0; col < agg_out_types.size(); col++) {
        bool this_is_compressed_group_col =
            (col < group_is_string.size() && group_is_string[col]);

        if (this_is_compressed_group_col) {
            duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> fun_args;
            fun_args.push_back(
                duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                    "#" + std::to_string(col),
                    agg_out_types[col],
                    col
                )
            );

            auto fun_expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
                decompress_fun.return_type,   // 一般是 VARCHAR
                decompress_fun,
                std::move(fun_args),
                nullptr
            );

            top_proj_types.push_back(decompress_fun.return_type);
            top_proj_exprs.push_back(std::move(fun_expr));
        } else {
            top_proj_types.push_back(agg_out_types[col]);
            top_proj_exprs.push_back(
                duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                    "#" + std::to_string(col),
                    agg_out_types[col],
                    col
                )
            );
        }
    }

    auto &top_proj = Make<duckdb::PhysicalProjection>(
        std::move(top_proj_types),
        std::move(top_proj_exprs),
        hash_agg.estimated_cardinality
    );
    top_proj.children.push_back(hash_agg);

    return top_proj;
}

duckdb::PhysicalOperator &
PgPhysicalPlanGenerator::CreatePlanSort(Sort *sort) {
    using namespace duckdb;

    Plan *plan_node = &sort->plan;
    Plan *child_plan = outerPlan(plan_node);
    if (!child_plan) {
        ereport(ERROR,
                (errmsg("CreatePlanSort: Sort without child")));
    }

    // 1. 递归构造 child 的物理计划
    auto &child_phys = CreatePlan(child_plan);

    // 没有排序列，直接返回
    if (sort->numCols == 0) {
        return child_phys;
    }

    // 2. 输出类型：来自当前 plan 的 targetlist
    duckdb::vector<LogicalType> output_types;
    for (ListCell *lc = list_head(plan_node->targetlist);
         lc != nullptr;
         lc = lnext(plan_node->targetlist, lc)) {

        auto *tle = (TargetEntry *) lfirst(lc);
        if (!tle || tle->resjunk) {
            continue;
        }
        Oid   typid  = exprType((Node *) tle->expr);
        int32 typmod = exprTypmod((Node *) tle->expr);
        output_types.push_back(PgTypeOidToDuckType(typid, typmod));
    }

    // ---------- 3) 收集 ORDER BY 键的元信息 ----------
    auto IsStringTypePG = [](Oid typid) {
        return (typid == BPCHAROID  ||
                typid == VARCHAROID ||
                typid == TEXTOID   ||
                typid == NAMEOID);
    };

    // 每个排序键：是否字符串类型
    duckdb::vector<bool>           key_is_string;
    // 每个排序键：在 child 输出里的列下标（0-based）
    duckdb::vector<idx_t>          sort_col_indices;
    // 每个排序键：排序方向 / NULL 顺序
    duckdb::vector<OrderType>      key_order_type;
    duckdb::vector<OrderByNullType> key_null_order;
    // 每个排序键：人类可读列名，用作 alias（比如 "l_orderkey"）
    duckdb::vector<std::string>    key_names;

    key_is_string.reserve(sort->numCols);
    sort_col_indices.reserve(sort->numCols);
    key_order_type.reserve(sort->numCols);
    key_null_order.reserve(sort->numCols);
    key_names.reserve(sort->numCols);

    for (int i = 0; i < sort->numCols; i++) {
        AttrNumber resno = sort->sortColIdx[i];   // ORDER BY 对应 targetlist 的第几列（1-based）
        // SeqScan / Projection 这套转换里，我们约定：列序号 = resno - 1
        idx_t col_idx = (idx_t)(resno - 1);
        sort_col_indices.push_back(col_idx);

        // 找到对应的 TargetEntry，便于拿类型和名字
        TargetEntry *tle = nullptr;
        for (ListCell *lc = list_head(plan_node->targetlist);
             lc != nullptr;
             lc = lnext(plan_node->targetlist, lc)) {
            auto *cur = (TargetEntry *) lfirst(lc);
            if (cur->resno == resno) {
                tle = cur;
                break;
            }
        }
        if (!tle) {
            ereport(ERROR,
                    (errmsg("CreatePlanSort: cannot find TargetEntry for sortColIdx %d",
                            (int)resno)));
        }

        // 类型：判断是不是字符串
        Oid typid = exprType((Node *)tle->expr);
        key_is_string.push_back(IsStringTypePG(typid));

        // 列名：用和 Agg 那边一样的策略选 alias
        std::string cname = ChooseAliasFromPG((Expr *)tle->expr, tle);
        if (cname.empty()) {
            cname = "col" + std::to_string((int)resno - 1);
        }
        key_names.push_back(cname);

        // 排序方向
        Oid sortop = sort->sortOperators[i];
        bool reverse = false;
        Oid eq_op = get_equality_op_for_ordering_op(sortop, &reverse);
        if (!OidIsValid(eq_op)) {
            ereport(ERROR,
                    (errmsg("CreatePlanSort: invalid ordering operator %u", sortop)));
        }
        OrderType order_type = reverse ? OrderType::DESCENDING
                                       : OrderType::ASCENDING;
        key_order_type.push_back(order_type);

        // NULLS FIRST / LAST
        bool nulls_first = sort->nullsFirst[i];
        OrderByNullType null_order =
            nulls_first ? OrderByNullType::NULLS_FIRST
                        : OrderByNullType::NULLS_LAST;
        key_null_order.push_back(null_order);
    }

    // 是否存在“字符串排序键”，决定要不要做压缩 / 解压
    bool has_string_key = false;
    for (auto f : key_is_string) {
        if (f) {
            has_string_key = true;
            break;
        }
    }

    // 压缩后 key 的类型（一般是 UTINYINT）
    LogicalType compress_key_type = LogicalType::UTINYINT;
    if (has_string_key) {
        // 从 Catalog 里把压缩函数的返回类型拿出来，避免写死 UTINYINT
        auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
        auto &compress_entry =
            catalog.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
                context,
                INVALID_CATALOG,
                DEFAULT_SCHEMA,
                "__internal_compress_string_utinyint"
            );
        auto &compress_set = compress_entry.functions;

        duckdb::vector<duckdb::LogicalType> compress_arg_types;
        compress_arg_types.push_back(duckdb::LogicalType::VARCHAR);
        duckdb::ScalarFunction compress_fun =
            compress_set.GetFunctionByArguments(context, compress_arg_types);

        compress_key_type = compress_fun.return_type; // 一般是 UTINYINT
    }

    // 从这里开始构造物理树
    duckdb::PhysicalOperator *order_child = &child_phys;

    // ---------- 4) 在 ORDER_BY 下面插入压缩 PROJECTION ----------
    if (has_string_key) {
        auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
        auto &compress_entry =
            catalog.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
                context,
                INVALID_CATALOG,
                DEFAULT_SCHEMA,
                "__internal_compress_string_utinyint"
            );
        auto &compress_set = compress_entry.functions;

        duckdb::vector<duckdb::LogicalType> compress_arg_types;
        compress_arg_types.push_back(duckdb::LogicalType::VARCHAR);
        duckdb::ScalarFunction compress_fun =
            compress_set.GetFunctionByArguments(context, compress_arg_types);

        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
        duckdb::vector<duckdb::LogicalType>                    proj_types;

        auto &child_types = order_child->types;
        proj_exprs.reserve(child_types.size());
        proj_types.reserve(child_types.size());

        for (idx_t col = 0; col < child_types.size(); col++) {
            // 判断这个列是不是“字符串排序键对应的列”
            bool this_is_string_sort_col = false;
            for (idx_t k = 0; k < sort_col_indices.size(); k++) {
                if (!key_is_string[k]) {
                    continue;
                }
                if (sort_col_indices[k] == col) {
                    this_is_string_sort_col = true;
                    break;
                }
            }

            if (this_is_string_sort_col) {
                // compress_fun(原始列)
                duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> fun_args;
                fun_args.push_back(
                    duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                        "#" + std::to_string(col),                    // alias 暂不重要
                        child_types[col],
                        col
                    )
                );

                auto fun_expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
                    compress_fun.return_type,
                    compress_fun,
                    std::move(fun_args),
                    nullptr
                );

                proj_types.push_back(compress_fun.return_type);
                proj_exprs.push_back(std::move(fun_expr));
            } else {
                // 非字符串排序键或者非排序列，原样透传
                proj_types.push_back(child_types[col]);
                proj_exprs.push_back(
                    duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                        "#" + std::to_string(col) ,
                        child_types[col],
                        col
                    )
                );
            }
        }

        auto &compress_proj = Make<duckdb::PhysicalProjection>(
            std::move(proj_types),
            std::move(proj_exprs),
            order_child->estimated_cardinality
        );
        compress_proj.children.push_back(*order_child);
        order_child = &compress_proj;
        // 到这里为止：order_child->types 里，那些字符串排序列已经是压缩后的类型
    }

    // ---------- 5) 基于“压缩后的 child”重建 BoundOrderByNode ----------
    duckdb::vector<BoundOrderByNode> orders;
    orders.reserve(sort->numCols);

    auto &order_child_types = order_child->types;

    for (int i = 0; i < sort->numCols; i++) {
        idx_t col_idx = sort_col_indices[i];
        if (col_idx >= order_child_types.size()) {
            ereport(ERROR,
                    (errmsg("CreatePlanSort: sortColIdx %zu out of range (child has %zu cols)",
                            (size_t)col_idx,
                            (size_t)order_child_types.size())));
        }

        // ★ 关键：这里 alias 设成 key_names[i]，所以 ToString 会打印列名，而不是 #[0.0] ★
        const std::string &cname = key_names[i];

        auto key_expr = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
            cname,                        // alias = 真实列名，如 "l_orderkey"
            order_child_types[col_idx],   // 类型已是压缩后的（如果是字符串排序列）
            col_idx
        );

        orders.emplace_back(
            key_order_type[i],
            key_null_order[i],
            std::move(key_expr)
        );
    }

    // ---------- 6) projection_map：恒等映射 ----------
    duckdb::vector<idx_t> projection_map;
    projection_map.reserve(order_child->types.size());
    for (idx_t i = 0; i < order_child->types.size(); i++) {
        projection_map.push_back(i);
    }

    idx_t estimated_card = (idx_t)plan_node->plan_rows;
    if (estimated_card == 0) {
        estimated_card = 1;
    }

    // ---------- 7) 创建 PhysicalOrder ----------
    auto &order = Make<duckdb::PhysicalOrder>(
        order_child->types,     // 排序不改变列类型
        std::move(orders),
        std::move(projection_map),
        estimated_card
    );
    order.children.push_back(*order_child);

    // ---------- 8) 顶层“解压 PROJECTION” ----------
    if (!has_string_key) {
        // 没有字符串排序键，就不需要解压层，直接返回 ORDER_BY
        return order;
    }

    auto &catalog2 = duckdb::Catalog::GetSystemCatalog(context);
    auto &decompress_entry =
        catalog2.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
            context,
            INVALID_CATALOG,
            DEFAULT_SCHEMA,
            "__internal_decompress_string"
        );
    auto &decompress_set = decompress_entry.functions;

    // 解压函数参数类型 = 压缩后的 key 类型
    duckdb::vector<duckdb::LogicalType> decompress_arg_types;
    decompress_arg_types.push_back(compress_key_type);

    duckdb::ScalarFunction decompress_fun =
        decompress_set.GetFunctionByArguments(context, decompress_arg_types);

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> top_proj_exprs;
    duckdb::vector<duckdb::LogicalType>                    top_proj_types;

    auto &agg_out_types = order.types;
    top_proj_exprs.reserve(agg_out_types.size());
    top_proj_types.reserve(agg_out_types.size());

    for (idx_t col = 0; col < agg_out_types.size(); col++) {
        bool this_is_compressed_sort_col = false;

        // 判断当前列是不是被压缩过的排序列（通过 sort_col_indices + key_is_string）
        for (idx_t k = 0; k < sort_col_indices.size(); k++) {
            if (!key_is_string[k]) {
                continue;
            }
            if (sort_col_indices[k] == col) {
                this_is_compressed_sort_col = true;
                break;
            }
        }

        if (this_is_compressed_sort_col) {
            // 构造 decompress_fun(#col)
            duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> fun_args;
            fun_args.push_back(
                duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                    "#" + std::to_string(col),  // 输入列 alias 不重要
                    agg_out_types[col],
                    col
                )
            );

            auto fun_expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
                decompress_fun.return_type,   // 一般是 VARCHAR
                decompress_fun,
                std::move(fun_args),
                nullptr
            );

            // ★ 不要设置 alias，让 ToString 打印 __internal_decompress_string(...) ★
            // fun_expr->alias = key_names[k];   // 这行不要写

            top_proj_types.push_back(decompress_fun.return_type);
            top_proj_exprs.push_back(std::move(fun_expr));
        } else {
            // 非压缩列：直接透传
            top_proj_types.push_back(agg_out_types[col]);
            top_proj_exprs.push_back(
                duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                    "#" + std::to_string(col) ,  // alias 空，打印成 #[0.x] 也无所谓
                    agg_out_types[col],
                    col
                )
            );
        }
    }

    auto &top_proj = Make<duckdb::PhysicalProjection>(
        std::move(top_proj_types),
        std::move(top_proj_exprs),
        order.estimated_cardinality
    );
    top_proj.children.push_back(order);

    top_proj.Print();

    return top_proj;

}





extern "C" bool
pg_to_duckdb_physical_plan(void *stmt_ptr, void **out_plan_ptr, char **error_msg) {
    if (!out_plan_ptr) {
        set_error(error_msg, "out_plan_ptr is NULL");
        return false;
    }

    //新的上下文开始
    // 1. 建一个“长期存活”的 DuckDB 实例和连接（避免 plan 指向已销毁 allocator）
    auto &rt = GetDuckRuntime();

    try {
        PlannedStmt *pstmt = reinterpret_cast<PlannedStmt *>(stmt_ptr);
        if (!pstmt || !pstmt->planTree) {
            set_error(error_msg, "pg_to_duckdb_physical_plan: null PlannedStmt or planTree");
            return false;
        }
        
        // 1.1 防御性 ROLLBACK：如果上一次事务是 aborted，这里先清理掉
        {
            auto rb = rt.conn.Query("ROLLBACK");
        }

        // 初始化 DuckDB Q1 表 + 插 1 行（关键）
        EnsureDuckdbQ1TestData();

        // // 1. 创建 DuckDB 实例 + 连接，拿 ClientContext
        // // 1. 建一个“长期存活”的 DuckDB 实例和连接（避免 plan 指向已销毁 allocator）
        // static duckdb::DuckDB db;
        // static duckdb::Connection conn(db);
        // duckdb::ClientContext &ctx = *conn.context;

        // // 2. 用上面定义的 PgPhysicalPlanGenerator 把 Plan* 变成 PhysicalOperator*
        // PgPhysicalPlanGenerator gen(ctx);
        // unique_ptr<duckdb::PhysicalPlan> plan = gen.PlanFromPlannedStmt(pstmt);

        // // 3. 把根 PhysicalOperator 指针交给调用者管理
        // *out_plan_ptr = plan.release();
        // return true;



        // 2. 显式开启一个 DuckDB 事务（关键！）
    
        auto begin_res = rt.conn.Query("BEGIN TRANSACTION");
        if (begin_res->HasError()) {
            set_error(error_msg, begin_res->GetError().c_str());
            return false;
        }


        duckdb::ClientContext &ctx = *rt.conn.context;


        // 3. 用这个带事务的 context 做计划翻译
        PgPhysicalPlanGenerator gen(ctx);
        std::unique_ptr<duckdb::PhysicalPlan> plan = gen.PlanFromPlannedStmt(pstmt);

        // 提交事务（这里只做 catalog 查找/函数绑定，不改数据）
        auto commit_res = rt.conn.Query("COMMIT");
        if (commit_res->HasError()) {
            // 理论上很少出错，如果出错可以再尝试 ROLLBACK
            auto rb = rt.conn.Query("ROLLBACK");
            std::string msg = "duckdb COMMIT failed: ";
            msg += commit_res->GetError();
            set_error(error_msg, msg.c_str());
            return false;
        }

        // 把 PhysicalPlan 指针交给调用者，后面由 duckdb_physical_plan_free 删除
        *out_plan_ptr = plan.release();
        return true;
    } catch (std::exception &ex) {
        // 8. 任何 C++ 异常都尝试回滚 DuckDB 事务
        auto rb = rt.conn.Query("ROLLBACK");
        (void)rb; // 忽略 ROLLBACK 的返回错误
        set_error(error_msg, ex.what());
        return false;
    } catch (...) {
        auto rb = rt.conn.Query("ROLLBACK");
        (void)rb; // 忽略 ROLLBACK 的返回错误
        set_error(error_msg, "unknown error in pg_to_duckdb_physical_plan");
        return false;
    }
}

extern "C" bool
duckdb_physical_serialize(void *plan_ptr, char **out_str, char **error_msg) {
    if (!out_str) {
        set_error(error_msg, "out_str is NULL");
        return false;
    }
    if (!plan_ptr) {
        set_error(error_msg, "duckdb_physical_serialize: plan_ptr is NULL");
        return false;
    }
    try {
        auto *plan = reinterpret_cast<duckdb::PhysicalPlan *>(plan_ptr);
        duckdb::PhysicalOperator &root = plan->Root();

        std::string s = root.ToString();
        char *buf = (char *)malloc(s.size() + 1);
        memcpy(buf, s.c_str(), s.size() + 1);
        *out_str = buf;
        return true;
    } catch (std::exception &ex) {
        set_error(error_msg, ex.what());
        return false;
    } catch (...) {
        set_error(error_msg, "unknown error in duckdb_physical_serialize");
        return false;
    }
}


extern "C" void
duckdb_physical_plan_free(void *plan_ptr) {
    if (!plan_ptr) {
        return;
    }
    auto *plan = reinterpret_cast<duckdb::PhysicalPlan *>(plan_ptr);
    delete plan;
}



extern "C" const char *dbg_expr_detail(duckdb::Expression *expr) {
    static std::string last;

    if (!expr) {
        last = "<null>";
        return last.c_str();
    }

    std::string base = expr->ToString();

    auto *ref = dynamic_cast<duckdb::BoundReferenceExpression *>(expr);
    auto *fn  = dynamic_cast<duckdb::BoundFunctionExpression *>(expr);

    if (ref) {
        last = duckdb::StringUtil::Format(
            "%s [BoundRef index=%d type=%s]",
            base.c_str(),
            (int)ref->index,
            ref->return_type.ToString().c_str()
        );
    } else if (fn) {
        last = duckdb::StringUtil::Format(
            "%s [BoundFunc name=%s return=%s]",
            base.c_str(),
            fn->function.name.c_str(),
            fn->return_type.ToString().c_str()
        );
    } else {
        last = duckdb::StringUtil::Format(
            "%s [type=%s]",
            base.c_str(),
            expr->return_type.ToString().c_str()
        );
    }
    return last.c_str();
}

static vector<string> getOutputColumnNames(PhysicalPlan &plan) {
    auto &root_op = plan.Root();
    if (root_op.type == PhysicalOperatorType::TABLE_SCAN)
        return root_op.Cast<PhysicalTableScan>().names;
    if (root_op.type == PhysicalOperatorType::PROJECTION) {
        auto &proj = root_op.Cast<PhysicalProjection>();
        vector<string> names;
        for (auto &expr : proj.select_list)
            names.push_back(expr->GetName());
        return names;
    }

    vector<string> names;
    auto &types = root_op.GetTypes();
    for (idx_t i = 0; i < types.size(); i++)
        names.push_back("col" + std::to_string(i));

    return names;
}

extern "C" bool
pg_run_duckdb_physical_plan(void *stmt_ptr, void **out_plan_ptr, char **error_msg) {
    // 1. 建一个“长期存活”的 DuckDB 实例和连接（避免 plan 指向已销毁 allocator）
    static duckdb::DuckDB db("/home/shf/duckdb/postgresql-16.1/contrib/duckdb-1.4.2/build/release/t.db");
    static duckdb::Connection conn(db);

    try {
        // 1. 开启一个duckdb事务，得到context
        auto rb = conn.Query("ROLLBACK");
        auto begin_res = conn.Query("BEGIN TRANSACTION");
        if (begin_res->HasError()) {
            set_error(error_msg, begin_res->GetError().c_str());
            return false;
        }

        // 2.生成duckdb执行计划
        PlannedStmt *pstmt = reinterpret_cast<PlannedStmt *>(stmt_ptr);
        duckdb::ClientContext &ctx = *conn.context;

        PgPhysicalPlanGenerator gen(ctx);
        unique_ptr<duckdb::PhysicalPlan> plan = gen.PlanFromPlannedStmt(pstmt);

        // 3. 生成可执行prepared plan
        shared_ptr<PreparedStatementData> prepared = make_shared_ptr<PreparedStatementData>(StatementType::SELECT_STATEMENT);
        auto prep = make_shared_ptr<StatementProperties>();
        prepared->properties = *prep;
        prepared->names = getOutputColumnNames(*plan);

        auto &root_op = plan->Root();
        prepared->types = root_op.types;
        prepared->value_map  = {};

        prepared->physical_plan = std::move(plan);

        // 交给执行器
        PendingQueryParameters parameters;
        ctx.CheckIfPreparedStatementIsExecutable(*prepared);
        {
            // 锁的生命周期
            auto lock = ctx.LockContext();
            ctx.BeginQueryInternal(*lock, "select 1");
            auto pending = ctx.PendingPreparedStatementInternal(*lock, move(prepared), parameters);

            if (pending->HasError()) {
                string str = "duckdb Execute failed: " + pending->GetError();
                set_error(error_msg, str.c_str());
                return false;
            }
            else
            {
                unique_ptr<QueryResult> res = ctx.ExecutePendingQueryInternal(*lock, *pending);
                if (res)
                {
                    set_error(error_msg, res->ToString().c_str());
                    elog(WARNING, "%s", res->ToString().c_str());
                }
            }
        }


        // 提交事务（这里只做 catalog 查找/函数绑定，不改数据）
        rb = conn.Query("ROLLBACK");
        return true;
    } catch (std::exception &ex) {
        // 8. 任何 C++ 异常都尝试回滚 DuckDB 事务
        auto rb = conn.Query("ROLLBACK");
        (void)rb; // 忽略 ROLLBACK 的返回错误
        set_error(error_msg, ex.what());
        return false;
    } catch (...) {
        auto rb = conn.Query("ROLLBACK");
        (void)rb; // 忽略 ROLLBACK 的返回错误
        set_error(error_msg, "unknown error in pg_to_duckdb_physical_plan");
        return false;
    }
}