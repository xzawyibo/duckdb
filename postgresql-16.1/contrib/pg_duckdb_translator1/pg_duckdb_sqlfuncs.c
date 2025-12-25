#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "utils/varlena.h"
#include "lib/stringinfo.h"
#include "utils/rel.h"
#include "optimizer/tlist.h"
#include <stdlib.h>
#include <time.h>
#include "pg_duckdb_translator.h"
#include "nodes/plannodes.h"
#include "optimizer/planner.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"

/* Provide explicit exported magic function so loader can find it reliably. */
/* Define same magic function name used by Postgres: "Pg_magic_func". */
extern const Pg_magic_struct *Pg_magic_func(void) __attribute__((visibility("default")));
const Pg_magic_struct *Pg_magic_func(void) {
    static const Pg_magic_struct Pg_magic_data = PG_MODULE_MAGIC_DATA;
    return &Pg_magic_data;
}

PG_FUNCTION_INFO_V1(pgsql_to_ducklogical);
PG_FUNCTION_INFO_V1(ducksql_to_ducklogical);    
PG_FUNCTION_INFO_V1(pg_duckdb_explain);
PG_FUNCTION_INFO_V1(pg_duckdb_run);



Datum
pgsql_to_ducklogical(PG_FUNCTION_ARGS) {
    text *sql_text = PG_GETARG_TEXT_PP(0);
    char *sql = text_to_cstring(sql_text);

    List *raw_parsetree_list = raw_parser(sql,RAW_PARSE_DEFAULT);
    if (!raw_parsetree_list || list_length(raw_parsetree_list) == 0) {
        pfree(sql);
        ereport(ERROR, (errmsg("failed to parse SQL"))) ;
    }

    /* Only handle single-statement inputs for now */
    if (list_length(raw_parsetree_list) > 1) {
        pfree(sql);
        ereport(ERROR, (errmsg("only single-statement SQL supported")));
    }

    RawStmt *parsetree = (RawStmt *) linitial(raw_parsetree_list);

    Query *query = parse_analyze_fixedparams(parsetree, sql, NULL, 0, NULL);
    if (!query) {
        {
            FILE *f = fopen("/tmp/pg_const_map.log", "a");
            if (f) {
                time_t t = time(NULL);
                fprintf(f, "pgsql_to_ducklogical: parse_analyze FAILED time=%ld\n", (long) t);
                fclose(f);
            }
        }
        pfree(sql);
        ereport(ERROR, (errmsg("parse_analyze failed")));
    }
    {
        FILE *f = fopen("/tmp/pg_const_map.log", "a");
        if (f) {
            time_t t = time(NULL);
            fprintf(f, "pgsql_to_ducklogical: parse_analyze OK Query=%p time=%ld\n", (void*) query, (long) t);
            fclose(f);
        }
    }
    /* Call diagnostic helper to map and log targetList entries (no-op for normal behavior) */
    pg_duckdb_log_targetlist_nodes((void *) query);

    void *plan_ptr = NULL;
    char *err = NULL;
    bool ok = pg_to_duckdb_logical_plan((void *) query, NULL, &plan_ptr, &err);
    if (!ok) {
        if (err) {
            char *msg = psprintf("translator error: %s", err);
            free(err);
            pfree(sql);
            ereport(ERROR, (errmsg_internal(msg)));
        } else {
            pfree(sql);
            ereport(ERROR, (errmsg("translator returned failure")));
        }
    }

    char *out_s = NULL;
    if (!duckdb_logical_serialize(plan_ptr, &out_s, &err)) {
        if (err) {
            char *msg = psprintf("serialize error: %s", err);
            free(err);
            duckdb_plan_free(plan_ptr);
            pfree(sql);
            ereport(ERROR, (errmsg_internal(msg)));
        } else {
            duckdb_plan_free(plan_ptr);
            pfree(sql);
            ereport(ERROR, (errmsg("serialize failed")));
        }
    }

    /* Free DuckDB plan */
    duckdb_plan_free(plan_ptr);

    text *result = cstring_to_text(out_s);
    free(out_s);
    pfree(sql);
    PG_RETURN_TEXT_P(result);

}

Datum
ducksql_to_ducklogical(PG_FUNCTION_ARGS)
{
    /* Real translator path: use DuckDB's planner to extract a logical plan
     * for the SQL string, serialize it, and return the textual representation.
     * This uses DuckDB's own planner (no Postgres analyze) and mirrors
     * duckdb_translate_sql's output for comparison testing.
     */
    text *sql_text = PG_GETARG_TEXT_PP(0);
    char *sql = text_to_cstring(sql_text);

    void *plan_ptr = NULL;
    char *err = NULL;

    if (!duckdb_extract_logical_plan_from_sql(sql, &plan_ptr, &err)) {
        if (err) {
            char *msg = psprintf("duckdb planner error: %s", err);
            free(err);
            pfree(sql);
            ereport(ERROR, (errmsg_internal(msg)));
        } else {
            pfree(sql);
            ereport(ERROR, (errmsg("duckdb planner failed")));
        }
    }

    char *out_s = NULL;
    if (!duckdb_logical_serialize(plan_ptr, &out_s, &err)) {
        if (err) {
            char *msg = psprintf("serialize error: %s", err);
            free(err);
            duckdb_plan_free(plan_ptr);
            pfree(sql);
            ereport(ERROR, (errmsg_internal(msg)));
        } else {
            duckdb_plan_free(plan_ptr);
            pfree(sql);
            ereport(ERROR, (errmsg("duckdb serialize failed")));
        }
    }

    /* Free DuckDB plan and input buffer */
    duckdb_plan_free(plan_ptr);
    pfree(sql);

    text *result = cstring_to_text(out_s);
    free(out_s);
    PG_RETURN_TEXT_P(result);

}

Datum
pg_duckdb_explain(PG_FUNCTION_ARGS)
{
    text       *sql_text = PG_GETARG_TEXT_PP(0);
    char       *sql = text_to_cstring(sql_text);

    bool        pushed = false;
    MemoryContext plan_mcxt = NULL;

    void       *plan_ptr = NULL;   /* C++ 返回的 PhysicalPlan* */
    char       *plan_str = NULL;   /* C++ serialize 出来的字符串 (malloc) */
    char       *err = NULL;        /* C++ 的错误信息 (malloc) */
    text       *result = NULL;

    PG_TRY();
    {
        /* 1) 事务快照 */
        if (!ActiveSnapshotSet()) {
            PushActiveSnapshot(GetTransactionSnapshot());
            pushed = true;
        }

        /* 2) 单独的内存上下文保存 PG 的 parse/analyze/plan */
        plan_mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                          "pg_duckdb_explain_planner_ctx",
                                          ALLOCSET_DEFAULT_SIZES);

        MemoryContext old = MemoryContextSwitchTo(plan_mcxt);

        /* 3) parse */
        List *raw_parsetree_list = raw_parser(sql, RAW_PARSE_DEFAULT);
        if (!raw_parsetree_list || list_length(raw_parsetree_list) == 0) {
            ereport(ERROR, (errmsg("failed to parse SQL")));
        }
        if (list_length(raw_parsetree_list) > 1) {
            ereport(ERROR, (errmsg("only single-statement SQL supported")));
        }
        RawStmt *parsetree = (RawStmt *) linitial(raw_parsetree_list);

        /* 4) analyze */
        Query *query = parse_analyze_fixedparams(parsetree, sql, NULL, 0, NULL);
        if (!query) {
            ereport(ERROR, (errmsg("parse_analyze failed")));
        }

        /* 5) plan */
        PlannedStmt *pstmt = standard_planner(query, sql, 0, NULL);
        if (!pstmt || !pstmt->planTree) {
            ereport(ERROR, (errmsg("standard_planner failed")));
        }

        MemoryContextSwitchTo(old);

        /* 6) 调 C++：PlannedStmt* -> PhysicalPlan* */
        if (!pg_to_duckdb_physical_plan((void *)pstmt, &plan_ptr, &err)) {
            if (err) {
                ereport(ERROR, (errmsg_internal("translator error: %s", err)));
            } else {
                ereport(ERROR, (errmsg("translator returned failure")));
            }
        }

        /* 7) 再调 C++：PhysicalPlan* -> char* (ToString) */
        if (!duckdb_physical_serialize(plan_ptr, &plan_str, &err)) {
            if (err) {
                ereport(ERROR, (errmsg_internal("serialize error: %s", err)));
            } else {
                ereport(ERROR, (errmsg("serialize returned failure")));
            }
        }

        /* 8) 把 C 字符串变成 text 返回给 SQL */
        result = cstring_to_text(plan_str);

        /* 9) 清理资源 */
        if (plan_ptr) {
            duckdb_physical_plan_free(plan_ptr);//plan_ptr就是转换完的哪个物理算子树
            plan_ptr = NULL;
        }
        if (plan_str) {
            free(plan_str);
            plan_str = NULL;
        }
        if (err) {
            free(err);
            err = NULL;
        }
        if (plan_mcxt) {
            MemoryContextDelete(plan_mcxt);
            plan_mcxt = NULL;
        }
        if (pushed) {
            PopActiveSnapshot();
            pushed = false;
        }
        if (sql) {
            pfree(sql);
            sql = NULL;
        }

        PG_RETURN_TEXT_P(result);
    }
    PG_CATCH();
    {
        /* 异常路径的清理，避免内存泄露 */
        if (plan_ptr) {
            duckdb_physical_plan_free(plan_ptr);
        }
        if (plan_str) {
            free(plan_str);
        }
        if (err) {
            free(err);
        }
        if (plan_mcxt) {
            MemoryContextDelete(plan_mcxt);
        }
        if (pushed) {
            PopActiveSnapshot();
        }
        if (sql) {
            pfree(sql);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_NULL(); /* 不会走到这，只是让编译器闭嘴 */
}

Datum
pg_duckdb_run(PG_FUNCTION_ARGS)
{
    text       *sql_text = PG_GETARG_TEXT_PP(0);
    char       *sql = text_to_cstring(sql_text);

    bool        pushed = false;
    MemoryContext plan_mcxt = NULL;

    void       *plan_ptr = NULL;   /* C++ 返回的 PhysicalPlan* */
    char       *plan_str = NULL;   /* C++ serialize 出来的字符串 (malloc) */
    char       *err = NULL;        /* C++ 的错误信息 (malloc) */
    text       *result = NULL;

    PG_TRY();
    {
        /* 1) 事务快照 */
        if (!ActiveSnapshotSet()) {
            PushActiveSnapshot(GetTransactionSnapshot());
            pushed = true;
        }

        /* 2) 单独的内存上下文保存 PG 的 parse/analyze/plan */
        plan_mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                          "pg_duckdb_explain_planner_ctx",
                                          ALLOCSET_DEFAULT_SIZES);

        MemoryContext old = MemoryContextSwitchTo(plan_mcxt);

        /* 3) parse */
        List *raw_parsetree_list = raw_parser(sql, RAW_PARSE_DEFAULT);
        if (!raw_parsetree_list || list_length(raw_parsetree_list) == 0) {
            ereport(ERROR, (errmsg("failed to parse SQL")));
        }
        if (list_length(raw_parsetree_list) > 1) {
            ereport(ERROR, (errmsg("only single-statement SQL supported")));
        }
        RawStmt *parsetree = (RawStmt *) linitial(raw_parsetree_list);

        /* 4) analyze */
        Query *query = parse_analyze_fixedparams(parsetree, sql, NULL, 0, NULL);
        if (!query) {
            ereport(ERROR, (errmsg("parse_analyze failed")));
        }

        /* 5) plan */
        PlannedStmt *pstmt = standard_planner(query, sql, 0, NULL);
        if (!pstmt || !pstmt->planTree) {
            ereport(ERROR, (errmsg("standard_planner failed")));
        }

        MemoryContextSwitchTo(old);

        /* 6) 调 C++：PlannedStmt* -> PhysicalPlan* */
        if (!pg_run_duckdb_physical_plan((void *)pstmt, &plan_ptr, &err)) {
            if (err) {
                ereport(ERROR, (errmsg_internal("translator error: %s", err)));
            } else {
                ereport(ERROR, (errmsg("translator returned failure")));
            }
        }

        if (err) {
            free(err);
            err = NULL;
        }
        if (plan_mcxt) {
            MemoryContextDelete(plan_mcxt);
            plan_mcxt = NULL;
        }
        if (pushed) {
            PopActiveSnapshot();
            pushed = false;
        }
        if (sql) {
            pfree(sql);
            sql = NULL;
        }

        PG_RETURN_NULL();
    }
    PG_CATCH();
    {
        /* 异常路径的清理，避免内存泄露 */
        if (err) {
            free(err);
        }
        if (plan_mcxt) {
            MemoryContextDelete(plan_mcxt);
        }
        if (pushed) {
            PopActiveSnapshot();
        }
        if (sql) {
            pfree(sql);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_NULL(); /* 不会走到这，只是让编译器闭嘴 */
}