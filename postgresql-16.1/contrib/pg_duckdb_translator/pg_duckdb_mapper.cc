/* Implementation of map_pg_expr moved out of pg_duckdb_translator.cc
 * This file includes DuckDB headers first (C++), then includes the
 * Postgres C headers within extern "C". Placing DuckDB includes first
 * avoids macro collisions (e.g. Abs, DAYS_PER_MONTH) from Postgres C
 * headers interfering with DuckDB C++ headers.
 */

#include "pg_duckdb_mapper.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <ctime>

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
#include "optimizer/tlist.h"      // get_tle_by_resno
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "nodes/plannodes.h"
}

using namespace duckdb;

extern PlannedStmt *g_current_pstmt;  // 在 translator 里定义并赋值
extern std::vector<std::string> g_current_colnames;
extern std::vector<duckdb::LogicalType> g_current_types;
extern std::vector<int> g_current_attnum_map;

// Implementation of map_pg_expr — accepts void* which is cast to Postgres Node*
unique_ptr<Expression> map_pg_expr(void *vnode) {
    if (!vnode) return nullptr;
    Node *node = reinterpret_cast<Node *>(vnode);
    // Log entry for debugging: record node tag we are about to map
    {
        FILE *f = fopen("/tmp/pg_const_map.log", "a");
        if (f) {
            time_t t = time(NULL);
            fprintf(f, "ENTER map_pg_expr: node=%p tag=%d time=%ld\n", (void*)node, (int) nodeTag(node), (long) t);
            fclose(f);
        }
    }

    // helper: unwrap common PG wrapper nodes that can contain the real expr
    auto unwrap_node = [](Node *n) -> Node * {
        while (n) {
            if (IsA(n, TargetEntry)) {
                TargetEntry *te = (TargetEntry *) n;
                n = (Node *) te->expr;
                continue;
            }
            if (IsA(n, RelabelType)) {
                RelabelType *rt = (RelabelType *) n;
                n = (Node *) rt->arg;
                continue;
            }
            if (IsA(n, CoerceViaIO)) {
                CoerceViaIO *cv = (CoerceViaIO *) n;
                n = (Node *) cv->arg;
                continue;
            }
            if (IsA(n, FuncExpr)) {
                FuncExpr *fe = (FuncExpr *) n;
                if (fe->args != NIL && list_length(fe->args) == 1) {
                    Node *inner = (Node *) linitial(fe->args);
                    FILE *f = fopen("/tmp/pg_const_map.log", "a");
                    if (f) {
                        time_t t = time(NULL);
                        fprintf(f, "unwrap_node: FuncExpr at %p -> inner %p tag=%d time=%ld\n", (void*)fe, (void*)inner, (int) nodeTag(inner), (long)t);
                        fclose(f);
                    }
                    n = inner;
                    continue;
                }
            }
            break;
        }
        return n;
    };

    switch (nodeTag(node)) {
        case T_Var: {
            Var *v = (Var *) node;
        
            // 先算一个列 index（用于 ColumnBinding 和 g_current_*）
            int colidx = -1;
            int varatt = v->varattno;
        
            // 1) 优先走你原来的 attnum 映射（scan 阶段用的）
            if (varatt > 0 && (size_t)(varatt - 1) < g_current_attnum_map.size()) {
                int mapped = g_current_attnum_map[varatt - 1];
                if (mapped >= 0) {
                    colidx = mapped;
                }
            }
        
            // 2) 如果映射表没给出任何东西，再退回用 varattno-1 兜底
            //    （适用于 Agg/Sort 这种 “Var 指 child 输出列” 的情况）
            if (colidx < 0 && varatt > 0) {
                colidx = varatt - 1;
            }
        
            // 最后的保险：如果还没确定，就保守给 0
            if (colidx < 0) {
                colidx = 0;
            }
        
            std::string alias;
        
            // 3) 第一步：如果 varno 是 base 表，就用 rtable 去查真正列名，就是这个car是base table的var比如order by那种
            if (g_current_pstmt && v->varlevelsup == 0 &&
                v->varno > 0 && v->varno <= list_length(g_current_pstmt->rtable)) {
                
                RangeTblEntry *rte = rt_fetch(v->varno, g_current_pstmt->rtable);
                if (rte && rte->rtekind == RTE_RELATION) {
                    Relation rel = table_open(rte->relid, AccessShareLock);
                    TupleDesc tupdesc = RelationGetDescr(rel);
                
                    if (v->varattno > 0 && v->varattno <= tupdesc->natts) {
                        Form_pg_attribute attr = TupleDescAttr(tupdesc, v->varattno - 1);
                        if (!attr->attisdropped) {
                            const char *attname = NameStr(attr->attname);
                            if (attname && attname[0] != '\0') {
                                alias = std::string(attname);  // 比如 "l_extendedprice"
                            }
                        }
                    }
                
                    table_close(rel, AccessShareLock);
                }
            }

            // 4) 如果 base-table 查不到名字，再用当前输出 schema 的列名兜底
            if (alias.empty()) 
            {
                if ((size_t)colidx < g_current_colnames.size()) {
                    alias = g_current_colnames[colidx];
                } else {
                    alias = std::string("col") + std::to_string(colidx);
                }
            }
        
            // 5) 选类型：沿用你之前的逻辑
            duckdb::LogicalType ltype = duckdb::LogicalType::VARCHAR;
            if ((size_t)colidx < g_current_types.size()) {
                ltype = g_current_types[colidx];
            }       
            return make_uniq<BoundReferenceExpression>(alias, ltype, colidx);
        }

        case T_Const: {
            Const *c = (Const *) node;
            if (c->constisnull) {
                return make_uniq<BoundConstantExpression>(duckdb::Value());
            }
            Oid typid = c->consttype;
            switch (typid) {
                case INT2OID: {
                    Oid typoutput; bool typisvarlena;
                    getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                    char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                    long v = strtol(out, NULL, 10);
                    std::string s_out(out);
                    FILE *f = fopen("/tmp/pg_const_map.log", "a");
                    if (f) { fprintf(f, "CONST INT2 typ=%u val=%s isnull=%d\n", typid, s_out.c_str(), c ? c->constisnull : 0); fclose(f); }
                    pfree(out);
                    return make_uniq<BoundConstantExpression>(duckdb::Value::SMALLINT((int16_t)v));
                }
                case INT4OID: {
                    Oid typoutput; bool typisvarlena;
                    getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                    char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                    long v = strtol(out, NULL, 10);
                    std::string s_out(out);
                    if (FILE *f = fopen("/tmp/pg_const_map.log", "a")) { fprintf(f, "CONST INT4 typ=%u val=%s\n", typid, s_out.c_str()); fclose(f); }
                    pfree(out);
                    return make_uniq<BoundConstantExpression>(duckdb::Value::INTEGER((int32_t)v));
                }
                case INT8OID: {
                    Oid typoutput; bool typisvarlena;
                    getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                    char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                    long long v = strtoll(out, NULL, 10);
                    std::string s_out(out);
                    if (FILE *f = fopen("/tmp/pg_const_map.log", "a")) { fprintf(f, "CONST INT8 typ=%u val=%s\n", typid, s_out.c_str()); fclose(f); }
                    pfree(out);
                    return make_uniq<BoundConstantExpression>(duckdb::Value::BIGINT((int64_t)v));
                }
                case FLOAT4OID: {
                    Oid typoutput; bool typisvarlena;
                    getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                    char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                    double dv = strtod(out, NULL);
                    std::string s_out(out);
                    if (FILE *f = fopen("/tmp/pg_const_map.log", "a")) { fprintf(f, "CONST FLOAT4 typ=%u val=%s\n", typid, s_out.c_str()); fclose(f); }
                    pfree(out);
                    return make_uniq<BoundConstantExpression>(duckdb::Value::FLOAT((float)dv));
                }
                case FLOAT8OID: {
                    Oid typoutput; bool typisvarlena;
                    getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                    char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                    double dv = strtod(out, NULL);
                    std::string s_out(out);
                    if (FILE *f = fopen("/tmp/pg_const_map.log", "a")) { fprintf(f, "CONST FLOAT8 typ=%u val=%s\n", typid, s_out.c_str()); fclose(f); }
                    pfree(out);
                    return make_uniq<BoundConstantExpression>(duckdb::Value::DOUBLE(dv));
                }
                case BOOLOID: {
                    bool b = DatumGetBool(c->constvalue);
                    return make_uniq<BoundConstantExpression>(duckdb::Value::BOOLEAN(b));
                }
                case TEXTOID:
                case VARCHAROID:
                case BPCHAROID: {
                    text *t = (text *) DatumGetPointer(c->constvalue);
                    char *s = text_to_cstring(t);
                    std::string ss(s);
                    pfree(s);
                    return make_uniq<BoundConstantExpression>(duckdb::Value(ss));
                }
                default: {
                    Oid typoutput; bool typisvarlena;
                    getTypeOutputInfo(typid, &typoutput, &typisvarlena);
                    char *out = OidOutputFunctionCall(typoutput, c->constvalue);
                    {
                        FILE *f = fopen("/tmp/pg_const_map.log", "a");
                        if (f) { time_t t = time(NULL); fprintf(f, "CONST DEFAULT typ=%u out=%s time=%ld\n", typid, out ? out : (char*)"(null)", (long) t); fclose(f); }
                    }
                    std::string s_out(out);
                    pfree(out);
                    return make_uniq<BoundConstantExpression>(duckdb::Value(s_out));
                }
            }
        }
        case T_Param: {
            Param *p = (Param *) node;
            {
                FILE *f = fopen("/tmp/pg_const_map.log", "a");
                if (f) { time_t t = time(NULL); fprintf(f, "PARAM node encountered id=%d kind=%d time=%ld\n", p->paramid, p->paramkind, (long) t); fclose(f); }
            }
            char buf[64];
            snprintf(buf, sizeof(buf), "PARAM#%d", p->paramid);
            return make_uniq<BoundConstantExpression>(duckdb::Value(std::string(buf)));
        }
        case T_OpExpr: {
            OpExpr *op = (OpExpr *) node;
            List *args = op->args;
            unique_ptr<Expression> left = nullptr;
            unique_ptr<Expression> right = nullptr;
            if (args != NIL) {
                ListCell *lc = list_head(args);
                if (lc) {
                    Node *orig_n = (Node *) lfirst(lc);
                    if (IsA(orig_n, FuncExpr)) {
                        FuncExpr *fe = (FuncExpr *) orig_n;
                        if (fe->args != NIL && list_length(fe->args) == 1) {
                            Node *inner = (Node *) linitial(fe->args);
                            if (inner && IsA(inner, Const)) {
                                Const *cc = (Const *) inner;
                                if (cc->constisnull) {
                                    left = make_uniq<BoundConstantExpression>(duckdb::Value());
                                } else {
                                    Oid typoutput; bool typisvarlena;
                                    getTypeOutputInfo(cc->consttype, &typoutput, &typisvarlena);
                                    char *out = OidOutputFunctionCall(typoutput, cc->constvalue);
                                    double dv = strtod(out, NULL);
                                    pfree(out);
                                    left = make_uniq<BoundConstantExpression>(duckdb::Value::DOUBLE(dv));
                                    FILE *f = fopen("/tmp/pg_const_map.log", "a"); if (f) { time_t t = time(NULL); fprintf(f, "OpExpr FUNCEXPR-unwrapped left parsed const typ=%u val=%f time=%ld\n", cc->consttype, dv, (long)t); fclose(f); }
                                }
                            }
                        }
                    }
                    Node *n = unwrap_node(orig_n);
                    if (n && IsA(n, Const)) {
                        Const *cc = (Const *) n;
                        if (cc->constisnull) left = make_uniq<BoundConstantExpression>(duckdb::Value());
                        else {
                            Oid typoutput; bool typisvarlena;
                            getTypeOutputInfo(cc->consttype, &typoutput, &typisvarlena);
                            char *out = OidOutputFunctionCall(typoutput, cc->constvalue);
                            double dv = strtod(out, NULL);
                            pfree(out);
                            left = make_uniq<BoundConstantExpression>(duckdb::Value::DOUBLE(dv));
                            FILE *f = fopen("/tmp/pg_const_map.log", "a"); if (f) { fprintf(f, "OpExpr left parsed const typ=%u val=%f\n", cc->consttype, dv); fclose(f); }
                        }
                    } else {
                        left = map_pg_expr(orig_n);
                    }
                        lc = lnext(args,lc);
                    if (lc) {
                        Node *orig_n2 = (Node *) lfirst(lc);
                        Node *n2 = unwrap_node(orig_n2);
                        if (n2 && IsA(n2, Const)) {
                            Const *cc2 = (Const *) n2;
                            if (cc2->constisnull) right = make_uniq<BoundConstantExpression>(duckdb::Value());
                            else {
                                Oid typoutput2; bool typisvarlena2;
                                getTypeOutputInfo(cc2->consttype, &typoutput2, &typisvarlena2);
                                char *out2 = OidOutputFunctionCall(typoutput2, cc2->constvalue);
                                double dv2 = strtod(out2, NULL);
                                pfree(out2);
                                right = make_uniq<BoundConstantExpression>(duckdb::Value::DOUBLE(dv2));
                                FILE *f = fopen("/tmp/pg_const_map.log", "a"); if (f) { fprintf(f, "OpExpr right parsed const typ=%u val=%f\n", cc2->consttype, dv2); fclose(f); }
                            }
                        } else {
                            right = map_pg_expr(orig_n2);
                        }
                        if (left && left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                            auto &bc = left->Cast<BoundConstantExpression>();
                            if (bc.value.IsNull()) { FILE *f = fopen("/tmp/pg_const_map.log", "a"); if (f) { fprintf(f, "OpExpr LEFT NULL orig_tag=%d\n", (int) nodeTag(orig_n)); fclose(f); } }
                        }
                        if (right && right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                            auto &bc2 = right->Cast<BoundConstantExpression>();
                            if (bc2.value.IsNull()) { FILE *f = fopen("/tmp/pg_const_map.log", "a"); if (f) { fprintf(f, "OpExpr RIGHT NULL orig_tag=%d\n", (int) nodeTag(orig_n2)); fclose(f); } }
                        }
                    }
                }
            }
            const char *oname = get_opname(op->opno);
            if (!oname) oname = "";
            std::string opname(oname);
                if (opname == "=") {
                return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(left), std::move(right));
            } else if (opname == "<") {
                return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, std::move(left), std::move(right));
            } else if (opname == "<=") {
                return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(left), std::move(right));
            } else if (opname == ">") {
                return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(left), std::move(right));
            } else if (opname == ">=") {
                return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(left), std::move(right));
            } else if (opname == "<>" || opname == "!=") {
                return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(left), std::move(right));
            } else if (opname == "+" || opname == "-" || opname == "*" || opname == "/") {
                try {
                    vector<unique_ptr<Expression>> fchildren;
                    if (left) fchildren.push_back(std::move(left));
                    if (right) fchildren.push_back(std::move(right));
                    vector<LogicalType> arg_types;
                    for (auto &c : fchildren) arg_types.push_back(c ? c->return_type : LogicalType::DOUBLE);
                    LogicalType ret_type = LogicalType::DOUBLE;
                    if (!fchildren.empty() && fchildren[0]) ret_type = fchildren[0]->return_type;
                    ScalarFunction sf(arg_types, ret_type, ScalarFunction::NopFunction);
                    sf.name = opname;
                    return make_uniq<BoundFunctionExpression>(ret_type, std::move(sf), std::move(fchildren), nullptr, true);
                } catch (...) {
                    std::string fname = std::string("op_") + opname;
                    ScalarFunction sf(vector<LogicalType>{}, LogicalType::DOUBLE, ScalarFunction::NopFunction);
                    sf.name = fname;
                    vector<unique_ptr<Expression>> fchildren;
                    if (left) fchildren.push_back(std::move(left));
                    if (right) fchildren.push_back(std::move(right));
                    return make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(sf), std::move(fchildren), nullptr, false);
                }
            } else {
                return make_uniq<BoundConstantExpression>(duckdb::Value());
            }
        }
        case T_BoolExpr: {
            BoolExpr *b = (BoolExpr *) node;
            if (b->boolop == AND_EXPR || b->boolop == OR_EXPR) {
                ExpressionType etype = (b->boolop == AND_EXPR) ? ExpressionType::CONJUNCTION_AND : ExpressionType::CONJUNCTION_OR;
                auto conj = make_uniq<BoundConjunctionExpression>(etype);
                ListCell *lc;
                foreach(lc, b->args) {
                    Node *n = (Node *) lfirst(lc);
                    unique_ptr<Expression> child = map_pg_expr(n);
                    if (child) conj->children.push_back(std::move(child));
                }
                return std::move(conj);
            } else if (b->boolop == NOT_EXPR) {
                if (b->args != NIL) {
                    Node *n = (Node *) linitial(b->args);
                    unique_ptr<Expression> child = map_pg_expr(n);
                    auto opb = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
                    if (child) opb->children.push_back(std::move(child));
                    return std::move(opb);
                }
                return make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
            }
            return make_uniq<BoundConstantExpression>(duckdb::Value());
        }
        case T_Aggref: {
            Aggref *a = (Aggref *) node;
            char *fname = NULL;
            if (OidIsValid(a->aggfnoid)) fname = get_func_name(a->aggfnoid);
            std::string aggname = fname ? std::string(fname) : std::string("agg");
            std::string aggname_l = aggname;
            for (auto &c : aggname_l) c = tolower(c);
            if (a->aggstar && (aggname_l == "count" || aggname_l == "count_star")) aggname = std::string("count_star");
            vector<unique_ptr<Expression>> children;
            if (a->args != NIL) {
                ListCell *alc;
                foreach(alc, a->args) {
                    Node *argnode = (Node *) lfirst(alc);
                    unique_ptr<Expression> carg = nullptr;
                    if (argnode && IsA(argnode, TargetEntry)) {
                        TargetEntry *te = (TargetEntry *) argnode;
                        if (te->expr) carg = map_pg_expr((Node *) te->expr);
                    } else {
                        carg = map_pg_expr(argnode);
                    }
                    if (!carg) carg = make_uniq<BoundConstantExpression>(duckdb::Value());
                    children.push_back(std::move(carg));
                }
            }
            if (a->aggdirectargs != NIL) {
                ListCell *dlc;
                foreach(dlc, a->aggdirectargs) {
                    Node *dnode = (Node *) lfirst(dlc);
                    unique_ptr<Expression> dc = map_pg_expr(dnode);
                    if (!dc) dc = make_uniq<BoundConstantExpression>(duckdb::Value());
                    children.push_back(std::move(dc));
                }
            }
            vector<LogicalType> arg_types;
            for (auto &ch : children) arg_types.push_back(ch ? ch->return_type : LogicalType::ANY);
            LogicalType ret_type = LogicalType::VARCHAR;
            if (!aggname.empty()) {
                std::string an = aggname;
                for (auto &c : an) c = tolower(c);
                if (an == "count" || an == "count_star" || an == "count(*)") ret_type = LogicalType::BIGINT;
                else if (an == "sum") ret_type = LogicalType::DOUBLE;
                else if (an == "avg") ret_type = LogicalType::DOUBLE;
                else if (an == "min" || an == "max") { if (!arg_types.empty()) ret_type = arg_types[0]; else ret_type = LogicalType::VARCHAR; }
                else ret_type = LogicalType::VARCHAR;
            }
            try {
                AggregateFunction agg_fun(aggname, arg_types, ret_type,
                    /*state_size*/ nullptr, /*initialize*/ nullptr, /*update*/ nullptr, /*combine*/ nullptr, /*finalize*/ nullptr,
                    /*null_handling*/ FunctionNullHandling::DEFAULT_NULL_HANDLING, /*simple_update*/ nullptr, /*bind*/ nullptr, /*destructor*/ nullptr,
                    /*statistics*/ nullptr, /*window*/ nullptr, /*serialize*/ nullptr, /*deserialize*/ nullptr);
                unique_ptr<FunctionData> bind_info = nullptr;
                return make_uniq<BoundAggregateExpression>(std::move(agg_fun), std::move(children), nullptr, std::move(bind_info), AggregateType::NON_DISTINCT);
            } catch (...) {
                ScalarFunction sf(std::vector<LogicalType>{}, LogicalType::DOUBLE, ScalarFunction::NopFunction);
                sf.name = aggname;
                return make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(sf), std::move(children), nullptr, false);
            }
        }
        default:
            return make_uniq<BoundConstantExpression>(duckdb::Value());
    }
}



unique_ptr<BoundConstantExpression> PgConstToDuckConst(void *pg_const_node) {
    // 安全兜底：传进来是 NULL，就返回一个 NULL 常量
    if (!pg_const_node) {
        return make_uniq<BoundConstantExpression>(Value());
    }

    // 直接复用你已有的 map_pg_expr，里面已经写好了所有 Const -> Value 的映射逻辑
    auto expr = map_pg_expr(pg_const_node);
    if (!expr) {
        // 转换失败，就返回空指针，让调用方自己决定要不要兜底
        return nullptr;
    }

    // 我们只接受真正的常量表达式
    if (expr->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
        // 不是常量（例如误传了 OpExpr），就返回 nullptr
        return nullptr;
    }

    // 这里强转成 BoundConstantExpression，然后拷贝里面的 Value
    auto &bc = expr->Cast<BoundConstantExpression>();

    // 再构造一个新的 BoundConstantExpression 返回出去
    // （这样调用者拿到的是独立的常量节点）
    return make_uniq<BoundConstantExpression>(bc.value);
}

