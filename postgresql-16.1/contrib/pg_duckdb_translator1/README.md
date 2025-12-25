DuckDB translator (prototype)
=================================

This directory contains a prototype skeleton for an in-kernel translator
that converts PostgreSQL `PlannedStmt` structures into DuckDB plan objects
(logical and physical). The extension currently includes a C++ translator
implementation and SQL-callable wrappers; several prototype/test files were
removed from the runtime extension and belong in a separate `test/` area.

Files relevant to the extension runtime:
- `pg_duckdb_translator.h` - C API header (opaque pointers)
- `pg_duckdb_translator.cc` - C++ translator implementation (core logic)
- `pg_duckdb_sqlfuncs.c` - SQL-callable glue functions (Postgres wrappers)
- `Makefile` - build/install helper (integrates with Postgres build)
- `pg_duckdb_translator.control` and `pg_duckdb_translator--1.0.sql` - packaging

Notes:
- Development-only prototype/test code (previously in `proto/` and
  `tests/`) has been removed from the extension directory to avoid
  shipping non-runtime artifacts with the installed extension. If you need
  the test harness, move it into a top-level `test/` or `examples/` folder.
- Proper integration into the Postgres build requires adding the generated
  `.o` files for C++ sources into the backend link step and ensuring the
  C++ runtime (`-lstdc++`) and DuckDB library are linked when building the
  server or extension shared object.

Next steps to complete functionality:
1. Complete Plan->DuckDB Logical conversion in `pg_duckdb_translator.cc`.
2. Map Postgres expression nodes (e.g., Aggref, FuncCall) correctly to
   DuckDB bound expressions and logical operators.
3. Add regression tests (pg_regress or separate harness) under `test/` to
   validate translator output against DuckDB planner output (e.g. TPCH Q1).

If you'd like, I can move the removed prototype files into a `test/`
subdirectory and add a small Makefile target there to build the test binary.
