# duckdb
编译:
cd postgresql-16.1
make && make install
--make duckdb
cd contrib/duckdb-1.4.2
make
cd ..

--make contrib
make && make install

-----notice
pwd  --- in postgre
find . -name libduckdb.so
cp   libduckdb.so  release/lib


用法：
create extension pg_duckdb_translator;
select pg_duckdb_run($$select 1$$);
select pg_duckdb_explain($$select 1$$);
