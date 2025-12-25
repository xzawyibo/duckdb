\set ON_ERROR_STOP on
SELECT pgsql_to_ducklogical($$
SELECT l_returnflag, sum(l_quantity) as sum_qty
FROM lineitem
GROUP BY l_returnflag
ORDER BY l_returnflag
LIMIT 10
$$);

SELECT ducksql_to_ducklogical($$
SELECT l_returnflag, sum(l_quantity) as sum_qty
FROM lineitem
GROUP BY l_returnflag
ORDER BY l_returnflag
LIMIT 10
$$);
