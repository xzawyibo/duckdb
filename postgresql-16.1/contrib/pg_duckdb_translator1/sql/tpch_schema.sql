-- REGION
CREATE TABLE region (
  r_regionkey INTEGER NOT NULL,
  r_name      CHAR(25) NOT NULL,
  r_comment   VARCHAR(152),
  PRIMARY KEY (r_regionkey)
);

-- NATION
CREATE TABLE nation (
  n_nationkey INTEGER NOT NULL,
  n_name      CHAR(25) NOT NULL,
  n_regionkey INTEGER NOT NULL,
  n_comment   VARCHAR(152),
  PRIMARY KEY (n_nationkey),
  FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)
);

-- PART
CREATE TABLE part (
  p_partkey     INTEGER NOT NULL,
  p_name        VARCHAR(55) NOT NULL,
  p_mfgr        CHAR(25) NOT NULL,
  p_brand       CHAR(10) NOT NULL,
  p_type        VARCHAR(25) NOT NULL,
  p_size        INTEGER NOT NULL,
  p_container   CHAR(10) NOT NULL,
  p_retailprice NUMERIC(15,2) NOT NULL,
  p_comment     VARCHAR(23) NOT NULL,
  PRIMARY KEY (p_partkey)
);

-- SUPPLIER
CREATE TABLE supplier (
  s_suppkey   INTEGER NOT NULL,
  s_name      CHAR(25) NOT NULL,
  s_address   VARCHAR(40) NOT NULL,
  s_nationkey INTEGER NOT NULL,
  s_phone     CHAR(15) NOT NULL,
  s_acctbal   NUMERIC(15,2) NOT NULL,
  s_comment   VARCHAR(101) NOT NULL,
  PRIMARY KEY (s_suppkey),
  FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
);

-- PARTSUPP
CREATE TABLE partsupp (
  ps_partkey    INTEGER NOT NULL,
  ps_suppkey    INTEGER NOT NULL,
  ps_availqty   INTEGER NOT NULL,
  ps_supplycost NUMERIC(15,2) NOT NULL,
  ps_comment    VARCHAR(199) NOT NULL,
  PRIMARY KEY (ps_partkey, ps_suppkey),
  FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
  FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
);

-- CUSTOMER
CREATE TABLE customer (
  c_custkey    INTEGER NOT NULL,
  c_name       VARCHAR(25) NOT NULL,
  c_address    VARCHAR(40) NOT NULL,
  c_nationkey  INTEGER NOT NULL,
  c_phone      CHAR(15) NOT NULL,
  c_acctbal    NUMERIC(15,2) NOT NULL,
  c_mktsegment CHAR(10) NOT NULL,
  c_comment    VARCHAR(117) NOT NULL,
  PRIMARY KEY (c_custkey),
  FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
);

-- ORDERS
CREATE TABLE orders (
  o_orderkey      INTEGER NOT NULL,
  o_custkey       INTEGER NOT NULL,
  o_orderstatus   CHAR(1) NOT NULL,
  o_totalprice    NUMERIC(15,2) NOT NULL,
  o_orderdate     DATE NOT NULL,
  o_orderpriority CHAR(15) NOT NULL,
  o_clerk         CHAR(15) NOT NULL,
  o_shippriority  INTEGER NOT NULL,
  o_comment       VARCHAR(79) NOT NULL,
  PRIMARY KEY (o_orderkey),
  FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
);

-- LINEITEM
CREATE TABLE lineitem (
  l_orderkey      INTEGER NOT NULL,
  l_partkey       INTEGER NOT NULL,
  l_suppkey       INTEGER NOT NULL,
  l_linenumber    INTEGER NOT NULL,
  l_quantity      NUMERIC(15,2) NOT NULL,
  l_extendedprice NUMERIC(15,2) NOT NULL,
  l_discount      NUMERIC(15,2) NOT NULL,
  l_tax           NUMERIC(15,2) NOT NULL,
  l_returnflag    CHAR(1) NOT NULL,
  l_linestatus    CHAR(1) NOT NULL,
  l_shipdate      DATE NOT NULL,
  l_commitdate    DATE NOT NULL,
  l_receiptdate   DATE NOT NULL,
  l_shipinstruct  CHAR(25) NOT NULL,
  l_shipmode      CHAR(10) NOT NULL,
  l_comment       VARCHAR(44) NOT NULL

);

-- （可选但常用）索引：TPC-H 常见推荐
CREATE INDEX idx_nation_regionkey   ON nation(n_regionkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX idx_orders_custkey     ON orders(o_custkey);
CREATE INDEX idx_lineitem_partkey   ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey   ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate  ON lineitem(l_shipdate);
CREATE INDEX idx_lineitem_orderkey  ON lineitem(l_orderkey);




/* =========================
   TPC-H Original 22 Queries
   (Q1 ~ Q22, canonical constants)
   ========================= */

-- Q1
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from
  lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;


  CREATE TABLE lineitem (
     l_orderkey INT,
     l_partkey INT,
     l_suppkey INT,
     l_linenumber INT,
     l_quantity double precision,
     l_extendedprice  double precision,
     l_discount    double precision,
     l_tax double precision,
     l_returnflag TEXT,
     l_linestatus TEXT,
     l_shipdate DATE,
     l_commitdate DATE,
     l_receiptdate DATE,
     l_shipinstruct TEXT,
     l_shipmode TEXT,
     l_comment TEXT
 );