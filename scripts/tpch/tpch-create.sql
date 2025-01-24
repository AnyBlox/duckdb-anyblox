create table part (
    p_partkey     integer        not null,
    p_name        varchar(55)    not null,
    p_mfgr        char(25)       not null,
    p_brand       char(10)       not null,
    p_type        varchar(25)    not null,
    p_size        integer        not null,
    p_container   char(10)       not null,
    p_retailprice double         not null,
    p_comment     varchar(23)    not null,
    primary key (p_partkey)
);

create table region (
    r_regionkey integer      not null,
    r_name      char(25)     not null,
    r_comment   varchar(152) not null,
    primary key (r_regionkey)
);

create table nation (
    n_nationkey integer      not null,
    n_name      char(25)     not null,
    n_regionkey integer      not null,
    n_comment   varchar(152) not null,
    primary key (n_nationkey)
);

create table supplier (
    s_suppkey   integer        not null,
    s_name      char(25)       not null,
    s_address   varchar(40)    not null,
    s_nationkey integer        not null,
    s_phone     char(15)       not null,
    s_acctbal   double         not null,
    s_comment   varchar(101)   not null,
    primary key (s_suppkey)
);

create table partsupp (
    ps_partkey    integer        not null,
    ps_suppkey    integer        not null,
    ps_availqty   integer        not null,
    ps_supplycost double         not null,
    ps_comment    varchar(199)   not null,
    primary key (ps_partkey, ps_suppkey)
);

create table customer (
    c_custkey    integer        not null,
    c_name       varchar(25)    not null,
    c_address    varchar(40)    not null,
    c_nationkey  integer        not null,
    c_phone      char(15)       not null,
    c_acctbal    double         not null,
    c_mktsegment char(10)       not null,
    c_comment    varchar(117)   not null,
    primary key (c_custkey)
);

create table orders (
    o_orderkey      integer        not null,
    o_custkey       integer        not null,
    o_orderstatus   char(1)        not null,
    o_totalprice    double         not null,
    o_orderdate     date           not null,
    o_orderpriority char(15)       not null,
    o_clerk         char(15)       not null,
    o_shippriority  integer        not null,
    o_comment       varchar(79)    not null,
    primary key (o_orderkey)
);

COPY part FROM '/home/gienieczko/hdd/tpch-dbgen/part.tbl' WITH (DELIMITER '|');
COPY region FROM '/home/gienieczko/hdd/tpch-dbgen/region.tbl' WITH (DELIMITER '|');
COPY nation FROM '/home/gienieczko/hdd/tpch-dbgen/nation.tbl' WITH (DELIMITER '|');
COPY supplier FROM '/home/gienieczko/hdd/tpch-dbgen/supplier.tbl' WITH (DELIMITER '|');
COPY partsupp FROM '/home/gienieczko/hdd/tpch-dbgen/partsupp.tbl' WITH (DELIMITER '|');
COPY customer FROM '/home/gienieczko/hdd/tpch-dbgen/customer.tbl' WITH (DELIMITER '|');
COPY orders FROM '/home/gienieczko/hdd/tpch-dbgen/orders.tbl' WITH (DELIMITER '|');

CREATE VIEW lineitem AS (
 SELECT
 l_orderkey,
 l_partkey,
 l_suppkey,
 l_linenumber,
 l_quantity,
 l_extendedprice,
 l_discount,
 l_tax,
 l_returnflag,
 l_linestatus,
 l_shipdate,
 l_commitdate,
 l_receiptdate,
 l_shipinstruct,
 l_shipmode,
 l_comment
 FROM ignition('/home/gienieczko/src/portable-decompress/dataset/tpch-vortex-s20.ignition')
);
