select
n.N_NAME as n_name,
s.S_NAME as s_name,
sum(l.l_quantity) as sum_qty,
sum(l.l_extendedprice) as sum_base_price,
sum(l.l_extendedprice * (1 - l.l_discount)) as sum_disc_price,
sum(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) as sum_charge,
avg(l.l_quantity) as avg_qty,
avg(l.l_extendedprice) as avg_price,
avg(l.l_discount) as avg_disc,
count(*) as count_order
from
lineitem l
join orders o on l.l_orderkey = o.o_orderkey
join customer c on o.o_custkey = c.c_custkey
join nation n on c.c_nationkey = n.n_nationkey
join partsupp ps on l.l_partkey = ps.ps_partkey and l.L_SUPPKEY = ps.ps_suppkey
join supplier s on ps.ps_suppkey = s.s_suppkey
where
l_shipdate <= date '1998-12-01' - interval '90' day
group by
n.N_NAME, s.S_NAME;
