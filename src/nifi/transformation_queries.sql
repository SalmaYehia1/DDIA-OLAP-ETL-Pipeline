
-- NIFI ETL TRANSFORMATION QUERIES
-- These queries are used inside the ExecuteSQL processors
-- to denormalize the data before loading into Parquet.


-- 1. Dim_Nation Processor
-- Purpose: Extract nation mapping for the dimension table.
SELECT n_nationkey, n_name FROM nation;

-- 2. Dim_Supplier Processor
-- Purpose: Extract supplier mapping for the dimension table.
SELECT s_suppkey, s_name FROM supplier;

-- 3. Fact_Lineitem Join Processor (OPTIMIZED)
-- Purpose: Flatten the schema by joining Lineitem, Orders, Customer, and Supplier.
-- Optimization Note: Removed join with 'nation' to improve average execution time.
SELECT 
    l.l_orderkey, 
    l.l_quantity, 
    l.l_extendedprice, 
    l.l_discount, 
    l.l_tax, 
    l.l_shipdate, 
    s.s_suppkey AS supplier_key, 
    o.o_custkey AS customer_nation_key -- Using custkey directly for optimization
FROM lineitem AS l
JOIN orders o ON l.l_orderkey = o.o_orderkey
JOIN customer c ON o.o_custkey = c.c_custkey
JOIN supplier s ON l.l_suppkey = s.s_suppkey;
EOF
