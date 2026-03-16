val epochs = 8
val times = new Array[Long](epochs)

for (i <- 0 until epochs) {
  val start = System.nanoTime()

  // Run query without showing results
  spark.sql("""
    SELECT
        n.n_name,
        s.s_name,
        SUM(CAST(f.l_quantity AS DOUBLE)) AS sum_qty,
        SUM(CAST(f.l_extendedprice AS DOUBLE)) AS sum_base_price,
        SUM(CAST(f.l_extendedprice AS DOUBLE) * (1 - CAST(f.l_discount AS DOUBLE))) AS sum_disc_price,
        SUM(CAST(f.l_extendedprice AS DOUBLE) *
            (1 - CAST(f.l_discount AS DOUBLE)) *
            (1 + CAST(f.l_tax AS DOUBLE))) AS sum_charge,
        AVG(CAST(f.l_quantity AS DOUBLE)) AS avg_qty,
        AVG(CAST(f.l_extendedprice AS DOUBLE)) AS avg_price,
        AVG(CAST(f.l_discount AS DOUBLE)) AS avg_disc,
        COUNT(*) AS count_order
    FROM fact_sales f
    JOIN dim_nation n
        ON f.customer_nation_key = n.n_nationkey
    JOIN dim_supplier s
        ON f.supplier_key = s.s_suppkey
    GROUP BY n.n_name, s.s_name
    ORDER BY n.n_name, s.s_name
  """).count()  // only count rows, no output

  val end = System.nanoTime()
  times(i) = (end - start) / 1000000  // milliseconds
  println(s"Epoch ${i + 1}: ${times(i)} ms")
}

println("Execution times (ms): " + times.mkString(", "))
println(s"Average execution time: ${times.sum / times.length} ms")
