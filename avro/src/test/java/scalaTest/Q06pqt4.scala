package scalaTest

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q06pqt4 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int): Unit = {
    init(path, typeId)
    val res = nested.select(explode($"PartsuppList.LineitemList"))
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_shipdate", $"col.l_quantity", $"col.l_extendedprice")
      .filter($"col.l_shipdate" >= "1993-11-01"
        && $"col.l_shipdate" < "1994-01-01"
        && $"col.l_discount" >= 0.04
        && $"col.l_discount" <= 0.04
        && $"col.l_quantity" < 3)
      .agg(sum($"l_extendedprice" * $"l_discount"))
    outputDF(res)
  }

}
