package scalaTest

import org.apache.spark.sql.functions.sum

/**
  * TPC-H Query 6
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q06raw1 extends TpchQueryRaw {

  import spark.implicits._

  override def execute(): Unit = {

    val res = lineitem.filter($"l_shipdate" >= "1992-01-01" && $"l_shipdate" < "1994-01-01" && $"l_discount" >= 0 && $"l_discount" <= 0.04 && $"l_quantity" < 40)
      .agg(sum($"l_extendedprice" * $"l_discount"))

    outputDF(res)

  }

}
