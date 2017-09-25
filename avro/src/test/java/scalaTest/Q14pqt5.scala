package scalaTest

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q14pqt5 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int): Unit = {
    init(path, typeId)
    val res = nested.select(explode($"PartsuppList.LineitemList"), $"p_type")
      .filter($"p_name".startsWith("PROMO"))
      .select(explode($"col"), $"p_type")
      .select($"col.l_discount", $"col.l_extendedprice", $"p_type")
      .filter($"col.l_shipdate" >= "1991-11-25"
        && $"col.l_shipdate" < "1994-01-04")
      .distinct().count()
  }

}
