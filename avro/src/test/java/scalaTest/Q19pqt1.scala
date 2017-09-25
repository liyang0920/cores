package scalaTest

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q19pqt1 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int): Unit = {
    init(path, typeId)
    val sm = udf { (x: String) => x.matches("SM|MED|LG|WRAP") }

    val res = nested.select(explode($"PartsuppList.LineitemList"))
      .filter(
        (($"p_brand" === "Brand#50") &&
          sm($"p_container") && $"p_size" <= 50) )
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_extendedprice")
      .filter(($"col.l_shipmode" === "TRUCK" || $"col.l_shipmode" === "AIR" || $"col.l_shipmode" === "SHIP" || $"col.l_shipmode" === "FOB") &&
        $"col.l_shipinstruct" === "DELIVER IN PERSON" && $"col.l_quantity" >= 0 && $"col.l_quantity" <= 50)
      .select($"l_extendedprice", $"l_discount").count()
  }

}
