package scalaTest

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q19pqt4 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int): Unit = {
    init(path, typeId)
    val sm = udf { (x: String) => x.matches("SM") }

    val res = nested.select(explode($"PartsuppList.LineitemList"))
      .filter(
        (($"p_brand" === "Brand#50") &&
          sm($"p_container") && $"p_size" <= 20) )
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_extendedprice")
      .filter(($"col.l_shipmode" === "TRUCK") &&
        $"col.l_shipinstruct" === "DELIVER IN PERSON" && $"col.l_quantity" >= 10 && $"col.l_quantity" <= 20)
      .select($"l_extendedprice", $"l_discount").count()
  }

}
