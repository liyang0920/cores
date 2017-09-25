package scalaTest

import org.apache.spark.sql.functions.{sum, udf}

/**
  * TPC-H Query 19
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q19raw1 extends TpchQueryRaw {

  import spark.implicits._

  override def execute(): Unit = {

    val sm = udf { (x: String) => x.matches("SM|MED|LG|WRAP") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    val res = part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "TRUCK" || $"l_shipmode" === "AIR" || $"l_shipmode" === "SHIP" || $"l_shipmode" === "FOB") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#50") &&
          sm($"p_container") &&
          $"l_quantity" >= 0 && $"l_quantity" <= 50 && $"p_size" <= 50) )
      .select($"l_extendedprice", $"l_discount").count()

  }

}
