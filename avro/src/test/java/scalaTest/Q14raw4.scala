package scalaTest

import org.apache.spark.sql.functions.{sum, udf}

/**
  * TPC-H Query 14
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q14raw4 extends TpchQueryRaw {

  import spark.implicits._

  override def execute(): Unit = {

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    val res = part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1991-11-25" && $"l_shipdate" < "1994-01-09")
      .select($"p_type", $"l_extendedprice", $"l_discount").count()

  }

}
