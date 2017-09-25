package scalaTest

import org.apache.spark.sql.functions.{sum, udf}

/**
  * TPC-H Query 20
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q20raw1 extends TpchQueryRaw {

  import spark.implicits._

  override def execute(): Unit = {

    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    val res = part.filter(forest($"p_name"))
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(lineitem, $"ps_suppkey" === lineitem("l_suppkey") && $"ps_partkey" === lineitem("l_partkey"))
      .filter($"l_shipdate" >= "1992-01-01" && $"l_shipdate" < "1998-01-01")
      .select($"ps_suppkey", $"ps_availqty").count()

  }

}
