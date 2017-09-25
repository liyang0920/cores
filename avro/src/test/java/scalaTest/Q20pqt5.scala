package scalaTest

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q20pqt5 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int): Unit = {
    init(path, typeId)
    val forest = udf { (x: String) => x.contains("hot sandy") }

    val res = nested.filter(forest($"p_name")).select(explode($"PartsuppList.LineitemList"), $"PartsuppList.ps_suppkey",
      $"PartsuppList.ps_availqty")
      .select(explode($"col"), $"ps_suppkey", $"ps_availqty")
      .filter($"col.l_shipdate" >= "1992-10-24"
        && $"col.l_shipdate" < "1993-12-10")
      .select($"ps_suppkey", $"ps_availqty").count()
  }
}
