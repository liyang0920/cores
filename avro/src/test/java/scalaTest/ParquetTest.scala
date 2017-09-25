package scalaTest

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{sum, udf, explode}
import com.databricks.spark.avro._

/**
  * Created by michael on 9/18/17.
  */
abstract class ParquetTest{

}

object ParquetTest {

  def executeQuery(queryNo: Int, countNo: Int): Unit = {
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ParquetTest")
      //.config("spark.main.scala.cores", "parquettest")
      .getOrCreate()

    runSchema(spark)

    runSimple(spark)

    runSelect(spark)

    runComplex(spark)

    runPseudo1(spark)

    runPseudo2(spark)

    runPseudo3(spark)

    runPseudo4(spark)

    runPlan1(spark)

    runPlan2(spark)

    runJsonSchema(spark)

    runJsonPeudo1(spark)

    runAvroSchema(spark)

    runAvroPeudo1(spark)

    runAvroPlan1(spark)

    runJsonPlan1(spark)

    spark.stop()
  }

  private def runJsonSchema(spark: SparkSession): Unit = {
    val pq = spark.read.json("src/resources/result.json")
    pq.printSchema()
  }

  private def runJsonPeudo1(spark: SparkSession) : Unit = {
    val pq = spark.read.json("src/resources/result.json")
    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    import spark.implicits._

    pq.filter(forest($"p_name")).select(explode($"PartsuppList.LineitemList"), $"PartsuppList.ps_suppkey",
      $"PartsuppList.ps_availqty")
      .select(explode($"col"), $"ps_suppkey", $"ps_availqty")
      .filter($"col.l_shipdate" >= "1993-05-01"
        && $"col.l_shipdate" < "1994-01-01")
      .select($"ps_suppkey", $"ps_availqty").distinct()
  }

  private def runJsonPlan1(spark: SparkSession) : Unit = {
    val pq = spark.read.json("src/resources/result.json")
    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    import spark.implicits._

    pq.select(explode($"PartsuppList.LineitemList"))
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_shipdate", $"col.l_quantity", $"col.l_extendedprice")
      .filter($"col.l_shipdate" >= "1992-01-01"
        && $"col.l_shipdate" < "1994-01-01"
        && $"col.l_discount" >= 0
        && $"col.l_discount" <= 0.04
        && $"col.l_quantity" < 40)
      .agg(sum($"l_extendedprice" * $"l_discount")).explain()
  }

  private def runAvroSchema(spark: SparkSession): Unit = {
    val pq = spark.read.format("com.databricks.spark.avro").load("src/resources/result.avro")
    pq.printSchema()
  }

  private def runAvroPeudo1(spark: SparkSession) : Unit = {
    val pq = spark.read.format("com.databricks.spark.avro").load("src/resources/result.avro")
    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    import spark.implicits._

    pq.filter(forest($"p_name")).select(explode($"PartsuppList.LineitemList"), $"PartsuppList.ps_suppkey",
      $"PartsuppList.ps_availqty")
      .select(explode($"col"), $"ps_suppkey", $"ps_availqty")
      .filter($"col.l_shipdate" >= "1993-05-01"
        && $"col.l_shipdate" < "1994-01-01")
      .select($"ps_suppkey", $"ps_availqty").distinct()
  }

  private def runAvroPlan1(spark: SparkSession) : Unit = {
    val pq = spark.read.format("com.databricks.spark.avro").load("src/resources/result.avro")
    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    import spark.implicits._

    pq.select(explode($"PartsuppList.LineitemList"))
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_shipdate", $"col.l_quantity", $"col.l_extendedprice")
      .filter($"col.l_shipdate" >= "1992-01-01"
        && $"col.l_shipdate" < "1994-01-01"
        && $"col.l_discount" >= 0
        && $"col.l_discount" <= 0.04
        && $"col.l_quantity" < 40)
      .agg(sum($"l_extendedprice" * $"l_discount")).explain()
  }

  private def runSchema(spark: SparkSession): Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")
    pq.printSchema()
  }

  private def runSimple(spark: SparkSession) : Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")

    import spark.implicits._

    pq.select($"p_partkey",
      $"PartsuppList.ps_partkey", $"PartsuppList.ps_suppkey")
      .show()
  }

  private def runSelect(spark: SparkSession) : Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")

    import spark.implicits._

    pq.select($"p_partkey",
      $"PartsuppList.ps_partkey", $"PartsuppList.ps_suppkey")
      //.foreach(pl => $"PartsuppList")
      .filter($"PartsuppList".getItem(0).getField("ps_partkey") < 5)
      .show()
  }

  private def runComplex(spark: SparkSession) : Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")

    import spark.implicits._

    val sf = udf {(pk: scala.collection.mutable.WrappedArray[String]) => (pk)}

    val pf = udf {(pk: scala.collection.mutable.WrappedArray[String]) => (pk exists (a =>  (a.toLong < 67735) ))}

    pq.select(explode($"PartsuppList.LineitemList"))
      .select(explode($"col"))
      .select($"col.l_partkey", $"col.l_suppkey")
      .filter($"col.l_partkey" < 5).distinct()
      .show(false)
  }

  private def runPseudo1(spark: SparkSession) : Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")

    import spark.implicits._

    pq.select(explode($"PartsuppList.LineitemList"))
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_shipdate", $"col.l_quantity", $"col.l_extendedprice")
      .filter($"col.l_shipdate" >= "1992-01-01"
        && $"col.l_shipdate" < "1994-01-01"
        && $"col.l_discount" >= 0
        && $"col.l_discount" <= 0.04
        && $"col.l_quantity" < 40)
      .agg(sum($"l_extendedprice" * $"l_discount"))
  }

  private def runPseudo2(spark: SparkSession) : Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")

    import spark.implicits._

    pq.select(explode($"PartsuppList.LineitemList"), $"p_type")
      .filter($"p_name".startsWith("PROMO"))
      .select(explode($"col"), $"p_type")
      .select($"col.l_discount", $"col.l_extendedprice", $"p_type")
      .filter($"col.l_shipdate" >= "1993-05-01"
        && $"col.l_shipdate" < "1994-01-01")
      .distinct()
  }

  private def runPseudo3(spark: SparkSession) : Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")
    val sm = udf { (x: String) => x.matches("SM|MED|LG|WRAP") }

    import spark.implicits._

    pq.select(explode($"PartsuppList.LineitemList"))
      .filter(
        (($"p_brand" === "Brand#50") &&
          sm($"p_container") && $"p_size" <= 50) )
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_extendedprice")
      .filter(($"col.l_shipmode" === "TRUCK" || $"col.l_shipmode" === "AIR" || $"col.l_shipmode" === "SHIP" || $"col.l_shipmode" === "FOB") &&
        $"col.l_shipinstruct" === "DELIVER IN PERSON" && $"col.l_quantity" >= 0 && $"col.l_quantity" <= 50)
      .select($"l_extendedprice", $"l_discount").distinct()
  }

  private def runPseudo4(spark: SparkSession) : Unit = {
    val pq = spark.read.parquet("src/resources/result.parquet")
    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    import spark.implicits._

    pq.filter(forest($"p_name")).select(explode($"PartsuppList.LineitemList"), $"PartsuppList.ps_suppkey",
      $"PartsuppList.ps_availqty")
      .select(explode($"col"), $"ps_suppkey", $"ps_availqty")
      .filter($"col.l_shipdate" >= "1993-05-01"
        && $"col.l_shipdate" < "1994-01-01")
      .select($"ps_suppkey", $"ps_availqty").distinct()
  }

  private def runPlan1(spark: SparkSession) : Unit = {
    spark.sql("SET spark.sql.parquet.filterPushdown=true")
    val pq = spark.read.parquet("src/resources/result.parquet").registerTempTable("nestedpart")
    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    import spark.implicits._

    spark.sql("SET spark.sql.parquet.filterPushdown=true")
    spark.sql("SELECT p_partkey FROM nestedpart WHERE p_partkey < 5").explain()

    //pq.filter($"p_partkey" < 5).select($"p_partkey").explain(true)
  }

  private def runPlan2(spark: SparkSession) : Unit = {
    spark.sql("SET spark.sql.parquet.filterPushdown=true")
    val pq = spark.read.parquet("src/resources/result.parquet")
    val forest = udf { (x: String) => x.contains("green|lemon|red") }

    import spark.implicits._

    pq.filter(forest($"p_name")).select(explode($"PartsuppList.LineitemList"), $"PartsuppList.ps_suppkey",
      $"PartsuppList.ps_availqty")
      .select(explode($"col"), $"ps_suppkey", $"ps_availqty")
      .filter($"col.l_shipdate" >= "1993-05-01"
        && $"col.l_shipdate" < "1994-01-01")
      .select($"ps_suppkey", $"ps_availqty").distinct().explain()
  }
}
