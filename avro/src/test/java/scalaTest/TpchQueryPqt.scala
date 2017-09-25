package scalaTest

import org.apache.spark.sql._

/**
  * Created by michael on 9/18/17.
  */
abstract class TpchQueryPqt {
  val INPUT_DIR: String = "hdfs://11.11.11.71:9000/warehouse/tpch"//"/dbgen"

  // if set write results to hdfs, if null write to stdout
  // val OUTPUT_DIR: String = "/tpch"
  val OUTPUT_DIR: String = null

  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  val spark = SparkSession.builder().appName("TPC-H " + className).getOrCreate()

  var nested: DataFrame = null // = spark.read.parquet(INPUT_DIR + "/result.parquet")

  def execute(path: String, typeId: Int): Unit

  def init(path: String, typeId: Int): Unit = {
    if (typeId == 0)
      nested = spark.read.json(INPUT_DIR + "/" + path)
    else if (typeId == 1)
      nested = spark.read.format("com.databricks.spark.avro").load(INPUT_DIR + "/" + path)
    else
      nested = spark.read.parquet(INPUT_DIR + "/" + path)
  }

  def outputDF(df: DataFrame): Unit = {

    if (OUTPUT_DIR == null || OUTPUT_DIR == "")
      df.collect().foreach(println)
    else
      df.write.mode("overwrite").json(OUTPUT_DIR + "/" + className + ".out") // json to avoid alias
  }
}

object TpchQueryPqt {
  /**
    * Execute query reflectively
    */
  def executeQuery(queryNo: Int, countNo: Int, path: String, typeId: Int): Unit = {
    assert(queryNo >= 1 && queryNo <= 32, "Invalid query number")

    Class.forName(f"scalaTest.Q${queryNo}%02dpqt${countNo}%1d")
      .newInstance.asInstanceOf[{ def execute(path: String, typeId: Int) }].execute(path, typeId)
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 4)
      executeQuery(args(0).toInt, args(1).toInt, args(2), args(3).toInt)
    else
      throw new RuntimeException("Invalid number of arguments")
  }
}
