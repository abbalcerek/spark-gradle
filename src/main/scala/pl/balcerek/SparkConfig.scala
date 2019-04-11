package pl.balcerek

import org.apache.spark.sql.SparkSession

trait SparkConfig {

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark example")
    .master("local[*]")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

}
