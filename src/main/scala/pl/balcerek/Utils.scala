package pl.balcerek

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Utils {

  def readCSV(name: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/" + name)
  }

}
