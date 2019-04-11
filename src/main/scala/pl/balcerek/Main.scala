
package pl.balcerek

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


object Main extends App with SparkConfig with Utils {

  import spark.implicits._

  val frame = readCSV("test.csv", spark).as[Person]

  frame.foreach(println(_))

  frame.createOrReplaceTempView("people")
  val countSql = spark.sql("select count(*) from people")

  frame.show()

  countSql.explain(true)

  val countDsl: Dataset[Int] = frame.select($"age").as
  countDsl.explain(true)
  countDsl.show()

  frame.write
    .format("hive")
    .option("fileFormat", "parquet")
    .mode(SaveMode.Overwrite)
    .partitionBy(/*"country", */"age")
//    .format("hive")
    .saveAsTable("people_impala_table")

  spark.read
    .table("people_impala_table")
    .foreachPartition(it => {
      var message = "partition: \n"
      it.foreach(r => message = message + r + "\n")
      println(message)
    })

  spark.read
    .parquet("/home/adam/projects/spark-gradle/spark-warehouse/people_impala_table")
    .show()

  readCSV("zip_code_database.csv", spark).show()

}
