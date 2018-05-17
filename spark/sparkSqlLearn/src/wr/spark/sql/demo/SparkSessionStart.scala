package wr.spark.sql.demo

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by spark on 5/16/18.
 */
object SparkSessionStart {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

    val addressRDD=spark.sparkContext
    .makeRDD(Seq("1,First Street","2,Second Street","3,Third Street"))
    .map(_.split(","))
    .map(xs => Row(xs(0).toInt,xs(1).trim))

    val schema=StructType(Array(
    StructField("id",IntegerType,nullable = false),
    StructField("street",StringType,nullable = false)
    ))

    val addressDF=spark.createDataFrame(addressRDD,schema)
  }

}
