package com.virtuslab.test

import org.apache.spark.sql.SparkSession

trait SparkInstance {

  implicit lazy val spark: SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 8)
      .config("spark.ui.enabled", false)
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
