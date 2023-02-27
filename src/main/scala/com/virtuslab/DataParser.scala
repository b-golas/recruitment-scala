package com.virtuslab

import com.virtuslab.model.IntKeyValuePair
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object DataParser {

  def parseData(input: RDD[String])(implicit spark: SparkSession): Dataset[IntKeyValuePair] = {
    import spark.implicits._
    parseTextData(normaliseDelimiter(input))
      .toDF("key", "value")
      .as[IntKeyValuePair]
  }

  def normaliseDelimiter(input: RDD[String]): RDD[String] =
    input
      .map(_.replace(",", "\t"))

  private[virtuslab] def parseTextData(input: RDD[String]): RDD[(Int, Int)] =
    input
      .map(text => {
        text.split("\\t") match {
          case Array(first) => (first.toInt, 0)
          case Array(first, second) if first.isEmpty => (0, second.toInt)
          case Array(first, second) => (first.toInt, second.toInt)
          case Array() => (0, 0)
        }
      })

}
