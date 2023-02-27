package com.virtuslab

import com.virtuslab.model.IntKeyValuePair
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object DataParser {

  def parseData(input: RDD[String])(implicit spark: SparkSession): Dataset[IntKeyValuePair] = {
    import spark.implicits._
    parseTextData(normaliseDelimiter(input)).toDS
  }

  def normaliseDelimiter(input: RDD[String]): RDD[String] =
    input
      .map(_.replace(",", "\t"))

  private[virtuslab] def parseTextData(input: RDD[String]): RDD[IntKeyValuePair] =
    input
      .map(text => {
        text.split("\\t") match {
          case Array(first) => IntKeyValuePair(first.toInt, 0)
          case Array(first, second) if first.isEmpty => IntKeyValuePair(0, second.toInt)
          case Array(first, second) => IntKeyValuePair(first.toInt, second.toInt)
          case Array() => IntKeyValuePair(0, 0)
        }
      })

}
