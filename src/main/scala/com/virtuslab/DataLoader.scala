package com.virtuslab

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataLoader {

  def loadDataUsingListOfFiles(paths: String*)(implicit spark: SparkSession): RDD[String] =
    paths.map(loadSingleFileWithoutHeader _).reduce(_ union _)

  private def loadSingleFileWithoutHeader(path: String)(implicit spark: SparkSession): RDD[String] =
    spark
      .sparkContext
      .textFile(path)
      .filter(_.nonEmpty)
      .mapPartitionsWithIndex((index: Int, iterator: Iterator[String]) => if (index == 0) iterator.drop(1) else iterator)

}
