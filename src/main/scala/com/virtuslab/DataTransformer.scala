package com.virtuslab

import com.virtuslab.model.IntKeyValuePair
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

object DataTransformer {

  def filterOddOccurrencesOfKeyValuePair(input: Dataset[IntKeyValuePair])(implicit spark: SparkSession): Dataset[IntKeyValuePair] = {
    import spark.implicits._
    input
      .withColumn("count", lit(1))
      .groupBy("key", "value")
      .sum("count")
      .where(col("sum(count)") % 2 === 1)
      .select("key", "value")
      .as[IntKeyValuePair]
  }


}
