package com.virtuslab

import com.virtuslab.DataParser.{normaliseDelimiter, parseTextData}
import com.virtuslab.model.IntKeyValuePair
import com.virtuslab.test.SparkTestSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataParserTest extends SparkTestSpec {

  it should "normalise a delimiter" in {
    val input = spark.sparkContext.parallelize(Seq(
      "1,1",
      "2\t2",
      "3,3"
    ))

    normaliseDelimiter(input).collect().toSeq should contain theSameElementsAs Seq(
      "1\t1",
      "2\t2",
      "3\t3"
    )
  }

  it should "parse text data" in {
    val input = spark.sparkContext.parallelize(Seq(
      "1\t1",
      "4\t4",
      "5\t",
      "\t5",
      "\t"
    ))

    parseTextData(input).collect().toSeq should contain theSameElementsAs Seq(
      IntKeyValuePair(1, 1),
      IntKeyValuePair(4, 4),
      IntKeyValuePair(5, 0),
      IntKeyValuePair(0, 5),
      IntKeyValuePair(0, 0)
    )
  }

}
