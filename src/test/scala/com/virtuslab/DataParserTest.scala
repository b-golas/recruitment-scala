package com.virtuslab

import com.virtuslab.DataParser.{normaliseDelimiter, parseTextData}
import com.virtuslab.test.SparkTestSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataParserTest extends SparkTestSpec {

  import spark.implicits._

  it should "normalise a delimiter" in {
    val input = Seq(
      "1,1",
      "2\t2",
      "3,3"
    ).toDF().as[String].rdd

    normaliseDelimiter(input).collect().toSeq should contain theSameElementsAs Seq(
      "1\t1",
      "2\t2",
      "3\t3"
    )
  }

  it should "parse text data" in {
    val input = Seq(
      "1\t1",
      "4\t4",
      "5\t",
      "\t5",
      "\t"
    ).toDF().as[String].rdd

    parseTextData(input).collect().toSeq should contain theSameElementsAs Seq(
      (1, 1),
      (4, 4),
      (5, 0),
      (0, 5),
      (0, 0)
    )
  }

}
