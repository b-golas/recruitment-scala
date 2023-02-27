package com.virtuslab

import com.virtuslab.DataTransformer.filterOddOccurrencesOfKeyValuePair
import com.virtuslab.model.IntKeyValuePair
import com.virtuslab.test.SparkTestSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataTransformerTest extends SparkTestSpec {

  import spark.implicits._

  it should "filter for odd key values pairs" in {
    val input = Seq(
      IntKeyValuePair(1, 1),
      IntKeyValuePair(2, 2),
      IntKeyValuePair(2, 2),
      IntKeyValuePair(3, 3),
      IntKeyValuePair(3, 3),
      IntKeyValuePair(3, 3),
      IntKeyValuePair(3, 3),
      IntKeyValuePair(4, 4),
      IntKeyValuePair(4, 4),
      IntKeyValuePair(4, 4),
    ).toDS

    filterOddOccurrencesOfKeyValuePair(input).collect().toSeq should contain theSameElementsAs Seq(
      IntKeyValuePair(1, 1),
      IntKeyValuePair(4, 4)
    )
  }

}
