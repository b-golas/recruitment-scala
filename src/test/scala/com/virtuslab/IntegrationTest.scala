package com.virtuslab

import com.virtuslab.DataParser.parseData
import com.virtuslab.DataTransformer.filterOddOccurrencesOfKeyValuePair
import com.virtuslab.model.IntKeyValuePair
import com.virtuslab.test.ResourceUtils.getResourcePaths
import com.virtuslab.test.SparkTestSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class IntegrationTest extends SparkTestSpec {

  it should "get correct result" in {
    val resources = getResourcePaths("test_1.csv", "test_tsv.csv", "test_2.csv")
    val input = DataLoader.loadDataUsingListOfFiles(resources: _*)
    val result = filterOddOccurrencesOfKeyValuePair(parseData(input))
    result.collect().toSeq should contain theSameElementsAs Seq(
      IntKeyValuePair(7, 0),
      IntKeyValuePair(8, 0),
      IntKeyValuePair(6, 1)
    )
  }

}
