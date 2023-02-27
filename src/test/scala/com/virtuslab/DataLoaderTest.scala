package com.virtuslab

import com.virtuslab.test.ResourceUtils.getResourcePaths
import com.virtuslab.test.SparkTestSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataLoaderTest extends SparkTestSpec {

  it should "load the data" in {
    val input = DataLoader
      .loadDataUsingListOfFiles(getResourcePaths("test_tsv.csv"): _*)
    input.collect().toSeq should contain theSameElementsAs Seq(
      "3\t4",
      "2\t5",
      "2\t5",
      "3\t1",
      "3\t6",
      "3\t6",
      "8\t"
    )
  }

  it should "skip header" in {
    val resources = getResourcePaths("header_test.csv", "header_test.tsv")
    val input = DataLoader
      .loadDataUsingListOfFiles(resources: _*)
    input.collect().toSeq should contain theSameElementsAs Seq(
      "1,1",
      "2\t2"
    )
  }

}
