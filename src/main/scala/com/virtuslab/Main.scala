package com.virtuslab

import com.amazonaws.services.s3.AmazonS3URI
import com.virtuslab.model.IntKeyValuePair
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

object Main {

  // Those numbers depend on average input and output data size
  val processingPartitionCount = 20
  val outputPartitionCount = 1

  implicit lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val (inputPath: String, outputPath: String, profile: Option[String]) = args match {
      case Array(inputPath, outputPath) => (inputPath, outputPath, None)
      case Array(inputPath, outputPath, profile) => (inputPath, outputPath, Some(profile))
      case _ => throw new RuntimeException("You need to provide input and output S3 paths. Optionally you can provide an AWS profile name")
    }

    Try(AWSClient.parseInputAndOutputURIs(inputPath, outputPath)) match {
      case Success((inputURI: AmazonS3URI, outputURI: AmazonS3URI)) =>
        Try(new AWSClient(profile)) match {
          case Success(awsClient: AWSClient) => loadProcessAndSaveTheData(inputURI, outputURI, awsClient)
          case Failure(exception) => throw new RuntimeException("Failure during creation of AWSClient", exception)
        }
      case Failure(exception) => throw new RuntimeException("Failure during parsing of S3 paths", exception)
    }
  }

  def loadProcessAndSaveTheData(inputURI: AmazonS3URI, outputURI: AmazonS3URI, awsClient: AWSClient): Unit = {
    import spark.implicits._

    awsClient.configureSparkWithS3(spark)

    val input: RDD[String] = DataLoader
      .loadDataUsingListOfFiles(
        awsClient.listS3FilesForSpark(inputURI): _*
      )
      .repartition(processingPartitionCount)

    val parsedData: Dataset[IntKeyValuePair] = DataParser.parseData(input)

    val data: Dataset[IntKeyValuePair] = DataTransformer.filterOddOccurrencesOfKeyValuePair(parsedData)
      .repartition(outputPartitionCount)

    data
      .write
      .mode("overwrite")
      .option("delimiter", "\t")
      .csv(AWSClient.s3URIToSparkPathString(outputURI))
  }

}