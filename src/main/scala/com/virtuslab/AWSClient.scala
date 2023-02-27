package com.virtuslab

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import com.amazonaws.services.s3.iterable.S3Objects
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.IteratorHasAsScala

class AWSClient(profileName: Option[String], region: Regions = Regions.US_EAST_1) {

  private val credentialsProvider: AWSCredentialsProvider =
    profileName
      .map(new ProfileCredentialsProvider(_))
      .getOrElse(DefaultAWSCredentialsProviderChain.getInstance())

  private val s3Client =
    AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(credentialsProvider).build

  def configureSparkWithS3(spark: SparkSession): Unit = {
    val credentials = credentialsProvider.getCredentials
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  }

  def listS3FilesForSpark(s3URI: AmazonS3URI): Seq[String] = {
    val bucket = s3URI.getBucket
    S3Objects
      .withPrefix(s3Client, bucket, s3URI.getKey)
      .iterator()
      .asScala
      .toList
      .map(_.getKey)
      .filterNot(_.endsWith("/"))
      .map(path => s"s3a://$bucket/$path")
  }

}

object AWSClient {
  def parseInputAndOutputURIs(input: String, output: String): (AmazonS3URI, AmazonS3URI) =
    (new AmazonS3URI(input), new AmazonS3URI(output))

  def s3URIToSparkPathString(s3URI: AmazonS3URI) = {
    s"s3a://${s3URI.getBucket}/${s3URI.getKey}"
  }
}
