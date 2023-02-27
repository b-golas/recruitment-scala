name := "recruitment-scala"

version := "0.1"

scalaVersion := "2.13.10"

val sparkVersion = "3.3.2"
val scalaTestVersion = "3.2.15"
val awsSdkVersion = "1.11.698"
val hadoopVersion = "3.3.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "com.amazonaws" % "aws-java-sdk" % awsSdkVersion
)