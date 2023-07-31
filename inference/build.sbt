ThisBuild / scalaVersion     := "2.12.14"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "edu.njit.cs643.dmg56"
ThisBuild / organizationName := "winequalityprediction"

lazy val root = (project in file("."))
  .settings(
    name := "wine-quality-prediction",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      "org.apache.spark" %% "spark-mllib" % "3.4.1",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4",
      "org.apache.hadoop" % "hadoop-common" % "3.3.4",
      "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.505",
      "com.amazonaws" % "aws-java-sdk" % "1.12.505",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % "provided",
      "net.java.dev.jets3t" % "jets3t" % "0.9.4",
      "software.amazon.awssdk" % "s3" % "2.16.71"
    )
  )