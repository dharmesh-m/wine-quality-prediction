ThisBuild / scalaVersion     := "2.13.11"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "org.apache.spark"
ThisBuild / organizationName := "winequalityprediction"

lazy val root = (project in file("."))
  .settings(
    name := "wine-quality-prediction"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
