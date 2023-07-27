package org.apache.spark.winequalityprediction

object Hello extends Greeting with App {
  println(greeting)
}

trait Greeting {
  lazy val greeting: String = "wahe guru"
}
