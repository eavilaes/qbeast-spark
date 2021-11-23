package io.qbeast.spark

import io.qbeast.spark.index.AnalyzeAndOptimizeTest
import org.scalatest.run

object K8sRunner {

  def main(args: Array[String]) {
    run(new AnalyzeAndOptimizeTest())
  }

}
