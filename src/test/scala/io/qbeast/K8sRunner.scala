package io.qbeast

import io.qbeast.context.QbeastContextTest
import io.qbeast.spark.delta.{OTreeIndexTest, QbeastSnapshotTest}
import io.qbeast.spark.index.writer.{BlockWriterTest, SparkDataWriterTest}
import io.qbeast.spark.index.{
  AnalyzeAndOptimizeTest,
  ColumnsToIndexTest,
  IndexTest,
  MaxWeightEstimationTest,
  NewRevisionTest,
  NormalizedWeightIntegrationTest,
  OTreeAlgorithmTest,
  QbeastColumnsTest,
  RevisionTest,
  SparkPointWeightIndexerTest,
  TransformerIndexingTest
}
import io.qbeast.spark.internal.QbeastSparkSessionExtension
import io.qbeast.spark.internal.sources.QbeastDataSourceTest
import io.qbeast.spark.utils.QbeastDataSourceIntegrationTest
import org.apache.spark.sql.SparkSession
import org.scalatest.run

object K8sRunner {
  protected var sparkMaster: String = "local[*]"
  protected var AzureBlobStorageKey: String = ""
  var isWasb: Boolean = false

  private def createSparkSession(): SparkSession = {
    printf("\nCREATING SPARK SESSION\n")
    SparkSession
      .builder()
      .master(sparkMaster)
      .appName("QbeastDataSource")
      .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-azure:3.2.0,io.delta:delta-core_2.12:1.0.0")
      .config(
        "spark.hadoop.fs.azure.account.key.blobqsql.blob.core.windows.net",
        AzureBlobStorageKey)
      .config("spark.eventLog.enabled", true)
      .config("spark.eventLog.dir", "wasb://tpc-ds@blobqsql.blob.core.windows.net/spark-logs")
      .withExtensions(new QbeastSparkSessionExtension())
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    try {
      // Process arguments
      for (a <- args) {
        if (a.startsWith("--master=")) {
          sparkMaster = a.substring("--master=".length)
        }
        if (a.startsWith("--azure-key=")) {
          AzureBlobStorageKey = a.substring("--azure-key=".length)
          isWasb = true
        }
      }

      // Create new spark session with proper config
      createSparkSession()

      // Run tests
      printf("Running tests!\n")
      run(new QbeastContextTest())

      // Tests for io.qbeast.context
      run(new QbeastContextTest())

      // Tests for io.qbeast.spark.delta
      run(new OTreeIndexTest())
      run(new QbeastSnapshotTest())

      // Tests for io.qbeast.spark.index
      run(new AnalyzeAndOptimizeTest())
      run(new ColumnsToIndexTest())
      run(new IndexTest())
      run(new MaxWeightEstimationTest())
      run(new NewRevisionTest())
      run(new NormalizedWeightIntegrationTest())
      run(new OTreeAlgorithmTest())
      run(new QbeastColumnsTest())
      run(new RevisionTest())
      run(new SparkPointWeightIndexerTest())
      run(new TransformerIndexingTest())

      // Tests for io.qbeast.spark.index.writer
      run(new BlockWriterTest())
      run(new SparkDataWriterTest())

      // Tests for io.qbeast.spark.internal.sources
      run(new QbeastDataSourceTest())

      // Tests for io.qbeast.spark.utils
      run(new QbeastDataSourceIntegrationTest())

      printf("Tests finished!\n")
    } finally {
      SparkSession.active.stop()
    }
  }

}
