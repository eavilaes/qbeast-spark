/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.core.model.CubeId
import io.qbeast.K8sRunner
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import io.qbeast.spark.delta.OTreeIndex
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class QbeastDataSourceIntegrationTest extends QbeastIntegrationTestSpec {

  private def loadTestData(spark: SparkSession): DataFrame = {
    var path = "ecommerce100K_2019_Oct.csv"
    if (K8sRunner.isWasb) {
      path = "wasb://datasets@blobqsql.blob.core.windows.net/" + path
    } else {
      path = "src/test/resources/" + path
    }
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      .distinct()
  }

  private def writeTestData(
      data: DataFrame,
      columnsToIndex: Seq[String],
      cubeSize: Int,
      tmpDir: String): Unit = {

    data.write
      .mode("overwrite")
      .format("qbeast")
      .options(
        Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString))
      .save(tmpDir)
  }

  "the Qbeast data source" should
    "expose the original number of columns and rows" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)
          writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

          val indexed = spark.read.format("qbeast").load(tmpDir)

          data.count() shouldBe indexed.count()

          assertLargeDatasetEquality(indexed, data, orderedComparison = false)

          data.columns.toSet shouldBe indexed.columns.toSet

        }
    }

  it should "index correctly on bigger spaces" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
          .withColumn("user_id", lit(col("user_id") * Long.MaxValue))
        // WRITE SOME DATA
        data.write
          .mode("overwrite")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val indexed = spark.read.format("qbeast").load(tmpDir)

        data.count() shouldBe indexed.count()

        assertLargeDatasetEquality(indexed, data, orderedComparison = false)

        data.columns.toSet shouldBe indexed.columns.toSet

      }
  }

  it should "index correctly on overwrite" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      val data = loadTestData(spark)
      // WRITE SOME DATA
      writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

      // OVERWRITE
      writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

      val indexed = spark.read.format("qbeast").load(tmpDir)

      data.count() shouldBe indexed.count()

      assertLargeDatasetEquality(indexed, data, orderedComparison = false)

      data.columns.toSet shouldBe indexed.columns.toSet

    }
  }
  it should
    "work with indexed columns within 0 and 1" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          import org.apache.spark.sql.functions._
          import spark.implicits._
          val data = loadTestData(spark)
          val stats = data
            .agg(
              max('user_id).as("max_user_id"),
              min('user_id).as("min_user_id"),
              max('product_id).as("max_product_id"),
              min('product_id).as("min_product_id"))
            .collect()
            .head
          val (max_user, min_user, max_p, min_p) = {
            (stats.getInt(0), stats.getInt(1), stats.getInt(2), stats.getInt(3))
          }
          val norm_user = udf((v: Int) => (v - min_user).toDouble / (max_user - min_user))
          val norm_p = udf((v: Int) => (v - min_p).toDouble / (max_p - min_p))

          val normalizedData = data
            .withColumn("tmp_user_id", norm_user('user_id))
            .withColumn("tmp_norm_p", norm_p('product_id))
            .drop("user_id", "product_id")
            .withColumnRenamed("tmp_user_id", "user_id")
            .withColumnRenamed("tmp_norm_p", "product_id")

          writeTestData(normalizedData, Seq("user_id", "product_id"), 10000, tmpDir)

          val indexed = spark.read.format("qbeast").load(tmpDir)

          normalizedData.count() shouldBe indexed.count()

          assertLargeDatasetEquality(indexed, normalizedData, orderedComparison = false)

          normalizedData.columns.toSet shouldBe indexed.columns.toSet

        }
    }
  "the Qbeast data source" should
    "not replicate any point if there are optimizations" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)
          writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

          val indexed = spark.read.format("parquet").load(tmpDir)
          assertLargeDatasetEquality(indexed, data, orderedComparison = false)

        }
    }

  it should
    "filter the files to read" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val dfDelta = spark.read.format("delta").load(tmpDir)
        val precision = 0.1

        val query = df.sample(withReplacement = false, precision)
        val deltaQuery = dfDelta.sample(withReplacement = false, precision)

        val executionPlan = query.queryExecution.executedPlan.collectLeaves()

        executionPlan.exists(p =>
          p
            .asInstanceOf[FileSourceScanExec]
            .relation
            .location
            .isInstanceOf[OTreeIndex]) shouldBe true

        val filesDeltaQuery =
          deltaQuery
            .withColumn("input_file", input_file_name())
            .select("input_file")
            .distinct()
            .collect()
        val filesQbeastQuery = query
          .withColumn("input_file", input_file_name())
          .select("input_file")
          .distinct()
          .collect()

        filesQbeastQuery.length shouldBe <=(filesDeltaQuery.length)
        filesQbeastQuery.foreach(f => filesDeltaQuery should contain(f))

      }
    }

  it should
    "return a valid sample of the original dataset" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)

          writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

          val df = spark.read.format("qbeast").load(tmpDir)
          val dataSize = data.count()
          // We allow a 1% of tolerance in the sampling
          val tolerance = 0.01

          List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(precision => {
            val result = df
              .sample(withReplacement = false, precision)
              .count()
              .toDouble

            result shouldBe (dataSize * precision) +- dataSize * precision * tolerance
          })

          // Testing collect() method
          df.sample(withReplacement = false, 0.1)
            .collect()
            .length
            .toDouble shouldBe (dataSize * 0.1) +- dataSize * 0.1 * tolerance

          data.columns.toSet shouldBe df.columns.toSet

        }
    }

  it should
    "append data to the original dataset" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        val columnsToIndex = Seq("user_id", "product_id")
        val cubeSize = 10000
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        var path = "ecommerce300k_2019_Nov.csv"
        if (K8sRunner.isWasb) {
          path = "wasb://datasets@blobqsql.blob.core.windows.net/" + path
        } else {
          path = "src/test/resources/" + path
        }
        val appendData = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(path)

        appendData.write
          .mode("append")
          .format("qbeast")
          .options(
            Map(
              "columnsToIndex" -> columnsToIndex.mkString(","),
              "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val dataSize = data.count() + appendData.count()

        df.count() shouldBe dataSize

        val precision = 0.1
        val tolerance = 0.01
        // We allow a 1% of tolerance in the sampling
        df.sample(withReplacement = false, precision)
          .count()
          .toDouble shouldBe (dataSize * precision) +- dataSize * precision * tolerance

      }
    }

  def optimize(spark: SparkSession, tmpDir: String, times: Int): Unit = {
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    (0 until times).foreach(_ => { qbeastTable.analyze(); qbeastTable.optimize() })

  }

  "An optimized index" should "sample correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)
        val dataSize = data.count()

        df.count() shouldBe dataSize

        val tolerance = 0.01
        List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(precision => {
          val result = df
            .sample(withReplacement = false, precision)
            .count()
            .toDouble

          result shouldBe (dataSize * precision) +- dataSize * precision * tolerance
        })
      }
  }

  it should "erase cube information when overwritten" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        // val tmpDir = "/tmp/qbeast3"
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)

        // Overwrite table
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)

        qbeastTable.analyze() shouldBe Seq(CubeId.root(2).string)

      }
  }

}
