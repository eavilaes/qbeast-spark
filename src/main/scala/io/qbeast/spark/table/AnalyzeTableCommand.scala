/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.spark.model.RevisionID
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 * The Analyze Table command implementation
 * @param revisionID the identifier of the revision to optimize
 * @param indexedTable indexed table to analyze
 */
case class AnalyzeTableCommand(revisionID: RevisionID, indexedTable: IndexedTable)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.analyze(revisionID).map(r => Row.fromSeq(Seq(r)))

  }

}
