/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.core.model.{
  IndexStatus,
  QbeastSnapshot,
  ReplicatedSet,
  Revision,
  RevisionID,
  mapper
}
import io.qbeast.spark.utils.MetadataConfig
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.delta.Snapshot

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot the internal Delta Lakes log snapshot
 */
case class DeltaQbeastSnapshot(snapshot: Snapshot) extends QbeastSnapshot {

  def isInitial: Boolean = snapshot.version == -1

  private val metadataMap: Map[String, String] = snapshot.metadata.configuration

  /**
   * Constructs revision dictionary
   *
   * @return a map of revision identifier and revision
   */
  private val revisionsMap: Map[RevisionID, Revision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))
    listRevisions.map { case (key: String, json: String) =>
      val revisionID = key.split('.').last.toLong
      val revision = mapper
        .readValue[Revision](json, classOf[Revision])
      (revisionID, revision)
    }
  }

  /**
   * Constructs replicated set for each revision
   *
   * @return a map of revision identifier and replicated set
   */
  private val replicatedSetsMap: Map[RevisionID, ReplicatedSet] = {
    val listReplicatedSets = metadataMap.filterKeys(_.startsWith(MetadataConfig.replicatedSet))

    listReplicatedSets.map { case (key: String, json: String) =>
      val revisionID = key.split('.').last.toLong
      val revision = getRevision(revisionID)
      val replicatedSet = mapper
        .readValue[Set[String]](json, classOf[Set[String]])
        .map(revision.createCubeId)
      (revisionID, replicatedSet)
    }
  }

  /**
   * Returns last available revision identifier
   *
   * @return revision identifier
   */
  private val lastRevisionID: RevisionID =
    metadataMap.getOrElse(MetadataConfig.lastRevisionID, "-1").toLong

  /**
   * Looks up for a revision with a certain identifier
   *
   * @param revisionID the ID of the revision
   * @return revision information for the corresponding identifier
   */
  private def getRevision(revisionID: RevisionID): Revision = {
    revisionsMap
      .getOrElse(
        revisionID,
        throw AnalysisExceptionFactory.create(s"No space revision available with $revisionID"))
  }

  /**
   * Returns the replicated set for a revision identifier if exists
   * @param revisionID the revision identifier
   * @return the replicated set
   */
  private def getReplicatedSet(revisionID: RevisionID): ReplicatedSet = {
    replicatedSetsMap
      .getOrElse(revisionID, Set.empty)
  }

  /**
   * Returns true if a revision with a specific revision identifier exists
   *
   * @param revisionID the identifier of the revision
   * @return boolean
   */
  def existsRevision(revisionID: RevisionID): Boolean = {
    revisionsMap.contains(revisionID)
  }

  /**
   * Obtains the latest IndexStatus for a given QTableID
   *
   * @return the latest IndexStatus for qtable
   */
  override def loadLatestIndexStatus: IndexStatus = {
    val revision = getRevision(lastRevisionID)
    val replicatedSet = getReplicatedSet(lastRevisionID)
    new IndexStatusBuilder(this, revision, replicatedSet).build()
  }

  /**
   * Obtains the latest IndexStatus for a given RevisionID
   *
   * @param revisionID the RevisionID
   * @return
   */
  override def loadIndexStatus(revisionID: RevisionID): IndexStatus = {
    val revision = getRevision(revisionID)
    val replicatedSet = getReplicatedSet(lastRevisionID)
    new IndexStatusBuilder(this, revision, replicatedSet).build()
  }

  /**
   * Obtain all Revisions for a given QTableID
   *
   * @return an immutable Seq of Revision for qtable
   */
  override def loadAllRevisions: IISeq[Revision] = revisionsMap.values.toVector

  /**
   * Obtain the last Revisions
   *
   * @return an immutable Seq of Revision for qtable
   */
  override def loadLatestRevision: Revision = {
    getRevision(lastRevisionID)
  }

  /**
   * Obtain the IndexStatus for a given RevisionID
   *
   * @param revisionID the RevisionID
   * @return the IndexStatus for revisionID
   */
  override def loadRevision(revisionID: RevisionID): Revision = {
    getRevision(revisionID)
  }

  /**
   * Loads the most updated revision at a given timestamp
   *
   * @param timestamp the timestamp in Long format
   * @return the latest Revision at a concrete timestamp
   */
  override def loadRevisionAt(timestamp: Long): Revision = {
    revisionsMap.values.find(_.timestamp <= timestamp).getOrElse {
      throw AnalysisExceptionFactory.create(s"No space revision available before $timestamp")
    }
  }

}
