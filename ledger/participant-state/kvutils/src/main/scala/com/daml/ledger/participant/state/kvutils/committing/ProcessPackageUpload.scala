// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.{Err, Pretty}
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.breakOut

private[kvutils] case class ProcessPackageUpload(
    entryId: DamlLogEntryId,
    recordTime: Timestamp,
    packageUploadEntry: DamlPackageUploadEntry,
    inputState: Map[DamlStateKey, Option[DamlStateValue]]) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val submissionId = packageUploadEntry.getSubmissionId
  private val archives = packageUploadEntry.getArchivesList.asScala

  private def tracelog(msg: String): Unit =
    logger.trace(
      s"""[entryId=${Pretty.prettyEntryId(entryId)}, submId=$submissionId], packages=${archives
        .map(_.getHash)
        .mkString(",")}]: $msg""")

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {
    // TODO: Add more comprehensive validity test, in particular, take the transitive closure
    // of all packages being uploaded and see if they compile
    archives.foldLeft[(Boolean, String)]((true, ""))(
      (acc, archive) =>
        if (archive.getPayload.isEmpty) (false, acc._2 ++ s"empty package '${archive.getHash}';")
        else acc) match {

      case (false, error) =>
        tracelog(s"Package upload failed, invalid package submitted")
        buildPackageRejectionLogEntry(
          recordTime,
          packageUploadEntry,
          _.setInvalidPackage(
            DamlPackageUploadRejectionEntry.InvalidPackage.newBuilder
              .setDetails(error)))
      case (_, _) =>
        val filteredArchives = archives
          .filter { archive =>
            val stateKey = DamlStateKey.newBuilder
              .setPackageId(archive.getHash)
              .build
            inputState
              .getOrElse(stateKey, throw Err.MissingInputState(stateKey))
              .isEmpty
          }
        tracelog(s"Packages committed")
        (
          DamlLogEntry.newBuilder
            .setRecordTime(buildTimestamp(recordTime))
            .setPackageUploadEntry(
              DamlPackageUploadEntry.newBuilder
                .setSubmissionId(submissionId)
                .addAllArchives(filteredArchives.asJava)
                .setSourceDescription(packageUploadEntry.getSourceDescription)
                .setParticipantId(packageUploadEntry.getParticipantId)
                .build
            )
            .build,
          filteredArchives
            .map(
              archive =>
                (
                  DamlStateKey.newBuilder.setPackageId(archive.getHash).build,
                  DamlStateValue.newBuilder.setArchive(archive).build
              )
            )(breakOut)
        )
    }
  }

  private def buildPackageRejectionLogEntry(
      recordTime: Timestamp,
      packageUploadEntry: DamlPackageUploadEntry,
      addErrorDetails: DamlPackageUploadRejectionEntry.Builder => DamlPackageUploadRejectionEntry.Builder
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {
    (
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setPackageUploadRejectionEntry(
          addErrorDetails(
            DamlPackageUploadRejectionEntry.newBuilder
              .setSubmissionId(packageUploadEntry.getSubmissionId)
              .setParticipantId(packageUploadEntry.getParticipantId)
          ).build)
        .build,
      Map.empty
    )
  }

}
