// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import java.util.concurrent.TimeUnit

import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.{Err, Pretty}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.breakOut

private[kvutils] case class ProcessPackageUpload(
    engine: Engine,
    entryId: DamlLogEntryId,
    recordTime: Timestamp,
    packageUploadEntry: DamlPackageUploadEntry,
    inputState: Map[DamlStateKey, Option[DamlStateValue]]) {

  private val submissionId = packageUploadEntry.getSubmissionId
  private val logger =
    LoggerFactory.getLogger(
      s"ProcessPackageUpload[entryId=${Pretty.prettyEntryId(entryId)}, submId=${submissionId}]")

  private val archives = packageUploadEntry.getArchivesList.asScala

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {
    // TODO: Add more comprehensive validity test, in particular, take the transitive closure
    // of all packages being uploaded and see if they compile
    archives.foldLeft[(Boolean, String)]((true, ""))(
      (acc, archive) =>
        if (archive.getPayload.isEmpty) (false, acc._2 ++ s"empty package '${archive.getHash}';")
        else acc) match {

      case (false, error) =>
        logger.trace(s"Package upload failed, invalid package submitted: $error")
        buildPackageRejectionLogEntry(
          recordTime,
          packageUploadEntry,
          _.setInvalidPackage(
            DamlPackageUploadRejectionEntry.InvalidPackage.newBuilder
              .setDetails(error)))
      case (_, _) =>
        // Preload the engine.
        logger.trace("Preloading engine...")
        val t0 = System.nanoTime()
        val loadedPackages = engine.compiledPackages().packageIds
        val packages = Map(
          archives
            .filterNot(
              a =>
                Ref.PackageId
                  .fromString(a.getHash)
                  .fold(_ => false, loadedPackages.contains))
            .map { archive =>
              Decode.readArchiveAndVersion(archive)._1
            }: _*)
        val t1 = System.nanoTime()
        logger.trace(s"Decoding of ${packages.size} archives completed in ${TimeUnit.NANOSECONDS
          .toMillis(t1 - t0)}ms")
        packages.headOption.foreach {
          case (pkgId, pkg) =>
            engine
              .preloadPackage(pkgId, pkg)
              .consume(
                _ => sys.error("Unexpected request to PCS in preloadPackage"),
                pkgId => packages.get(pkgId),
                _ => sys.error("Unexpected request to keys in preloadPackage")
              )
        }
        val t2 = System.nanoTime()
        logger.trace(s"Preload completed in ${TimeUnit.NANOSECONDS.toMillis(t2 - t0)}ms")

        // Filter out archives that already exists.
        val filteredArchives = archives
          .filter { archive =>
            val stateKey = DamlStateKey.newBuilder
              .setPackageId(archive.getHash)
              .build
            inputState
              .getOrElse(stateKey, throw Err.MissingInputState(stateKey))
              .isEmpty
          }
        logger.trace(s"Packages committed: ${filteredArchives.map(_.getHash).mkString(", ")}")
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
