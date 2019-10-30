// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import java.util.concurrent.{Executors, ThreadFactory}

import com.codahale.metrics
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.{Err, Pretty}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.Ast
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[kvutils] case class ProcessPackageUpload(
    engine: Engine,
    entryId: DamlLogEntryId,
    recordTime: Timestamp,
    packageUploadEntry: DamlPackageUploadEntry,
    inputState: Map[DamlStateKey, Option[DamlStateValue]]) {
  import ProcessPackageUpload._

  private val submissionId = packageUploadEntry.getSubmissionId
  private val logger =
    LoggerFactory.getLogger(
      s"ProcessPackageUpload[entryId=${Pretty.prettyEntryId(entryId)}, submId=${submissionId}]")
  private val archives = packageUploadEntry.getArchivesList.asScala

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = Metrics.runTimer.time { () =>
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

        // Queue the preloading of the package to the engine.
        serialContext.execute(preload)

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
            )
            .toMap
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

  private val preload: Runnable = () => {
    val ctx = Metrics.preloadTimer.time()
    try {
      logger.trace("Preloading engine...")
      val loadedPackages = engine.compiledPackages().packageIds
      val packages: Map[Ref.PackageId, Ast.Package] = Metrics.decodeTimer.time { () =>
        archives
          .filterNot(
            a =>
              Ref.PackageId
                .fromString(a.getHash)
                .fold(_ => false, loadedPackages.contains))
          .map { archive =>
            Decode.readArchiveAndVersion(archive)._1
          }
          .toMap
      }
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
      logger.trace(s"Preload complete.")
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.error("preload exception: $err")
    } finally {
      val _ = ctx.stop()
    }
  }
}

private[kvutils] object ProcessPackageUpload {
  private[committing] val serialContext = {
    Executors.newSingleThreadExecutor(new ThreadFactory {
      // Use a custom thread factory that creates daemon threads to avoid blocking the JVM from exiting.
      override def newThread(runnable: Runnable): Thread = {
        val t = new Thread(runnable)
        t.setDaemon(true)
        t
      }
    })
  }

  private[committing] object Metrics {
    private val registry = metrics.SharedMetricRegistries.getOrCreate("kvutils")
    private val prefix = "kvutils.committing.package"
    val runTimer: Timer = registry.timer(s"$prefix.run-timer")
    val preloadTimer: Timer = registry.timer(s"$prefix.preload-timer")
    val decodeTimer: Timer = registry.timer(s"$prefix.decode-timer")
    val accepts: Counter = registry.counter(s"$prefix.accepts")
  }
}
