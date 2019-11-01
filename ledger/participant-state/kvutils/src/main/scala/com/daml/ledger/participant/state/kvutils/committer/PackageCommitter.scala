// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.util.concurrent.{Executors, ThreadFactory}
import com.codahale.metrics.{Counter, Gauge, Timer}
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import scala.collection.JavaConverters._

private[kvutils] case class PackageCommitter(engine: Engine)
    extends Committer[DamlPackageUploadEntry, DamlPackageUploadEntry.Builder] {
  private object Metrics {
    val preloadTimer: Timer = metricsRegistry.timer(metricsName("preload-timer"))
    val decodeTimer: Timer = metricsRegistry.timer(metricsName("decode-timer"))
    val accepts: Counter = metricsRegistry.counter(metricsName("accepts"))
    metricsRegistry.register(metricsName("loaded-packages"), new Gauge[Int] {
      override def getValue: Int = engine.compiledPackages().packageIds.size
    })
  }

  private val validateEntry: Step = (ctx, uploadEntry) => {
    // NOTE(JM): Currently the proper validation is unimplemented. The package is decoded and preloaded
    // in background and we're just checking that hash and payload are set. See comment in [[preload]].
    val errors = uploadEntry.getArchivesList.asScala.foldLeft(List.empty[String]) {
      (errors, archive) =>
        if (archive.getHashBytes.size > 0 && archive.getPayload.size > 0)
          errors
        else
          s"Invalid archive ${archive.getHash}" :: errors
    }
    if (errors.isEmpty)
      StepContinue(uploadEntry)
    else
      StepStop(
        buildPackageRejectionLogEntry(
          ctx,
          uploadEntry,
          _.setInvalidPackage(DamlPackageUploadRejectionEntry.InvalidPackage.newBuilder
            .setDetails(errors.mkString(", ")))))
  }

  private val filterDuplicates: Step = (ctx, uploadEntry) => {
    val archives = uploadEntry.getArchivesList.asScala.filter { archive =>
      val stateKey = DamlStateKey.newBuilder
        .setPackageId(archive.getHash)
        .build
      ctx.get(stateKey).isEmpty
    }
    StepContinue(uploadEntry.clearArchives().addAllArchives(archives.asJava))
  }

  private val enqueuePreload: Step = (_, uploadEntry) => {
    preloadExecutor.execute(
      preload(uploadEntry.getSubmissionId, uploadEntry.getArchivesList.asScala))
    StepContinue(uploadEntry)
  }

  private val buildLogEntry: Step = (ctx, uploadEntry) => {
    logger.trace(
      s"Packages committed: ${uploadEntry.getArchivesList.asScala.map(_.getHash).mkString(", ")}")

    uploadEntry.getArchivesList.forEach { archive =>
      ctx.set(
        DamlStateKey.newBuilder.setPackageId(archive.getHash).build,
        DamlStateValue.newBuilder.setArchive(archive).build
      )
    }
    StepStop(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(ctx.getRecordTime))
        .setPackageUploadEntry(uploadEntry)
        .build
    )
  }

  override def init(uploadEntry: DamlPackageUploadEntry): DamlPackageUploadEntry.Builder =
    uploadEntry.toBuilder

  override val steps: Iterable[(StepInfo, Step)] = Iterable(
    "validateEntry" -> validateEntry,
    "filterDuplicates" -> filterDuplicates,
    "enqueuePreload" -> enqueuePreload,
    "buildLogEntry" -> buildLogEntry
  )

  private def buildPackageRejectionLogEntry(
      ctx: CommitContext,
      packageUploadEntry: DamlPackageUploadEntry.Builder,
      addErrorDetails: DamlPackageUploadRejectionEntry.Builder => DamlPackageUploadRejectionEntry.Builder)
    : DamlLogEntry = {
    DamlLogEntry.newBuilder
      .setRecordTime(buildTimestamp(ctx.getRecordTime))
      .setPackageUploadRejectionEntry(
        addErrorDetails(
          DamlPackageUploadRejectionEntry.newBuilder
            .setSubmissionId(packageUploadEntry.getSubmissionId)
            .setParticipantId(packageUploadEntry.getParticipantId)
        )
      )
      .build
  }

  /** Preload the archives to the engine in a background thread.
    *
    * The background loading is a temporary workaround for handling processing of large packages. When our current
    * integrations using kvutils can handle long-running submissions this can be removed and complete
    * package type-checking and preloading can be done during normal processing.
    */
  private def preload(submissionId: String, archives: Iterable[Archive]): Runnable = { () =>
    val ctx = Metrics.preloadTimer.time()
    def trace(msg: String): Unit = logger.trace("[submissionId=$submissionId]: " + msg)
    try {
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
      trace(s"Preloading engine with ${packages.size} new packages")
      packages.foreach {
        case (pkgId, pkg) =>
          engine
            .preloadPackage(pkgId, pkg)
            .consume(
              _ => sys.error("Unexpected request to PCS in preloadPackage"),
              pkgId => packages.get(pkgId),
              _ => sys.error("Unexpected request to keys in preloadPackage")
            )
      }
      trace(s"Preload complete.")
    } catch {
      case scala.util.control.NonFatal(err) =>
        logger.error(s"[submissionId=$submissionId]: Preload exception: $err")
    } finally {
      val _ = ctx.stop()
    }
  }

  private val preloadExecutor = {
    Executors.newSingleThreadExecutor(new ThreadFactory {
      // Use a custom thread factory that creates daemon threads to avoid blocking the JVM from exiting.
      override def newThread(runnable: Runnable): Thread = {
        val t = new Thread(runnable)
        t.setDaemon(true)
        t
      }
    })
  }

}
