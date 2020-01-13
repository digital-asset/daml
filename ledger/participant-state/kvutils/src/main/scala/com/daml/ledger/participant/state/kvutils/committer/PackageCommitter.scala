// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.util.concurrent.Executors

import com.codahale.metrics.{Counter, Gauge, Timer}
import com.daml.ledger.participant.state.kvutils.Conversions.{buildTimestamp, packageUploadDedupKey}
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
    // kvutils.PackageCommitter.*
    val preloadTimer: Timer = metricsRegistry.timer(metricsName("preload_timer"))
    val decodeTimer: Timer = metricsRegistry.timer(metricsName("decode_timer"))
    val accepts: Counter = metricsRegistry.counter(metricsName("accepts"))
    val rejections: Counter = metricsRegistry.counter(metricsName("rejections"))
    metricsRegistry.gauge(
      metricsName("loaded_packages"),
      () =>
        new Gauge[Int] {
          override def getValue: Int = engine.compiledPackages().packageIds.size
      }
    )
  }

  private def rejectionTraceLog(
      msg: String,
      packageUploadEntry: DamlPackageUploadEntry.Builder): Unit =
    logger.trace(
      s"Package upload rejected, $msg, correlationId=${packageUploadEntry.getSubmissionId}")

  private val authorizeSubmission: Step = (ctx, uploadEntry) => {
    if (ctx.getParticipantId == uploadEntry.getParticipantId)
      StepContinue(uploadEntry)
    else {
      val msg =
        s"participant id ${uploadEntry.getParticipantId} did not match authenticated participant id ${ctx.getParticipantId}"
      rejectionTraceLog(msg, uploadEntry)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          uploadEntry,
          _.setParticipantNotAuthorized(ParticipantNotAuthorized.newBuilder
            .setDetails(msg))))
    }
  }

  private val validateEntry: Step = (ctx, uploadEntry) => {
    // NOTE(JM): Currently the proper validation is unimplemented. The package is decoded and preloaded
    // in background and we're just checking that hash and payload are set. See comment in [[preload]].
    val archives = uploadEntry.getArchivesList.asScala
    val errors = if (archives.nonEmpty) {
      archives.foldLeft(List.empty[String]) { (errors, archive) =>
        if (archive.getHashBytes.size > 0 && archive.getPayload.size > 0)
          errors
        else
          s"Invalid archive ${archive.getHash}" :: errors
      }
    } else {
      List("No archives in package")
    }
    if (errors.isEmpty) {
      StepContinue(uploadEntry)
    } else {
      val msg = errors.mkString(", ")
      rejectionTraceLog(msg, uploadEntry)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          uploadEntry,
          _.setInvalidPackage(Invalid.newBuilder
            .setDetails(msg))))
    }
  }

  private val deduplicateSubmission: Step = (ctx, uploadEntry) => {
    val submissionKey = packageUploadDedupKey(ctx.getParticipantId, uploadEntry.getSubmissionId)
    if (ctx.get(submissionKey).isEmpty)
      StepContinue(uploadEntry)
    else {
      val msg = s"duplicate submission='${uploadEntry.getSubmissionId}'"
      rejectionTraceLog(msg, uploadEntry)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          uploadEntry,
          _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(msg))
        )
      )
    }
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

  private val preloadExecutor = {
    Executors.newSingleThreadExecutor((runnable: Runnable) => {
      val t = new Thread(runnable)
      t.setDaemon(true)
      t
    })
  }

  private val enqueuePreload: Step = (_, uploadEntry) => {
    preloadExecutor.execute(
      preload(uploadEntry.getSubmissionId, uploadEntry.getArchivesList.asScala))
    StepContinue(uploadEntry)
  }

  private val buildLogEntry: Step = (ctx, uploadEntry) => {
    Metrics.accepts.inc()
    logger.trace(
      s"Packages committed, packages=[${uploadEntry.getArchivesList.asScala.map(_.getHash).mkString(", ")}] correlationId=${uploadEntry.getSubmissionId}")

    uploadEntry.getArchivesList.forEach { archive =>
      ctx.set(
        DamlStateKey.newBuilder.setPackageId(archive.getHash).build,
        DamlStateValue.newBuilder.setArchive(archive).build
      )
    }
    ctx.set(
      packageUploadDedupKey(ctx.getParticipantId, uploadEntry.getSubmissionId),
      DamlStateValue.newBuilder
        .setSubmissionDedup(
          DamlSubmissionDedupValue.newBuilder
            .setRecordTime(buildTimestamp(ctx.getRecordTime))
            .build)
        .build
    )
    StepStop(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(ctx.getRecordTime))
        .setPackageUploadEntry(uploadEntry)
        .build
    )
  }

  override def init(
      ctx: CommitContext,
      uploadEntry: DamlPackageUploadEntry): DamlPackageUploadEntry.Builder =
    uploadEntry.toBuilder

  override val steps: Iterable[(StepInfo, Step)] = Iterable(
    "authorize_submission" -> authorizeSubmission,
    "validate_entry" -> validateEntry,
    "deduplicate_submission" -> deduplicateSubmission,
    "filter_duplicates" -> filterDuplicates,
    "enqueue_preload" -> enqueuePreload,
    "build_log_entry" -> buildLogEntry
  )

  override lazy val committerName = "package_upload"

  private def buildRejectionLogEntry(
      ctx: CommitContext,
      packageUploadEntry: DamlPackageUploadEntry.Builder,
      addErrorDetails: DamlPackageUploadRejectionEntry.Builder => DamlPackageUploadRejectionEntry.Builder)
    : DamlLogEntry = {
    Metrics.rejections.inc()
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
    def trace(msg: String): Unit = logger.trace(s"$msg, correlationId=$submissionId")
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
      trace(s"Preload complete")
    } catch {
      case scala.util.control.NonFatal(err) =>
        logger.error(
          s"Preload exception, correlationId=$submissionId error='$err' stackTrace='${err.getStackTrace
            .mkString(", ")}'")
    } finally {
      val _ = ctx.stop()
    }
  }

}
