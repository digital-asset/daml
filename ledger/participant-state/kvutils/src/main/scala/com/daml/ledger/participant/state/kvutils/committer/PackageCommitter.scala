// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.util.concurrent.Executors

import com.daml.ledger.participant.state.kvutils.{Conversions, Raw}
import com.daml.ledger.participant.state.kvutils.Conversions.packageUploadDedupKey
import com.daml.ledger.participant.state.kvutils.committer.Committer.buildLogEntryWithOptionalRecordTime
import com.daml.ledger.participant.state.kvutils.store.events.PackageUpload.{
  DamlPackageUploadEntry,
  DamlPackageUploadRejectionEntry,
}
import com.daml.ledger.participant.state.kvutils.store.events.{
  Duplicate,
  Invalid,
  ParticipantNotAuthorized,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue,
  DamlSubmissionDedupValue,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.lf
import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.logging.entries.LoggingEntries
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[committer] object PackageCommitter {
  final case class Result(
      uploadEntry: DamlPackageUploadEntry.Builder,
      hashesAndArchives: Iterable[(String, Raw.Archive)],
      packagesCache: Map[Ref.PackageId, Ast.Package],
  )

  type Step = CommitStep[Result]
}

final private[kvutils] class PackageCommitter(
    engine: Engine,
    override protected val metrics: Metrics,
    validationMode: PackageValidationMode = PackageValidationMode.Lenient,
    preloadingMode: PackagePreloadingMode = PackagePreloadingMode.Asynchronous,
) extends Committer[PackageCommitter.Result] {

  import PackageCommitter._

  private final val logger = ContextualizedLogger.get(getClass)

  override protected val committerName: String = "package_upload"

  override protected def extraLoggingContext(result: Result): LoggingEntries =
    LoggingEntries(
      "packages" -> result.hashesAndArchives.map(_._1)
    )

  /** The initial internal state passed to first step. */
  override protected def init(
      ctx: CommitContext,
      submission: DamlSubmission,
  )(implicit loggingContext: LoggingContext): Result =
    Result(submission.getPackageUploadEntry.toBuilder, Iterable.empty, Map.empty)

  private def rejectionTraceLog(message: String)(implicit loggingContext: LoggingContext): Unit =
    logger.trace(s"Package upload rejected: $message.")

  private def reject(
      recordTime: Option[Timestamp],
      submissionId: String,
      participantId: String,
      addErrorDetails: DamlPackageUploadRejectionEntry.Builder => DamlPackageUploadRejectionEntry.Builder,
  ): StepStop = {
    metrics.daml.kvutils.committer.packageUpload.rejections.inc()
    StepStop(buildRejectionLogEntry(recordTime, submissionId, participantId, addErrorDetails))
  }

  private def buildRejectionLogEntry(
      recordTime: Option[Timestamp],
      submissionId: String,
      participantId: String,
      addErrorDetails: DamlPackageUploadRejectionEntry.Builder => DamlPackageUploadRejectionEntry.Builder,
  ): DamlLogEntry =
    buildLogEntryWithOptionalRecordTime(
      recordTime,
      _.setPackageUploadRejectionEntry(
        addErrorDetails(
          DamlPackageUploadRejectionEntry.newBuilder
            .setSubmissionId(submissionId)
            .setParticipantId(participantId)
        )
      ),
    )

  private def setOutOfTimeBoundsLogEntry(
      uploadEntry: DamlPackageUploadEntry.Builder,
      commitContext: CommitContext,
  ): Unit =
    commitContext.outOfTimeBoundsLogEntry = Some(
      buildRejectionLogEntry(
        recordTime = None,
        uploadEntry.getSubmissionId,
        uploadEntry.getParticipantId,
        identity,
      )
    )

  private def readArchiveHashes: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      try {
        val hashesAndArchives = partialResult.uploadEntry.getArchivesList.asScala.view.map {
          archive =>
            val rawArchive = Raw.Archive(archive)
            Conversions.extractHashFromArchive(rawArchive) -> rawArchive
        }

        StepContinue(partialResult.copy(hashesAndArchives = hashesAndArchives))
      } catch {
        case NonFatal(e) =>
          val uploadEntry = partialResult.uploadEntry
          rejectionTraceLog(e.getMessage)
          reject(
            ctx.recordTime,
            uploadEntry.getSubmissionId,
            uploadEntry.getParticipantId,
            _.setInvalidPackage(Invalid.newBuilder.setDetails("Cannot parse archive")),
          )
      }
    }
  }

  private def authorizeSubmission: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val uploadEntry = partialResult.uploadEntry
      if (ctx.participantId == uploadEntry.getParticipantId) {
        StepContinue(partialResult)
      } else {
        val message =
          s"Participant ID '${uploadEntry.getParticipantId}' did not match authorized participant ID '${ctx.participantId}'"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setParticipantNotAuthorized(ParticipantNotAuthorized.newBuilder.setDetails(message)),
        )
      }
    }
  }

  private def deduplicateSubmission: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val uploadEntry = partialResult.uploadEntry
      val submissionKey = packageUploadDedupKey(ctx.participantId, uploadEntry.getSubmissionId)
      if (ctx.get(submissionKey).isEmpty) {
        StepContinue(partialResult)
      } else {
        val message = s"duplicate submission='${uploadEntry.getSubmissionId}'"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(message)),
        )
      }
    }
  }

  // Checks that packages are not repeated in the submission.
  private def checkForDuplicates: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val (seenOnce, duplicates) = partialResult.hashesAndArchives.iterator
        .foldLeft((Set.empty[String], Set.empty[String])) {
          case ((seenOnce, duplicates), (hash, _)) =>
            if (seenOnce(hash))
              (seenOnce, duplicates + hash)
            else
              (seenOnce + hash, duplicates)
        }

      if (seenOnce.isEmpty || duplicates.nonEmpty) {
        val uploadEntry = partialResult.uploadEntry
        val message =
          if (seenOnce.isEmpty)
            "No archives in submission"
          else
            duplicates.iterator
              .map(pkgId => s"package $pkgId appears more than once")
              .mkString(", ")
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setInvalidPackage(Invalid.newBuilder.setDetails(message)),
        )
      } else {
        StepContinue(partialResult)
      }
    }
  }

  private def decodePackages(
      hashesAndArchives: Iterable[(String, Raw.Archive)]
  ): Either[String, Map[Ref.PackageId, Ast.Package]] =
    metrics.daml.kvutils.committer.packageUpload.decodeTimer.time { () =>
      type Result = Either[List[String], Map[Ref.PackageId, Ast.Package]]
      hashesAndArchives
        .foldLeft[Result](Right(Map.empty)) { case (acc, (hash, rawArchive)) =>
          try {
            acc.map { result =>
              val archive = ArchiveParser.assertFromByteString(rawArchive.bytes)
              result + lf.archive.Decode.assertDecodeArchive(archive)
            }
          } catch {
            case NonFatal(e) =>
              Left(
                s"Cannot decode archive $hash: ${e.getMessage}" :: acc.left.getOrElse(Nil)
              )
          }
        }
        .left
        .map(_.mkString(", "))
    }

  private def decodePackagesIfNeeded(
      pkgsCache: Map[Ref.PackageId, Ast.Package],
      hashesAndArchives: Iterable[(String, Raw.Archive)],
  ): Either[String, Map[PackageId, Ast.Package]] =
    if (pkgsCache.isEmpty)
      decodePackages(hashesAndArchives)
    else
      Right(pkgsCache)

  private def validatePackages(
      pkgs: Map[Ref.PackageId, Ast.Package]
  ): Either[String, Unit] =
    metrics.daml.kvutils.committer.packageUpload.validateTimer.time { () =>
      engine.validatePackages(pkgs).left.map(_.message)
    }

  // Strict validation
  private def strictlyValidatePackages: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val Result(uploadEntry, hashesAndArchives, packagesCache) = partialResult
      val result = for {
        packages <- decodePackagesIfNeeded(
          packagesCache,
          hashesAndArchives,
        )
        _ <- validatePackages(packages)
      } yield StepContinue(Result(uploadEntry, hashesAndArchives, packages))

      result match {
        case Right(result) => result
        case Left(message) =>
          rejectionTraceLog(message)
          reject(
            ctx.recordTime,
            uploadEntry.getSubmissionId,
            uploadEntry.getParticipantId,
            _.setInvalidPackage(Invalid.newBuilder.setDetails(message)),
          )
      }
    }
  }

  // Minimal validation.
  // Checks that package IDs are valid and package payloads are non-empty.
  private def looselyValidatePackages: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val uploadEntry = partialResult.uploadEntry
      val archives = uploadEntry.getArchivesList.asScala
      val errors =
        archives.foldLeft(List.empty[String]) { (errors, rawArchive) =>
          val archive = ArchiveParser.assertFromByteString(rawArchive)
          if (archive.getPayload.isEmpty)
            s"Empty archive '${archive.getHash}'" :: errors
          else
            Ref.PackageId
              .fromString(archive.getHash)
              .fold(msg => s"Invalid hash: $msg" :: errors, _ => errors)
        }

      if (errors.isEmpty) {
        StepContinue(partialResult)
      } else {
        val message = errors.mkString(", ")
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setInvalidPackage(Invalid.newBuilder.setDetails(message)),
        )
      }
    }
  }

  private def uploadPackages(packages: Map[Ref.PackageId, Ast.Package]): Either[String, Unit] =
    metrics.daml.kvutils.committer.packageUpload.preloadTimer.time { () =>
      val errors = packages.flatMap { case (pkgId, pkg) =>
        engine
          .preloadPackage(pkgId, pkg)
          .consume(_ => None, packages.get, _ => None)
          .fold(err => List(err.message), _ => List.empty)
      }.toList
      metrics.daml.kvutils.committer.packageUpload.loadedPackages(() =>
        engine.compiledPackages().packageIds.size
      )
      Either.cond(
        errors.isEmpty,
        (),
        errors.mkString(", "),
      )
    }

  private def preloadSynchronously: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val Result(uploadEntry, hashesAndArchives, packagesCache) = partialResult
      val result = for {
        packages <- decodePackagesIfNeeded(
          packagesCache,
          hashesAndArchives,
        )
        _ <- uploadPackages(packages)
      } yield StepContinue(Result(uploadEntry, hashesAndArchives, packages))

      result match {
        case Right(partialResult) =>
          partialResult
        case Left(message) =>
          rejectionTraceLog(message)
          reject(
            ctx.recordTime,
            uploadEntry.getSubmissionId,
            uploadEntry.getParticipantId,
            _.setInvalidPackage(Invalid.newBuilder.setDetails(message)),
          )
      }
    }
  }

  private lazy val preloadExecutor =
    Executors.newSingleThreadExecutor { (runnable: Runnable) =>
      val t = new Thread(runnable, "package-preload-executor")
      t.setDaemon(true)
      t
    }

  /** Preload the archives to the engine in a background thread.
    *
    * The background loading is a temporary workaround for handling processing of large packages. When our current
    * integrations using kvutils can handle long-running submissions this can be removed and complete
    * package type-checking and preloading can be done during normal processing.
    *
    * This assumes the engine validates the archive it receives.
    */
  private def preloadAsynchronously: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val Result(uploadEntry, hashesAndArchives, packagesCache) = partialResult
      preloadExecutor.execute { () =>
        logger.trace(s"Uploading ${uploadEntry.getArchivesCount} archive(s).")
        val result = for {
          packages <- decodePackagesIfNeeded(packagesCache, hashesAndArchives)
          _ <- uploadPackages(packages)
        } yield ()

        result.fold(
          msg => logger.trace(s"Uploading failed: $msg."),
          _ => logger.trace("Uploading successful."),
        )
      }
      StepContinue(partialResult)
    }
  }

  // Filter out packages already on the ledger.
  // Should be done after decoding, validation or preloading, as those step may
  // require packages on the ledger by not loaded by the engine.
  private def filterKnownPackages: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val Result(uploadEntry, hashesAndArchives, packagesCache) = partialResult
      val newHashesAndArchives = hashesAndArchives.filter { case (hash, _) =>
        val stateKey = DamlStateKey.newBuilder
          .setPackageId(hash)
          .build
        ctx.get(stateKey).isEmpty
      }
      val newUploadEntry = uploadEntry.clearArchives()
      newHashesAndArchives.foreach { case (_, rawArchive) =>
        newUploadEntry.addArchives(rawArchive.bytes)
      }
      StepContinue(
        Result(
          newUploadEntry,
          newHashesAndArchives,
          packagesCache,
        )
      )
    }
  }

  private[committer] def buildLogEntry: Step = new Step {
    def apply(
        ctx: CommitContext,
        partialResult: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      metrics.daml.kvutils.committer.packageUpload.accepts.inc()
      logger.trace("Packages committed.")

      partialResult.hashesAndArchives.foreach { case (hash, rawArchive) =>
        ctx.set(
          DamlStateKey.newBuilder
            .setPackageId(hash)
            .build,
          DamlStateValue.newBuilder.setArchive(rawArchive.bytes).build,
        )
      }
      val uploadEntry = partialResult.uploadEntry
      ctx.set(
        packageUploadDedupKey(ctx.participantId, uploadEntry.getSubmissionId),
        DamlStateValue.newBuilder
          .setSubmissionDedup(DamlSubmissionDedupValue.newBuilder)
          .build,
      )
      val successLogEntry =
        buildLogEntryWithOptionalRecordTime(ctx.recordTime, _.setPackageUploadEntry(uploadEntry))
      if (ctx.preExecute) {
        setOutOfTimeBoundsLogEntry(uploadEntry, ctx)
      }
      StepStop(successLogEntry)
    }
  }
  override protected val steps: Steps[Result] = {
    val builder = List.newBuilder[(StepInfo, Step)]

    builder += "read_archive_hashes" -> readArchiveHashes

    validationMode match {
      case PackageValidationMode.No =>
      case _ =>
        builder += "authorize_submission" -> authorizeSubmission
        builder += "deduplicate_submission" -> deduplicateSubmission
        builder += "check_for_duplicate" -> checkForDuplicates
        validationMode match {
          case PackageValidationMode.Strict =>
            builder += "validate_packages" -> strictlyValidatePackages
          case _ =>
            builder += "validate_packages" -> looselyValidatePackages
        }
    }
    preloadingMode match {
      case PackagePreloadingMode.No =>
      case PackagePreloadingMode.Synchronous =>
        builder += "synchronously_preload" -> preloadSynchronously
      case PackagePreloadingMode.Asynchronous =>
        builder += "asynchronously_preload" -> preloadAsynchronously
    }
    builder += "filter_known_packages" -> filterKnownPackages
    builder += "build_log_entry" -> buildLogEntry

    builder.result()
  }
}
