// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.util.concurrent.Executors

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.kvutils.DamlKvutils
import com.daml.ledger.participant.state.kvutils.Conversions.packageUploadDedupKey
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer.{
  StepInfo,
  buildLogEntryWithOptionalRecordTime
}
import com.daml.lf
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object PackageCommitter {}

final private[kvutils] class PackageCommitter(
    engine: Engine,
    override protected val metrics: Metrics,
    validationMode: PackageValidationMode = PackageValidationMode.Lenient,
    preloadingMode: PackagePreloadingMode = PackagePreloadingMode.Asynchronous,
) extends Committer[(DamlPackageUploadEntry.Builder, Map[Ref.PackageId, Ast.Package])] {

  /** The initial internal state passed to first step. */
  override protected def init(
      ctx: CommitContext,
      submission: DamlSubmission,
  ): (DamlPackageUploadEntry.Builder, Map[Ref.PackageId, Ast.Package]) =
    (submission.getPackageUploadEntry.toBuilder, Map.empty)

  def traceLog(msg: String, uploadEntry: DamlPackageUploadEntry.Builder): Unit =
    logger.trace(s"$msg, correlationId=${uploadEntry.getSubmissionId}")

  private[this] def rejectionTraceLog(
      msg: String,
      uploadEntry: DamlPackageUploadEntry.Builder,
  ): Unit =
    traceLog(s"Package upload rejected, $msg", uploadEntry)

  private[this] def reject(
      recordTime: Option[Timestamp],
      submissionId: String,
      participantId: String,
      addErrorDetails: DamlPackageUploadRejectionEntry.Builder => DamlPackageUploadRejectionEntry.Builder,
  ): StepStop = {
    metrics.daml.kvutils.committer.packageUpload.rejections.inc()
    StepStop(buildRejectionLogEntry(recordTime, submissionId, participantId, addErrorDetails))
  }

  private[this] def buildRejectionLogEntry(
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
      )
    )

  private[this] def setOutOfTimeBoundsLogEntry(
      uploadEntry: DamlPackageUploadEntry.Builder,
      commitContext: CommitContext): Unit =
    commitContext.outOfTimeBoundsLogEntry = Some(
      buildRejectionLogEntry(
        recordTime = None,
        uploadEntry.getSubmissionId,
        uploadEntry.getParticipantId,
        identity,
      )
    )

  private[this] def authorizeSubmission: Step = {
    case (ctx, partialResult @ (uploadEntry, _)) =>
      if (ctx.participantId == uploadEntry.getParticipantId) {
        StepContinue(partialResult)
      } else {
        val msg =
          s"Participant ID '${uploadEntry.getParticipantId}' did not match authorized participant ID '${ctx.participantId}'"
        rejectionTraceLog(msg, uploadEntry)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setParticipantNotAuthorized(ParticipantNotAuthorized.newBuilder.setDetails(msg))
        )
      }
  }

  private[this] def deduplicateSubmission: Step = {
    case (ctx, partialResult @ (uploadEntry, _)) =>
      val submissionKey = packageUploadDedupKey(ctx.participantId, uploadEntry.getSubmissionId)
      if (ctx.get(submissionKey).isEmpty) {
        StepContinue(partialResult)
      } else {
        val msg = s"duplicate submission='${uploadEntry.getSubmissionId}'"
        rejectionTraceLog(msg, uploadEntry)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(msg))
        )
      }
  }

// Checks that packages are not repeated in the submission.
  private[this] def checkForDuplicates: Step = {
    case (ctx, partialResult @ (uploadEntry, _)) =>
      val (seenOnce, duplicates) = uploadEntry.getArchivesList
        .iterator()
        .asScala
        .foldLeft((Set.empty[ByteString], Set.empty[ByteString])) {
          case ((seenOnce, duplicates), pkg) =>
            val hash = pkg.getHashBytes
            if (seenOnce(hash))
              (seenOnce, duplicates + hash)
            else
              (seenOnce + hash, duplicates)
        }

      if (seenOnce.isEmpty || duplicates.nonEmpty) {
        val validationError =
          if (seenOnce.isEmpty)
            "No archives in submission"
          else
            duplicates.iterator
              .map(pkgId => s"package ${pkgId.toStringUtf8} appears more than once")
              .mkString(", ")
        rejectionTraceLog(validationError, uploadEntry)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setInvalidPackage(DamlKvutils.Invalid.newBuilder.setDetails(validationError))
        )
      } else {
        StepContinue(partialResult)
      }
  }

  private[this] def decodePackages(
      archives: Traversable[DamlLf.Archive],
  ): Either[String, Map[Ref.PackageId, Ast.Package]] =
    metrics.daml.kvutils.committer.packageUpload.decodeTimer.time { () =>
      type Result = Either[List[String], Map[Ref.PackageId, Ast.Package]]
      val knownPackages = engine.compiledPackages().packageIds.toSet[String]

      archives
        .foldLeft[Result](Right(Map.empty)) { (acc, arch) =>
          try {
            if (knownPackages(arch.getHash)) {
              // If the package is already known by the engine, we don't decode it but still verify its hash.
              lf.archive.Reader.HashChecker.decodeArchive(arch)
              acc
            } else {
              acc.map(_ + lf.archive.Decode.decodeArchive(arch))
            }
          } catch {
            case NonFatal(e) =>
              Left(
                s"Cannot decode archive ${arch.getHash}: ${e.getMessage}" :: acc.left
                  .getOrElse(Nil))
          }
        }
        .left
        .map(_.mkString(", "))
    }

  private[this] def decodePackagesIfNeeded(
      pkgsCache: Map[Ref.PackageId, Ast.Package],
      archives: Traversable[DamlLf.Archive],
  ): Either[String, Map[PackageId, Ast.Package]] =
    if (pkgsCache.isEmpty)
      decodePackages(archives)
    else
      Right(pkgsCache)

  private[this] def validatePackages(
      uploadEntry: DamlPackageUploadEntry.Builder,
      pkgs: Map[Ref.PackageId, Ast.Package],
  ): Either[String, Unit] =
    metrics.daml.kvutils.committer.packageUpload.validateTimer.time { () =>
      val allPkgIds = uploadEntry.getArchivesList
        .iterator()
        .asScala
        .map(pkg => Ref.PackageId.assertFromString(pkg.getHash))
        .toSet
      engine.validatePackages(allPkgIds, pkgs).left.map(_.detailMsg)
    }

  // Strict validation
  private[this] def strictlyValidatePackages: Step = {
    case (ctx, (uploadEntry, pkgsCache)) =>
      val result = for {
        pkgs <- decodePackagesIfNeeded(pkgsCache, uploadEntry.getArchivesList.asScala)
        _ <- validatePackages(uploadEntry, pkgs)
      } yield StepContinue((uploadEntry, pkgs))

      result match {
        case Right(result) => result
        case Left(msg) =>
          rejectionTraceLog(msg, uploadEntry)
          reject(
            ctx.recordTime,
            uploadEntry.getSubmissionId,
            uploadEntry.getParticipantId,
            _.setInvalidPackage(DamlKvutils.Invalid.newBuilder.setDetails(msg))
          )
      }
  }

  // Minimal validation.
  // Checks that package IDs are valid and package payloads are non-empty.
  private[this] def looselyValidatePackages: Step = {
    case (ctx, partialResult @ (uploadEntry, _)) =>
      val archives = uploadEntry.getArchivesList.asScala
      val errors =
        archives.foldLeft(List.empty[String]) { (errors, archive) =>
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
        val msg = errors.mkString(", ")
        rejectionTraceLog(msg, uploadEntry)
        reject(
          ctx.recordTime,
          uploadEntry.getSubmissionId,
          uploadEntry.getParticipantId,
          _.setInvalidPackage(Invalid.newBuilder.setDetails(msg))
        )
      }
  }

  private[this] def uploadPackages(pkgs: Map[Ref.PackageId, Ast.Package]): Either[String, Unit] =
    metrics.daml.kvutils.committer.packageUpload.preloadTimer.time { () =>
      val errors = pkgs.flatMap {
        case (pkgId, pkg) =>
          engine
            .preloadPackage(pkgId, pkg)
            .consume(_ => None, pkgs.get, _ => None)
            .fold(err => List(err.detailMsg), _ => List.empty)
      }.toList
      metrics.daml.kvutils.committer.packageUpload.loadedPackages(() =>
        engine.compiledPackages().packageIds.size)
      Either.cond(
        errors.isEmpty,
        (),
        errors.mkString(", ")
      )
    }

  private[this] def preloadSynchronously: Step = {
    case (ctx, (uploadEntry, pkgsCache)) =>
      val result = for {
        pkgs <- decodePackagesIfNeeded(pkgsCache, uploadEntry.getArchivesList.asScala)
        _ <- uploadPackages(pkgs)
      } yield StepContinue((uploadEntry, pkgs))

      result match {
        case Right(partialResult) =>
          partialResult
        case Left(msg) =>
          rejectionTraceLog(msg, uploadEntry)
          reject(
            ctx.recordTime,
            uploadEntry.getSubmissionId,
            uploadEntry.getParticipantId,
            _.setInvalidPackage(DamlKvutils.Invalid.newBuilder.setDetails(msg))
          )
      }
  }

  private[this] def preloadExecutor =
    Executors.newSingleThreadExecutor { (runnable: Runnable) =>
      val t = new Thread(runnable)
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
  private[this] def preloadAsynchronously: Step = {
    case (_, partialResult @ (uploadEntry, pkgsCache)) =>
      // we need to extract the archives synchronously as other steps may modify uploadEntry
      val archives = uploadEntry.getArchivesList.iterator().asScala.toList
      preloadExecutor.execute { () =>
        traceLog(s"Uploading ${uploadEntry.getArchivesCount} archive", uploadEntry)
        val result = for {
          pkgs <- decodePackagesIfNeeded(pkgsCache, archives)
          _ <- uploadPackages(pkgs)
        } yield ()

        result.fold(
          msg => traceLog(s"Uploading failed: $msg", uploadEntry),
          _ => traceLog("Uploading successful", uploadEntry),
        )
      }
      StepContinue(partialResult)
  }

// Filter out packages already on the ledger.
// Should be done after decoding, validation or preloading, as those step may
// require packages on the ledger by not loaded by the engine.
  private[this] def filterKnownPackages: Step = {
    case (ctx, (uploadEntry, pkgs)) =>
      val archives = uploadEntry.getArchivesList.asScala.filter { archive =>
        val stateKey = DamlStateKey.newBuilder
          .setPackageId(archive.getHash)
          .build
        ctx.get(stateKey).isEmpty
      }
      StepContinue(uploadEntry.clearArchives().addAllArchives(archives.asJava) -> pkgs)
  }

  private[committer] def buildLogEntry: Step = {
    case (ctx, (uploadEntry, _)) =>
      metrics.daml.kvutils.committer.packageUpload.accepts.inc()
      logger.trace(
        s"Packages committed, packages=[${uploadEntry.getArchivesList.asScala.map(_.getHash).mkString(", ")}] correlationId=${uploadEntry.getSubmissionId}")

      uploadEntry.getArchivesList.forEach { archive =>
        ctx.set(
          DamlStateKey.newBuilder.setPackageId(archive.getHash).build,
          DamlStateValue.newBuilder.setArchive(archive).build
        )
      }
      ctx.set(
        packageUploadDedupKey(ctx.participantId, uploadEntry.getSubmissionId),
        DamlStateValue.newBuilder
          .setSubmissionDedup(DamlSubmissionDedupValue.newBuilder)
          .build
      )
      val successLogEntry =
        buildLogEntryWithOptionalRecordTime(ctx.recordTime, _.setPackageUploadEntry(uploadEntry))
      if (ctx.preExecute) {
        setOutOfTimeBoundsLogEntry(uploadEntry, ctx)
      }
      StepStop(successLogEntry)
  }

  override protected val committerName: String = "package_upload"

  val steps = {
    val builder = List.newBuilder[(StepInfo, Step)]

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
