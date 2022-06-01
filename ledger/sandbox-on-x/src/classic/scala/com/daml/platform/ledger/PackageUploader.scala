// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.daml.ledger.api.domain.PackageEntry
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.WriteService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}

import java.io.File
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.chaining._
import scala.util.{Failure, Success}

class PackageUploader(
    writeService: WriteService,
    indexService: IndexService,
)(implicit executionContext: ExecutionContext, materializer: Materializer) {

  def upload(damlPackages: List[File])(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[Unit] =
    ResourceOwner.forFuture(() => {
      val packageSubmissionsTrackerMap = damlPackages.map { file =>
        val uploadCompletionPromise = Promise[Unit]()
        UUID.randomUUID().toString -> (file, uploadCompletionPromise)
      }.toMap

      uploadAndWaitPackages(indexService, writeService, packageSubmissionsTrackerMap)
        .tap { _ =>
          scheduleUploadTimeout(packageSubmissionsTrackerMap.iterator.map(_._2._2), 30.seconds)
        }
    })

  private def scheduleUploadTimeout(
      packageSubmissionsPromises: Iterator[Promise[Unit]],
      packageUploadTimeout: FiniteDuration,
  ): Unit =
    packageSubmissionsPromises.foreach { uploadCompletionPromise =>
      materializer.system.scheduler.scheduleOnce(packageUploadTimeout) {
        // Package upload timeout, meant to allow fail-fast for testing
        // TODO Remove once in-memory backend (e.g. H2) is deemed stable
        uploadCompletionPromise.tryFailure(
          new RuntimeException(s"Package upload timeout after $packageUploadTimeout")
        )
        ()
      }
    }

  private def uploadAndWaitPackages(
      indexService: IndexService,
      writeService: WriteService,
      packageSubmissionsTrackerMap: Map[String, (File, Promise[Unit])],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    implicit val noOpTelemetryContext: TelemetryContext = NoOpTelemetryContext

    val uploadCompletionSink = Sink.foreach[PackageEntry] {
      case PackageEntry.PackageUploadAccepted(submissionId, _) =>
        packageSubmissionsTrackerMap.get(submissionId) match {
          case Some((_, uploadCompletionPromise)) =>
            uploadCompletionPromise.complete(Success(()))
          case None =>
            throw new RuntimeException(s"Completion promise for $submissionId not found")
        }
      case PackageEntry.PackageUploadRejected(submissionId, _, reason) =>
        packageSubmissionsTrackerMap.get(submissionId) match {
          case Some((_, uploadCompletionPromise)) =>
            uploadCompletionPromise.complete(
              Failure(new RuntimeException(s"Package upload at initialization failed: $reason"))
            )
          case None =>
            throw new RuntimeException(s"Completion promise for $submissionId not found")
        }
    }

    val (killSwitch, packageEntriesStreamDone) = indexService
      .packageEntries(None)
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(uploadCompletionSink)(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

    val uploadAndWaitPackagesF = Future.traverse(packageSubmissionsTrackerMap.toVector) {
      case (submissionId, (file, promise)) =>
        for {
          dar <- Future.fromTry(DarParser.readArchiveFromFile(file).toTry)
          refSubmissionId = Ref.SubmissionId.assertFromString(submissionId)
          _ <- writeService
            .uploadPackages(refSubmissionId, dar.all, None)
            .asScala
          uploadResult <- promise.future
        } yield uploadResult
    }

    uploadAndWaitPackagesF
      .map(_ => ())
      .andThen { case _ =>
        killSwitch.shutdown()
        packageEntriesStreamDone
      }
  }
}
