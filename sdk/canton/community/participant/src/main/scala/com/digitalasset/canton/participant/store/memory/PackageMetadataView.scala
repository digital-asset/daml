// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.implicits.catsSyntaxSemigroup
import com.daml.timer.FutureCheck.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{PackageMetadataViewConfig, ProcessingTimeout}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.{CommonErrors, PackageServiceErrors}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.store.packagemeta.PackageMetadata.Implicits.packageMetadataSemigroup
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.digitalasset.canton.{LfPackageId, LfPackageRef}
import com.digitalasset.daml.lf.archive.{DamlLf, Decode}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** In-memory view of Daml-related package metadata (see
  * [[com.digitalasset.canton.store.packagemeta.PackageMetadata]]) for all packages stored on the
  * current participant.
  */
trait PackageMetadataView extends AutoCloseable {
  def getSnapshot(implicit errorLoggingContext: ErrorLoggingContext): PackageMetadata
  val packageUpgradeValidator: PackageUpgradeValidator
}

/** Exposes mutable accessors to the [[PackageMetadataView]] to be used only during state
  * initialization and on new package uploads.
  */
trait MutablePackageMetadataView extends PackageMetadataView {

  /** Update the current package metadata view by merging the series of `newPackageMetadata` into
    * it.
    */
  def updateMany(newPackagesMetadata: Seq[PackageMetadata])(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Re-initialize the package metadata view state from the underlying Daml packages store. */
  def refreshState(implicit tc: TraceContext): FutureUnlessShutdown[Unit]
}

class MutablePackageMetadataViewImpl(
    clock: Clock,
    damlPackageStore: DamlPackageStore,
    val packageUpgradeValidator: PackageUpgradeValidator,
    val loggerFactory: NamedLoggerFactory,
    packageMetadataViewConfig: PackageMetadataViewConfig,
    val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
)(implicit val actorSystem: ActorSystem, executionContext: ExecutionContext)
    extends MutablePackageMetadataView
    with FlagCloseable
    with NamedLogging {
  private val loggingSubject = "Package Metadata View"
  private val packageMetadataRef: AtomicReference[Option[PackageMetadata]] =
    new AtomicReference(None)

  private val mutatePackageMetadataExecutionQueue = new SimpleExecutionQueue(
    "sequential-upload-dar-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  def updateMany(newPackagesMetadata: Seq[PackageMetadata])(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] =
    mutatePackageMetadataExecutionQueue.execute(
      execution = Future {
        newPackagesMetadata.foreach(other =>
          packageMetadataRef.updateAndGet {
            case Some(packageMetadata) => Some(packageMetadata |+| other)
            case None =>
              throw CommonErrors.ServiceInternalError
                .Generic(s"$loggingSubject is not initialized")
                .asGrpcError
          }.discard
        )
      },
      description = s"update $loggingSubject",
    )

  def refreshState(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    synchronizeWithClosing(s"Refreshing $loggingSubject") {
      mutatePackageMetadataExecutionQueue.executeUS(
        execution = refreshStateInternal,
        description = s"refresh $loggingSubject",
      )
    }

  private def refreshStateInternal(implicit tc: TraceContext) = {
    val startedTime = clock.now

    def elapsedDurationMillis(): Long = (clock.now - startedTime).toMillis

    val initializationFUS =
      damlPackageStore
        .listPackages()
        .flatMap(packages =>
          FutureUnlessShutdown.outcomeF(
            Source(packages)
              .mapAsyncUnordered(packageMetadataViewConfig.initLoadParallelism) { pkgDesc =>
                logger.debug(s"Fetching package ${pkgDesc.packageId}")
                fetchPackage(pkgDesc.packageId).asGrpcFuture
              }
              .mapAsyncUnordered(packageMetadataViewConfig.initProcessParallelism) { archive =>
                logger.debug(s"Decoding archive ${archive.getHash} to package metadata")
                decodePackageMetadata(archive)
              }
              .runFold(PackageMetadata())(_ |+| _)
              .map(initialized => packageMetadataRef.set(Some(initialized)))
          )
        )

    FutureUnlessShutdown(
      initializationFUS.unwrap
        .checkIfComplete(
          packageMetadataViewConfig.initTakesTooLongInitialDelay,
          packageMetadataViewConfig.initTakesTooLongInterval,
        ) {
          logger.warn(
            s"$loggingSubject initialization takes too long (${elapsedDurationMillis()} ms)"
          )
        }
        .map { result =>
          logger.info(
            s"$loggingSubject has been initialized (${elapsedDurationMillis()} ms)"
          )
          result
        }
    )

  }

  def getSnapshot(implicit errorLoggingContext: ErrorLoggingContext): PackageMetadata =
    packageMetadataRef
      .get()
      .getOrElse(
        throw PackageServiceErrors.InternalError
          .Generic(s"$loggingSubject is not initialized.")
          .asGrpcError
      )

  override def onClosed(): Unit =
    LifeCycle.close(mutatePackageMetadataExecutionQueue)(logger)

  private def decodePackageMetadata(
      archive: DamlLf.Archive
  )(implicit tc: TraceContext): Future[PackageMetadata] =
    PackageService
      .catchUpstreamErrors(Decode.decodeArchive(archive))
      .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error()))
      .leftSemiflatMap(err => Future.failed(err.asGrpcError))
      .merge
      .map { case (pkgId, pkg) => PackageMetadata.from(pkgId, pkg) }

  private def fetchPackage(
      packageId: LfPackageId
  )(implicit tc: TraceContext): FutureUnlessShutdown[DamlLf.Archive] =
    damlPackageStore
      .getPackage(packageId)
      .flatMap {
        case Some(pkg) => FutureUnlessShutdown.pure(pkg)
        case None =>
          FutureUnlessShutdown.failed(
            PackageServiceErrors.InternalError.Error(Set(LfPackageRef.Id(packageId))).asGrpcError
          )
      }
}
