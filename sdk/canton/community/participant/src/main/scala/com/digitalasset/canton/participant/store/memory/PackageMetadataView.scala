// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.implicits.catsSyntaxSemigroup
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.archive.Decode
import com.daml.timer.FutureCheck.*
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.{CommonErrors, PackageServiceErrors}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.platform.indexer.PackageMetadataViewConfig
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.Implicits.packageMetadataSemigroup
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** In-memory view of Daml-related package metadata
  * (see [[com.digitalasset.canton.platform.store.packagemeta.PackageMetadata]])
  * for all packages stored on the current participant.
  */
trait PackageMetadataView extends AutoCloseable {
  def getSnapshot(implicit contextualizedErrorLogger: ContextualizedErrorLogger): PackageMetadata
}

/** Exposes mutable accessors to the [[PackageMetadataView]] to be used
  * only during state initialization and on new package uploads.
  */
trait MutablePackageMetadataView extends PackageMetadataView {
  def update(other: PackageMetadata)(implicit tc: TraceContext): Unit

  /** Re-initialize the package metadata view state from the underlying Daml packages store.
    *
    * Note: Not thread-safe!
    */
  def refreshState(implicit tc: TraceContext): FutureUnlessShutdown[Unit]
}

class MutablePackageMetadataViewImpl(
    clock: Clock,
    damlPackageStore: DamlPackageStore,
    val loggerFactory: NamedLoggerFactory,
    packageMetadataViewConfig: PackageMetadataViewConfig,
    val timeouts: ProcessingTimeout,
)(implicit val actorSystem: ActorSystem, executionContext: ExecutionContext)
    extends MutablePackageMetadataView
    with FlagCloseable
    with NamedLogging {
  private val loggingSubject = "Package Metadata View"
  private val packageMetadataRef: AtomicReference[Option[PackageMetadata]] =
    new AtomicReference(None)

  def update(other: PackageMetadata)(implicit tc: TraceContext): Unit =
    packageMetadataRef.updateAndGet {
      case Some(packageMetadata) => Some(packageMetadata |+| other)
      case None =>
        throw CommonErrors.ServiceInternalError
          .Generic(s"$loggingSubject is not initialized")
          .asGrpcError
    }.discard

  def refreshState(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingF(s"Refreshing $loggingSubject") {
      val startedTime = clock.now
      def elapsedDurationMillis(): Long = (clock.now - startedTime).toMillis

      val initializationF = Source
        .future(damlPackageStore.listPackages())
        .mapConcat(identity)
        .mapAsyncUnordered(packageMetadataViewConfig.initLoadParallelism) { pkgDesc =>
          logger.debug(s"Fetching package ${pkgDesc.packageId}")
          fetchPackage(pkgDesc.packageId)
        }
        .mapAsyncUnordered(packageMetadataViewConfig.initProcessParallelism) { archive =>
          logger.debug(s"Decoding archive ${archive.getHash} to package metadata")
          decodePackageMetadata(archive)
        }
        .runFold(PackageMetadata())(_ |+| _)
        .map(initialized => packageMetadataRef.set(Some(initialized)))

      initializationF
        .checkIfComplete(
          packageMetadataViewConfig.initTakesTooLongInitialDelay,
          packageMetadataViewConfig.initTakesTooLongInterval,
        ) {
          logger.warn(
            s"$loggingSubject initialization takes too long (${elapsedDurationMillis()} ms)"
          )
        }
        .map { _ =>
          logger.info(
            s"$loggingSubject has been initialized (${elapsedDurationMillis()} ms)"
          )
        }
    }

  def getSnapshot(implicit contextualizedErrorLogger: ContextualizedErrorLogger): PackageMetadata =
    packageMetadataRef
      .get()
      .getOrElse(
        throw PackageServiceErrors.InternalError
          .Generic(s"$loggingSubject is not initialized.")
          .asGrpcError
      )

  private def decodePackageMetadata(
      archive: DamlLf.Archive
  )(implicit tc: TraceContext): Future[PackageMetadata] =
    PackageService
      .catchUpstreamErrors(Decode.decodeArchive(archive))
      .onShutdown(Left(CommonErrors.ServerIsShuttingDown.Reject()))
      .leftSemiflatMap(err => Future.failed(err.asGrpcError))
      .merge
      .map { case (pkgId, pkg) => PackageMetadata.from(pkgId, pkg) }

  private def fetchPackage(packageId: LfPackageId)(implicit tc: TraceContext) =
    damlPackageStore
      .getPackage(packageId)
      .flatMap {
        case Some(pkg) => Future.successful(pkg)
        case None =>
          Future.failed(PackageServiceErrors.InternalError.Error(Set(packageId)).asGrpcError)
      }
}

object NoopPackageMetadataView extends MutablePackageMetadataView {
  override def refreshState(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit
  override def getSnapshot(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PackageMetadata = PackageMetadata()
  override def update(other: PackageMetadata)(implicit tc: TraceContext): Unit = ()
  override def close(): Unit = ()
}
