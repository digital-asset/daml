// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.metrics.Timed
import com.daml.timer.FutureCheck.*
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.PackageId
import com.digitalasset.canton.platform.packages.PackageUpgradeValidator
import com.digitalasset.canton.platform.store.backend.PackageStorageBackend
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.packagemeta.{PackageMetadata, PackageMetadataView}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object UpdatePackageMetadataView {
  def apply(
      packageStorageBackend: PackageStorageBackend,
      metrics: Metrics,
      dbDispatcher: DbDispatcher,
      packageMetadataView: PackageMetadataView,
      computationExecutionContext: ExecutionContext,
      config: PackageMetadataViewConfig,
      disableUpgradeValidation: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit materializer: Materializer, traceContext: TraceContext): Future[Unit] = {

    val logger = loggerFactory.getTracedLogger(getClass)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace.empty
    implicit val ec: ExecutionContext = computationExecutionContext

    logger.debug("Package Metadata View initialization has been started.")
    val startedTime = System.nanoTime()

    def loadLfArchive(
        packageId: PackageId
    ): Future[(PackageId, Array[Byte])] =
      dbDispatcher
        .executeSql(metrics.daml.index.db.loadArchive)(connection =>
          packageStorageBackend
            .lfArchive(packageId)(connection)
            .getOrElse(
              // should never happen as we received a reference to packageId
              sys.error(s"LfArchive does not exist by packageId=$packageId")
            )
        )
        .map(packageId -> _)

    def lfPackagesSource(): Future[Source[PackageId, NotUsed]] =
      dbDispatcher.executeSql(metrics.daml.index.db.loadPackages)(connection =>
        Source(packageStorageBackend.lfPackages(connection).map { case (pkgId, _) => pkgId })
      )

    def toMetadataDefinition(
        packageBytes: Array[Byte]
    ): (PackageMetadata, PackageId, Ast.Package) = {
      val archive = ArchiveParser.assertFromByteArray(packageBytes)
      Timed.value(
        metrics.daml.index.packageMetadata.decodeArchive,
        PackageMetadata.from(archive),
      )
    }

    def processPackage(
        archive: (PackageId, Array[Byte])
    ): Future[(PackageMetadata, PackageId, Ast.Package)] = {
      val (packageId, archiveBytes) = archive
      Future(toMetadataDefinition(archiveBytes)).recoverWith { case NonFatal(e) =>
        logger.error(
          s"Failed to decode loaded LF Archive by packageId=$packageId",
          e,
        )
        Future.failed(e)
      }
    }

    def elapsedDurationNanos(): Long = System.nanoTime() - startedTime

    def checkLoadedPackages(loadedPackages: Vector[(PackageId, Ast.Package)]): Future[Unit] =
      if (disableUpgradeValidation) {
        logger.debug("Skipping loaded packages upgrade compatibility check.")
        Future.unit
      } else {
        logger.debug("Checking loaded packages for upgrade compatibility.")
        val upgradeCompatibilityCheckStartTime = System.nanoTime()
        PackageUpgradeValidator
          .validateSelfContainedPackageListUpgrade(loggerFactory, loadedPackages.toList)
          .map { _ =>
            val upgradeCompatibilityCheckDuration =
              System.nanoTime() - upgradeCompatibilityCheckStartTime

            logger.info(
              s"Checking loaded packages for upgrade compatibility finished after (${upgradeCompatibilityCheckDuration / 1000000L} ms)"
            )
          }
          .valueOr { error =>
            logger.error(
              s"Package upgrade validation failed on Indexer start-up. Crashing the participant. Please contact support.",
              error.asGrpcError,
            )
            sys.exit(1)
          }
      }

    Source
      .futureSource(lfPackagesSource())
      .mapAsyncUnordered(config.initLoadParallelism)(loadLfArchive)
      .mapAsyncUnordered(config.initProcessParallelism)(processPackage)
      .runFold(Vector.empty[(Ref.PackageId, Ast.Package)]) {
        case (acc, (pkgMetadata, packageId, pkg)) =>
          packageMetadataView.update(pkgMetadata)
          acc :+ (packageId -> pkg)
      }
      .checkIfComplete(config.initTakesTooLongInitialDelay, config.initTakesTooLongInterval) {
        logger.warn(
          s"Package Metadata View initialization takes to long (${elapsedDurationNanos() / 1000000L} ms)"
        )
      }
      .flatMap { loadedPackages =>
        val durationNanos = elapsedDurationNanos()
        metrics.daml.index.packageMetadata.viewInitialisation
          .update(durationNanos, TimeUnit.NANOSECONDS)
        logger.info(
          s"Package Metadata View has been initialized (${durationNanos / 1000000L} ms)"
        )

        checkLoadedPackages(loadedPackages)
      }(computationExecutionContext)
      .recover { case NonFatal(e) =>
        logger.error(s"Failed to initialize Package Metadata View", e)
        throw e
      }
  }
}
