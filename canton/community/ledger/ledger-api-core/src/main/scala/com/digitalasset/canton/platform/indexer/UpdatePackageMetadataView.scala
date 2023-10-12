// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import akka.NotUsed
import akka.stream.*
import akka.stream.scaladsl.{Sink, Source}
import cats.Applicative
import com.daml.lf.archive.ArchiveParser
import com.daml.lf.data.Time
import com.daml.metrics.Timed
import com.daml.timer.FutureCheck.*
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.PackageId
import com.digitalasset.canton.platform.store.backend.PackageStorageBackend
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.packagemeta.{PackageMetadata, PackageMetadataView}
import com.digitalasset.canton.tracing.TraceContext

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
      loggerFactory: NamedLoggerFactory,
  )(implicit materializer: Materializer, traceContext: TraceContext): Future[Unit] = {
    val logger = loggerFactory.getTracedLogger(getClass)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace.empty
    implicit val ec: ExecutionContext = computationExecutionContext
    logger.info("Package Metadata View initialization has been started.")
    val startedTime = System.nanoTime()

    def loadLfArchive(
        packageId: Timestamped[PackageId]
    ): Future[Timestamped[(PackageId, Array[Byte])]] =
      packageId.traverse { packageId =>
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
      }

    def lfPackagesSource(): Future[Source[Timestamped[PackageId], NotUsed]] =
      dbDispatcher.executeSql(metrics.daml.index.db.loadPackages)(connection =>
        Source(packageStorageBackend.lfPackages(connection).map { case (pkgId, pkgDetails) =>
          Timestamped(pkgId, pkgDetails.knownSince)
        })
      )

    def toMetadataDefinition(
        timestampedPackageBytes: Timestamped[Array[Byte]]
    ): PackageMetadata = timestampedPackageBytes match {
      case Timestamped(packageBytes, timestamp) =>
        val archive = ArchiveParser.assertFromByteArray(packageBytes)
        Timed.value(
          metrics.daml.index.packageMetadata.decodeArchive,
          PackageMetadata.from(archive, timestamp),
        )
    }

    def processPackage(
        timestampedArchive: Timestamped[(PackageId, Array[Byte])]
    ): Future[PackageMetadata] = {
      val Timestamped((packageId, archive), timestamp) = timestampedArchive
      Future(toMetadataDefinition(Timestamped(archive, timestamp))).recoverWith {
        case NonFatal(e) =>
          logger.error(
            s"Failed to decode loaded LF Archive by packageId=$packageId",
            e,
          )
          Future.failed(e)
      }
    }

    def elapsedDurationNanos(): Long = System.nanoTime() - startedTime

    Source
      .futureSource(lfPackagesSource())
      .mapAsyncUnordered(config.initLoadParallelism)(loadLfArchive)
      .mapAsyncUnordered(config.initProcessParallelism)(processPackage)
      .runWith(Sink.foreach(packageMetadataView.update))
      .checkIfComplete(config.initTakesTooLongInitialDelay, config.initTakesTooLongInterval) {
        logger.warn(
          s"Package Metadata View initialization takes to long (${elapsedDurationNanos() / 1000000L} ms)"
        )
      }
      .map { _ =>
        val durationNanos = elapsedDurationNanos()
        metrics.daml.index.packageMetadata.viewInitialisation
          .update(durationNanos, TimeUnit.NANOSECONDS)
        logger.info(
          s"Package Metadata View has been initialized (${durationNanos / 1000000L} ms)"
        )
      }(computationExecutionContext)
      .recover { case NonFatal(e) =>
        logger.error(s"Failed to initialize Package Metadata View", e)
        throw e
      }
  }

  final case class Timestamped[T](value: T, timestamp: Time.Timestamp) {
    import cats.implicits.*
    def traverse[F[_], O](f: T => F[O])(implicit applicative: Applicative[F]): F[Timestamped[O]] =
      f(value).map(Timestamped(_, timestamp))
  }
}
