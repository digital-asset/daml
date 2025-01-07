// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.service

import cats.data.EitherT
import com.digitalasset.canton.admin.grpc.{GrpcPruningScheduler, HasPruningScheduler}
import com.digitalasset.canton.admin.pruning.v30.LocatePruningTimestamp
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.mediator.Mediator.PruningError
import com.digitalasset.canton.synchronizer.mediator.{Mediator, MediatorPruningScheduler}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcMediatorAdministrationService(
    mediator: Mediator,
    pruningScheduler: MediatorPruningScheduler,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends v30.MediatorAdministrationServiceGrpc.MediatorAdministrationService
    with GrpcPruningScheduler
    with HasPruningScheduler
    with NamedLogging {

  override def prune(
      request: v30.MediatorPruning.PruneRequest
  ): Future[v30.MediatorPruning.PruneResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    EitherTUtil.toFuture {
      for {
        timestamp <- EitherT
          .fromEither[Future](
            ProtoConverter
              .parseRequired(CantonTimestamp.fromProtoTimestamp, "timestamp", request.timestamp)
          )
          .leftMap(err => exception(Status.INVALID_ARGUMENT, err.toString))
        _ <- mediator
          .prune(timestamp)
          .leftMap {
            case e: PruningError.CannotPruneAtTimestamp =>
              exception(Status.INVALID_ARGUMENT, e.message)

            case e: PruningError.MissingSynchronizerParametersForValidPruningTsComputation =>
              exception(Status.INTERNAL, e.message)

            case e @ PruningError.NoDataAvailableForPruning =>
              exception(Status.FAILED_PRECONDITION, e.message)
          }
          .onShutdown(Left(exception(Status.ABORTED, "Aborted due to shutdown.")))
      } yield v30.MediatorPruning.PruneResponse()
    }
  }

  override protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[PruningScheduler] = Future.successful(pruningScheduler)

  override def locatePruningTimestamp(
      request: LocatePruningTimestamp.Request
  ): Future[LocatePruningTimestamp.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      nonNegativeSkip <- NonNegativeInt
        .create(request.index - 1) // Translate 1-based index to 0-based skip used at store level
        .fold(
          e =>
            Future.failed(
              exception(
                Status.INVALID_ARGUMENT,
                e.message,
              )
            ),
          index => Future.successful(index),
        )

      timestamp <- mediator.stateInspection
        .locatePruningTimestamp(nonNegativeSkip)(
          traceContext,
          CloseContext(pruningScheduler),
        )
        .failOnShutdownTo(exception(Status.ABORTED, "Aborted due to shutdown."))
      // If we have just determined the oldest mediator response timestamp, report the metric
      _ = if (nonNegativeSkip.value == 0)
        mediator.stateInspection.reportMaxResponseAgeMetric(timestamp)
    } yield LocatePruningTimestamp.Response(timestamp.map(_.toProtoTimestamp))
  }

  private def exception(status: Status, message: String) =
    status.withDescription(message).asException()

}
