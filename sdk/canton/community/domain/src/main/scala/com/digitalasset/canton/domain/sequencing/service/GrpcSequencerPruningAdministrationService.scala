// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.admin.grpc.{GrpcPruningScheduler, HasPruningScheduler}
import com.digitalasset.canton.admin.pruning.v30.LocatePruningTimestamp
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.{PruningError, Sequencer}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.{Status, StatusException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerPruningAdministrationService(
    sequencer: Sequencer,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationService
    with GrpcPruningScheduler
    with HasPruningScheduler
    with NamedLogging {

  /** Remove data from the Sequencer */
  override def prune(
      req: v30.SequencerPruning.PruneRequest
  ): Future[v30.SequencerPruning.PruneResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture[StatusException, v30.SequencerPruning.PruneResponse] {
      for {
        requestedTimestamp <- EitherT
          .fromEither[Future](
            ProtoConverter
              .parseRequired(CantonTimestamp.fromProtoTimestamp, "timestamp", req.timestamp)
          )
          .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString).asException())
        details <- sequencer
          .prune(requestedTimestamp)
          .leftMap {
            case e @ PruningError.NotSupported =>
              Status.UNIMPLEMENTED.withDescription(e.message).asException()

            case e: PruningError.UnsafePruningPoint =>
              Status.FAILED_PRECONDITION.withDescription(e.message).asException()
          }
      } yield v30.SequencerPruning.PruneResponse(details)
    }
  }
  override def locatePruningTimestamp(
      request: LocatePruningTimestamp.Request
  ): Future[LocatePruningTimestamp.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    EitherTUtil.toFuture[StatusException, LocatePruningTimestamp.Response] {
      for {
        index <- EitherT
          .fromEither[Future](
            PositiveInt
              .create(request.index)
              .leftMap(e => Status.INVALID_ARGUMENT.withDescription(e.message).asException())
          )
        ts <- sequencer
          .locatePruningTimestamp(index)
          .leftMap(e => Status.UNIMPLEMENTED.withDescription(e.message).asException())
        // If we just fetched the oldest event, take the opportunity to report the max-event-age metric
        _ = if (index.value == 1) sequencer.reportMaxEventAgeMetric(ts)
      } yield LocatePruningTimestamp.Response(ts.map(_.toProtoTimestamp))
    }
  }

  override protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[PruningScheduler] = sequencer.pruningScheduler match {
    case None =>
      Future.failed(
        Status.UNIMPLEMENTED.withDescription(PruningError.NotSupported.message).asException()
      )
    case Some(scheduler) => Future.successful(scheduler)
  }
}
