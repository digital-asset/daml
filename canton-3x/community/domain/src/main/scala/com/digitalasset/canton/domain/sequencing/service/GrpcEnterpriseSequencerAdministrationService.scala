// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.admin.grpc.{GrpcPruningScheduler, HasPruningScheduler}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.sequencing.sequencer.{LedgerIdentity, PruningError, Sequencer}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.pruning.admin.v0.LocatePruningTimestamp
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.empty.Empty
import io.grpc.{Status, StatusException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcEnterpriseSequencerAdministrationService(
    sequencer: Sequencer,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationService
    with GrpcPruningScheduler
    with HasPruningScheduler
    with NamedLogging {

  /** Remove data from the Sequencer */
  override def prune(req: v0.Pruning.Request): Future[v0.Pruning.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture[StatusException, v0.Pruning.Response] {
      for {
        requestedTimestamp <- EitherT
          .fromEither[Future](
            ProtoConverter
              .parseRequired(CantonTimestamp.fromProtoPrimitive, "timestamp", req.timestamp)
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
      } yield v0.Pruning.Response(details)
    }
  }

  override def snapshot(request: v0.Snapshot.Request): Future[v0.Snapshot.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      timestamp <- EitherT
        .fromEither[Future](
          ProtoConverter
            .parseRequired(CantonTimestamp.fromProtoPrimitive, "timestamp", request.timestamp)
        )
        .leftMap(_.toString)
      result <- sequencer.snapshot(timestamp)
    } yield result)
      .fold[v0.Snapshot.Response](
        error =>
          v0.Snapshot.Response(v0.Snapshot.Response.Value.Failure(v0.Snapshot.Failure(error))),
        result =>
          v0.Snapshot.Response(
            v0.Snapshot.Response.Value.VersionedSuccess(
              v0.Snapshot.VersionedSuccess(result.toProtoVersioned.toByteString)
            )
          ),
      )
  }

  override def disableMember(requestP: v0.DisableMemberRequest): Future[Empty] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture[StatusException, Empty] {
      for {
        member <- EitherT.fromEither[Future](
          Member
            .fromProtoPrimitive(requestP.member, "member")
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString).asException())
        )
        _ <- EitherT.right(sequencer.disableMember(member))
      } yield Empty()
    }
  }

  override def authorizeLedgerIdentity(
      request: v0.LedgerIdentity.AuthorizeRequest
  ): Future[v0.LedgerIdentity.AuthorizeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      identity <- ProtoConverter
        .parseRequired(LedgerIdentity.fromProtoV0, "identity", request.identify)
        .leftMap(_.toString)
        .toEitherT[Future]
      result <- sequencer.authorizeLedgerIdentity(identity)
    } yield result
    res.fold(
      err =>
        v0.LedgerIdentity.AuthorizeResponse(
          v0.LedgerIdentity.AuthorizeResponse.Value.Failure(v0.LedgerIdentity.Failure(err))
        ),
      _ =>
        v0.LedgerIdentity.AuthorizeResponse(
          v0.LedgerIdentity.AuthorizeResponse.Value.Success(v0.LedgerIdentity.Success())
        ),
    )
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
      } yield LocatePruningTimestamp.Response(ts.map(_.toProtoPrimitive))
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
