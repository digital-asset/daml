// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.admin.v30
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, Synchronizer}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Admin service to expose the time of synchronizers to a participant and other nodes
  * @param lookupTimeTracker
  *   Lookup the time tracker
  * @tparam SId
  *   Participant nodes can serve requests for LSId and PSId but synchronizer nodes only serve
  *   requests for PSId.
  */
private[time] class GrpcSynchronizerTimeService(
    lookupTimeTracker: Option[Synchronizer] => Either[String, SynchronizerTimeTracker],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends v30.SynchronizerTimeServiceGrpc.SynchronizerTimeService
    with NamedLogging {

  override def fetchTime(requestP: v30.FetchTimeRequest): Future[v30.FetchTimeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    handle {
      for {
        synchronizerO <- EitherT.fromEither[Future](
          requestP.synchronizer
            .traverse(Synchronizer.fromProtoV30)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        )

        freshnessBound <- EitherT.fromEither[Future](
          ProtoConverter
            .parseRequired(
              NonNegativeFiniteDuration.fromProtoPrimitive("freshnessBound"),
              "freshnessBound",
              requestP.freshnessBound,
            )
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        )

        timeTracker <- EitherT
          .fromEither[Future](lookupTimeTracker(synchronizerO))
          .leftMap(
            Status.INVALID_ARGUMENT.withDescription
          )

        timestamp <- EitherT(
          timeTracker
            .fetchTime(freshnessBound)
            .map(Right(_))
            .onShutdown(Left(Status.ABORTED.withDescription("shutdown")))
        )
      } yield FetchTimeResponse(timestamp).toProtoV30
    }
  }

  override def awaitTime(requestP: v30.AwaitTimeRequest): Future[v30.AwaitTimeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    handle {
      for {
        synchronizerO <- EitherT.fromEither[Future](
          requestP.synchronizer
            .traverse(Synchronizer.fromProtoV30)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        )

        timestamp <- EitherT.fromEither[Future](
          ProtoConverter
            .parseRequired(
              CantonTimestamp.fromProtoTimestamp,
              "timestamp",
              requestP.timestamp,
            )
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        )

        timeTracker <- EitherT
          .fromEither[Future](lookupTimeTracker(synchronizerO))
          .leftMap(
            Status.INVALID_ARGUMENT.withDescription
          )
        _ = logger.debug(
          s"Waiting for synchronizer [$synchronizerO] to reach time $timestamp (is at ${timeTracker.latestTime})"
        )
        _ <- EitherT.right(
          timeTracker.awaitTick(timestamp).getOrElse(Future.unit)
        )
      } yield v30.AwaitTimeResponse()
    }
  }

  private def handle[A](handler: EitherT[Future, Status, A]): Future[A] =
    EitherTUtil.toFuture(handler.leftMap(_.asRuntimeException()))
}

final case class FetchTimeResponse(timestamp: CantonTimestamp) {
  def toProtoV30: v30.FetchTimeResponse =
    v30.FetchTimeResponse(timestamp.toProtoTimestamp.some)
}

object FetchTimeResponse {
  def fromProto(
      requestP: v30.FetchTimeResponse
  ): ParsingResult[FetchTimeResponse] =
    for {
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "timestamp",
        requestP.timestamp,
      )
    } yield FetchTimeResponse(timestamp)
}

object GrpcSynchronizerTimeService {

  /** To use the time service for a participant a SynchronizerId must be specified as a participant
    * can be connected to many synchronizers
    */
  def forParticipant(
      timeTrackerLookup: Synchronizer => Either[String, SynchronizerTimeTracker],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): GrpcSynchronizerTimeService =
    new GrpcSynchronizerTimeService(
      requestSynchronizerO =>
        for {
          requestSynchronizer <- requestSynchronizerO.toRight(
            "Synchronizer must be specified to lookup a time on a participant"
          )
          timeTracker <- timeTrackerLookup(requestSynchronizer)
        } yield timeTracker,
      loggerFactory,
    )

  /** synchronizer entities have a constant synchronizer id so always have the same time tracker and
    * cannot fetch another
    */
  def forSynchronizerEntity(
      psid: PhysicalSynchronizerId,
      timeTracker: SynchronizerTimeTracker,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): GrpcSynchronizerTimeService =
    new GrpcSynchronizerTimeService(
      // allow none or the actual synchronizerId to return the time tracker
      requestPSIdO =>
        for {
          _ <- Either.cond(
            requestPSIdO.forall(_.isCompatibleWith(psid)),
            (),
            "Provided synchronizer id does not match running synchronizer",
          )
        } yield timeTracker,
      loggerFactory,
    )
}
