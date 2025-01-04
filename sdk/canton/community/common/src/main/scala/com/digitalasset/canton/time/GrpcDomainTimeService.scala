// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.admin.v30
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Admin service to expose the time of domains to a participant and other nodes */
private[time] class GrpcDomainTimeService(
    lookupTimeTracker: Option[SynchronizerId] => Either[String, DomainTimeTracker],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends v30.DomainTimeServiceGrpc.DomainTimeService
    with NamedLogging {

  override def fetchTime(requestP: v30.FetchTimeRequest): Future[v30.FetchTimeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    handle {
      for {
        request <- EitherT
          .fromEither[Future](FetchTimeRequest.fromProto(requestP))
          .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        timeTracker <- EitherT
          .fromEither[Future](lookupTimeTracker(request.synchronizerIdO))
          .leftMap(Status.INVALID_ARGUMENT.withDescription)
        timestamp <- EitherT(
          timeTracker
            .fetchTime(request.freshnessBound)
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
        request <- EitherT
          .fromEither[Future](AwaitTimeRequest.fromProto(requestP))
          .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        timeTracker <- EitherT
          .fromEither[Future](lookupTimeTracker(request.synchronizerIdO))
          .leftMap(Status.INVALID_ARGUMENT.withDescription)
        _ = logger.debug(
          s"Waiting for domain [${request.synchronizerIdO}] to reach time ${request.timestamp} (is at ${timeTracker.latestTime})"
        )
        _ <- EitherT.right(
          timeTracker
            .awaitTick(request.timestamp)
            .fold(Future.unit)(_.void)
        )
      } yield v30.AwaitTimeResponse()
    }
  }

  private def handle[A](handler: EitherT[Future, Status, A]): Future[A] =
    EitherTUtil.toFuture(handler.leftMap(_.asRuntimeException()))
}

final case class FetchTimeRequest(
    synchronizerIdO: Option[SynchronizerId],
    freshnessBound: NonNegativeFiniteDuration,
) {
  def toProtoV30: v30.FetchTimeRequest =
    v30.FetchTimeRequest(
      synchronizerIdO.map(_.toProtoPrimitive),
      freshnessBound.toProtoPrimitive.some,
    )
}

object FetchTimeRequest {
  def fromProto(
      requestP: v30.FetchTimeRequest
  ): ParsingResult[FetchTimeRequest] =
    for {
      synchronizerIdO <- requestP.synchronizerId.traverse(
        SynchronizerId.fromProtoPrimitive(_, "synchronizerId")
      )
      freshnessBound <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("freshnessBound"),
        "freshnessBound",
        requestP.freshnessBound,
      )
    } yield FetchTimeRequest(synchronizerIdO, freshnessBound)
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

final case class AwaitTimeRequest(
    synchronizerIdO: Option[SynchronizerId],
    timestamp: CantonTimestamp,
) {
  def toProtoV30: v30.AwaitTimeRequest =
    v30.AwaitTimeRequest(synchronizerIdO.map(_.toProtoPrimitive), timestamp.toProtoTimestamp.some)
}

object AwaitTimeRequest {
  def fromProto(
      requestP: v30.AwaitTimeRequest
  ): ParsingResult[AwaitTimeRequest] =
    for {
      synchronizerIdO <- requestP.synchronizerId.traverse(
        SynchronizerId.fromProtoPrimitive(_, "synchronizerId")
      )
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "timestamp",
        requestP.timestamp,
      )
    } yield AwaitTimeRequest(synchronizerIdO, timestamp)
}

object GrpcDomainTimeService {

  /** To use the time service for a participant a SynchronizerId must be specified as a participant can be connected to many domains */
  def forParticipant(
      timeTrackerLookup: SynchronizerId => Option[DomainTimeTracker],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): GrpcDomainTimeService =
    new GrpcDomainTimeService(
      synchronizerIdO =>
        for {
          synchronizerId <- synchronizerIdO.toRight(
            "Synchronizer id must be specified to lookup a time on a participant"
          )
          timeTracker <- timeTrackerLookup(synchronizerId).toRight(
            s"Time tracker for domain $synchronizerId not found"
          )
        } yield timeTracker,
      loggerFactory,
    )

  /** Domain entities have a constant synchronizer id so always have the same time tracker and cannot fetch another */
  def forDomainEntity(
      synchronizerId: SynchronizerId,
      timeTracker: DomainTimeTracker,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): GrpcDomainTimeService =
    new GrpcDomainTimeService(
      // allow none or the actual synchronizerId to return the time tracker
      synchronizerIdO =>
        for {
          _ <- Either.cond(
            synchronizerIdO.forall(_ == synchronizerId),
            (),
            "Provided synchronizer id does not match running domain",
          )
        } yield timeTracker,
      loggerFactory,
    )
}
