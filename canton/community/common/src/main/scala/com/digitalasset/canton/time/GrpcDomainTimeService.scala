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
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil}
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Admin service to expose the time of domains to a participant and other nodes */
private[time] class GrpcDomainTimeService(
    lookupTimeTracker: Option[DomainId] => Either[String, DomainTimeTracker],
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
          .fromEither[Future](lookupTimeTracker(request.domainIdO))
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
          .fromEither[Future](lookupTimeTracker(request.domainIdO))
          .leftMap(Status.INVALID_ARGUMENT.withDescription)
        _ = logger.debug(
          s"Waiting for domain [${request.domainIdO}] to reach time ${request.timestamp} (is at ${timeTracker.latestTime})"
        )
        _ <- EitherT.liftF(
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
    domainIdO: Option[DomainId],
    freshnessBound: NonNegativeFiniteDuration,
) {
  def toProtoV30: v30.FetchTimeRequest =
    v30.FetchTimeRequest(domainIdO.map(_.toProtoPrimitive), freshnessBound.toProtoPrimitive.some)
}

object FetchTimeRequest {
  def fromProto(
      requestP: v30.FetchTimeRequest
  ): ParsingResult[FetchTimeRequest] =
    for {
      domainIdO <- requestP.domainId.traverse(DomainId.fromProtoPrimitive(_, "domainId"))
      freshnessBound <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("freshnessBound"),
        "freshnessBound",
        requestP.freshnessBound,
      )
    } yield FetchTimeRequest(domainIdO, freshnessBound)
}

final case class FetchTimeResponse(timestamp: CantonTimestamp) {
  def toProtoV30: v30.FetchTimeResponse =
    v30.FetchTimeResponse(timestamp.toProtoPrimitive.some)
}

object FetchTimeResponse {
  def fromProto(
      requestP: v30.FetchTimeResponse
  ): ParsingResult[FetchTimeResponse] =
    for {
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "timestamp",
        requestP.timestamp,
      )
    } yield FetchTimeResponse(timestamp)
}

final case class AwaitTimeRequest(domainIdO: Option[DomainId], timestamp: CantonTimestamp) {
  def toProtoV30: v30.AwaitTimeRequest =
    v30.AwaitTimeRequest(domainIdO.map(_.toProtoPrimitive), timestamp.toProtoPrimitive.some)
}

object AwaitTimeRequest {
  def fromProto(
      requestP: v30.AwaitTimeRequest
  ): ParsingResult[AwaitTimeRequest] =
    for {
      domainIdO <- requestP.domainId.traverse(DomainId.fromProtoPrimitive(_, "domainId"))
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "timestamp",
        requestP.timestamp,
      )
    } yield AwaitTimeRequest(domainIdO, timestamp)
}

object GrpcDomainTimeService {

  /** To use the time service for a participant a DomainId must be specified as a participant can be connected to many domains */
  def forParticipant(
      timeTrackerLookup: DomainId => Option[DomainTimeTracker],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): GrpcDomainTimeService = {
    new GrpcDomainTimeService(
      domainIdO =>
        for {
          domainId <- domainIdO.toRight(
            "Domain id must be specified to lookup a time on a participant"
          )
          timeTracker <- timeTrackerLookup(domainId).toRight(
            s"Time tracker for domain $domainId not found"
          )
        } yield timeTracker,
      loggerFactory,
    )
  }

  /** Domain entities have a constant domain id so always have the same time tracker and cannot fetch another */
  def forDomainEntity(
      domainId: DomainId,
      timeTracker: DomainTimeTracker,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): GrpcDomainTimeService = {
    new GrpcDomainTimeService(
      // allow none or the actual domainId to return the time tracker
      domainIdO =>
        for {
          _ <- EitherUtil.condUnitE(
            domainIdO.forall(_ == domainId),
            "Provided domain id does not match running domain",
          )
        } yield timeTracker,
      loggerFactory,
    )
  }
}
