// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.admin.participant.v30.{
  PartyManagementServiceGrpc,
  StartPartyReplicationRequest,
  StartPartyReplicationResponse,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.admin.PartyReplicationCoordinator
import com.digitalasset.canton.participant.admin.PartyReplicationCoordinator.{
  ChannelId,
  PartyReplicationArguments,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.{Status, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** grpc service to allow modifying party hosting on participants
  */
class GrpcPartyManagementService(
    coordinator: PartyReplicationCoordinator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PartyManagementServiceGrpc.PartyManagementService
    with NamedLogging {

  override def startPartyReplication(
      request: StartPartyReplicationRequest
  ): Future[StartPartyReplicationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    def toStatusRuntimeException(status: Status)(err: String): StatusRuntimeException =
      status.withDescription(err).asRuntimeException()

    EitherTUtil.toFuture(for {
      args <- EitherT.fromEither[Future](
        verifyArguments(request).leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )

      _ <- coordinator
        .startPartyReplication(args)
        .leftMap(toStatusRuntimeException(Status.FAILED_PRECONDITION))
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield StartPartyReplicationResponse())
  }

  private def verifyArguments(
      request: StartPartyReplicationRequest
  ): Either[String, PartyReplicationArguments] =
    for {
      id <- ChannelId.fromString(request.id.getOrElse(UUID.randomUUID().toString))
      partyId <- convert(request.partyUid, "partyUid", PartyId(_))
      sourceParticipantId <- convert(
        request.sourceParticipantUid,
        "sourceParticipantUid",
        ParticipantId(_),
      )
      synchronizerId <- convert(
        request.domainUid,
        "domainUid",
        SynchronizerId(_),
      )
    } yield PartyReplicationArguments(id, partyId, sourceParticipantId, synchronizerId)

  private def convert[T](
      rawId: String,
      field: String,
      wrap: UniqueIdentifier => T,
  ): Either[String, T] =
    UniqueIdentifier.fromProtoPrimitive(rawId, field).bimap(_.toString, wrap)
}
