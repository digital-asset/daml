// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.participant.v30.{
  AddPartyAsyncRequest,
  AddPartyAsyncResponse,
  PartyManagementServiceGrpc,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

/** grpc service to allow modifying party hosting on participants
  */
class GrpcPartyManagementService(
    adminWorkflow: PartyReplicationAdminWorkflow,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PartyManagementServiceGrpc.PartyManagementService
    with NamedLogging {

  override def addPartyAsync(
      request: AddPartyAsyncRequest
  ): Future[AddPartyAsyncResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    def toStatusRuntimeException(status: Status)(err: String): StatusRuntimeException =
      status.withDescription(err).asRuntimeException()

    EitherTUtil.toFuture(for {
      args <- EitherT.fromEither[Future](
        verifyArguments(request).leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )

      hash <- adminWorkflow.partyReplicator
        .addPartyAsync(args, adminWorkflow)
        .leftMap(toStatusRuntimeException(Status.FAILED_PRECONDITION))
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield AddPartyAsyncResponse(partyReplicationId = hash.toHexString))
  }

  private def verifyArguments(
      request: AddPartyAsyncRequest
  ): Either[String, PartyReplicationArguments] =
    for {
      partyId <- convert(request.partyUid, "partyUid", PartyId(_))
      sourceParticipantIdO <- Option
        .when(request.sourceParticipantUid.nonEmpty)(request.sourceParticipantUid)
        .traverse(convert(_, "sourceParticipantUid", ParticipantId(_)))
      synchronizerId <- convert(
        request.synchronizerId,
        "synchronizer_id",
        SynchronizerId(_),
      )
      serialO <- Option
        .when(request.serial != 0)(request.serial)
        .traverse(ProtoConverter.parsePositiveInt("serial", _).leftMap(_.message))

    } yield PartyReplicationArguments(partyId, synchronizerId, sourceParticipantIdO, serialO)

  private def convert[T](
      rawId: String,
      field: String,
      wrap: UniqueIdentifier => T,
  ): Either[String, T] =
    UniqueIdentifier.fromProtoPrimitive(rawId, field).bimap(_.toString, wrap)
}
