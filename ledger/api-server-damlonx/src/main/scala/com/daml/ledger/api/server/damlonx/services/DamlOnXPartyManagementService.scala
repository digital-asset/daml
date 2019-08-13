// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.stream.Materializer
import com.daml.ledger.participant.state.index.v1.{
  PartyManagementService => IndexPartyManagementService
}

import com.daml.ledger.participant.state.v1.{PartyAllocationResult, WritePartyService}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.ServerServiceDefinition
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

class DamlOnXPartyManagementService private (
    writeService: WritePartyService,
    indexService: IndexPartyManagementService)
    extends PartyManagementService
    with GrpcApiService {

  protected val logger = LoggerFactory.getLogger(this.getClass)
  implicit val ec: ExecutionContext = DirectExecutionContext

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def getParticipantId(
      request: GetParticipantIdRequest): Future[GetParticipantIdResponse] =
    indexService.getParticipantId
      .map(pid => GetParticipantIdResponse(pid.toString))

  private[this] def mapPartyDetails(
      details: com.digitalasset.ledger.api.domain.PartyDetails): PartyDetails =
    PartyDetails(details.party, details.displayName.getOrElse(""), details.isLocal)

  override def listKnownParties(
      request: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    indexService.listParties
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val party = if (request.partyIdHint.isEmpty) None else Some(request.partyIdHint)
    val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)

    FutureConverters
      .toScala(
        writeService
          .allocateParty(party, displayName))
      .flatMap {
        case PartyAllocationResult.Ok(details) =>
          Future.successful(AllocatePartyResponse(Some(mapPartyDetails(details))))
        case r @ PartyAllocationResult.AlreadyExists =>
          Future.failed(ErrorFactories.invalidArgument(r.description))
        case r @ PartyAllocationResult.InvalidName(_) =>
          Future.failed(ErrorFactories.invalidArgument(r.description))
        case r @ PartyAllocationResult.ParticipantNotAuthorized =>
          Future.failed(ErrorFactories.permissionDenied(r.description))
        case r @ PartyAllocationResult.NotSupported =>
          Future.failed(ErrorFactories.unimplemented(r.description))
      }
  }

}

object DamlOnXPartyManagementService {
  def apply(writeService: WritePartyService, indexService: IndexPartyManagementService)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): GrpcApiService =
    new DamlOnXPartyManagementService(writeService, indexService) with PartyManagementServiceLogging
}
