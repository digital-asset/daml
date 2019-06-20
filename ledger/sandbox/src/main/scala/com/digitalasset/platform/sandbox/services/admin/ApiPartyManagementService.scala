// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import akka.stream.Materializer
import com.daml.ledger.participant.state.index.v2.IndexPartyManagementService
import com.daml.ledger.participant.state.v2.{PartyAllocationResult, WritePartyService}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    writeService: WritePartyService
) extends PartyManagementService
    with GrpcApiService {

  protected val logger = LoggerFactory.getLogger(this.getClass)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, DE)

  override def getParticipantId(
      request: GetParticipantIdRequest): Future[GetParticipantIdResponse] =
    partyManagementService
      .getParticipantId()
      .map(pid => GetParticipantIdResponse(pid.unwrap))(DE)

  private[this] def mapPartyDetails(
      details: com.digitalasset.ledger.api.domain.PartyDetails): PartyDetails =
    PartyDetails(details.party, details.displayName.getOrElse(""), details.isLocal)

  override def listKnownParties(
      request: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    partyManagementService
      .listParties()
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))(DE)

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val party = if (request.partyIdHint.isEmpty) None else Some(request.partyIdHint)
    val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)

    FutureConverters
      .toScala(writeService.allocateParty(party, displayName))
      .flatMap {
        case PartyAllocationResult.Ok(details) =>
          Future.successful(AllocatePartyResponse(Some(mapPartyDetails(details))))
        case PartyAllocationResult.AlreadyExists =>
          Future.failed(ErrorFactories.invalidArgument(s"The requested party name already exists"))
        case PartyAllocationResult.InvalidName =>
          Future.failed(ErrorFactories.invalidArgument(s"The requested party name is invalid"))
        case PartyAllocationResult.NotSupported =>
          Future.failed(
            ErrorFactories.unimplemented(
              s"Synchronous party allocation is not supported on this ledger"))
      }(DE)
  }

}

object ApiPartyManagementService {
  def createApiService(readBackend: IndexPartyManagementService, writeBackend: WritePartyService)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): GrpcApiService with BindableService with PartyManagementServiceLogging =
    new ApiPartyManagementService(readBackend, writeBackend) with BindableService
    with PartyManagementServiceLogging
}
