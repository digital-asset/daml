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
import io.grpc.ServerServiceDefinition
import org.slf4j.LoggerFactory

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
      .map(pid => GetParticipantIdResponse(pid.toString))(DE)

  private[this] def mapPartyDetails(
      details: com.digitalasset.ledger.api.domain.PartyDetails): PartyDetails =
    PartyDetails(details.party, details.displayName.getOrElse(""), details.isLocal)

  override def listKnownParties(
      request: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    partyManagementService
      .listParties()
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))(DE)

  /**
    * Continuously polls the party management service to check if a party has been persisted.
    *
    * Despite the `go` inner function not being stack-safe per se, only one stack frame will be on
    * the stack at any given time since the "recursive" invocation happens on a different thread.
    *
    * @param party The party whose persistence we're waiting for
    * @return The number of attempts before the party was found wrapped in a [[Future]]
    */
  private def pollUntilPersisted(party: String): Future[Int] = {
    def go(party: String, attempt: Int): Future[Int] = {
      partyManagementService
        .listParties()
        .flatMap {
          case persisted if persisted.exists(_.party == party) => Future.successful(attempt)
          case _ => go(party, attempt + 1)
        }(DE)
    }
    go(party, 1)
  }

  /**
    * Wraps a call [[pollUntilPersisted]] so that it can be chained on the party allocation with a `flatMap`.
    *
    * Checks invariants and forwards the original result after the party is found to be persisted.
    *
    * @param result The result of the party allocation
    * @return The result of the party allocation received originally, wrapped in a [[Future]]
    */
  private def pollUntilPersisted(result: AllocatePartyResponse): Future[AllocatePartyResponse] = {
    require(result.partyDetails.isDefined, "Party allocation response must have the party details")
    pollUntilPersisted(result.partyDetails.get.party).map { numberOfAttempts =>
      logger.debug(s"Party available read after $numberOfAttempts attempt(s)")
      result
    }(DE)
  }

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val party = if (request.partyIdHint.isEmpty) None else Some(request.partyIdHint)
    val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)

    FutureConverters
      .toScala(writeService
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
      }(DE)
      .flatMap(pollUntilPersisted)(DE)
  }

}

object ApiPartyManagementService {
  def createApiService(readBackend: IndexPartyManagementService, writeBackend: WritePartyService)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): GrpcApiService =
    new ApiPartyManagementService(readBackend, writeBackend) with PartyManagementServiceLogging

}
