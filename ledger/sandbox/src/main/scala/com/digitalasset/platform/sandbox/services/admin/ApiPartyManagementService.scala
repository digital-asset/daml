// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import java.util.UUID

import akka.actor.Scheduler
import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.index.v2.IndexPartyManagementService
import com.daml.ledger.participant.state.v1.{SubmissionResult, WritePartyService}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.ServerServiceDefinition

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    writeService: WritePartyService,
    scheduler: Scheduler,
    loggerFactory: NamedLoggerFactory
) extends PartyManagementService
    with GrpcApiService {

  protected val logger = loggerFactory.getLogger(this.getClass)

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
    * Wraps a call [[PollingUtils.pollUntilPersisted]] so that it can be chained on the party allocation with a `flatMap`.
    *
    * Checks invariants and forwards the original result after the party is found to be persisted.
    *
    * @param result The result of the party allocation
    * @return The result of the party allocation received originally, wrapped in a [[Future]]
    */
  private def pollUntilPersisted(result: AllocatePartyResponse): Future[AllocatePartyResponse] = {
    require(result.partyDetails.isDefined, "Party allocation response must have the party details")
    val newParty = result.partyDetails.get.party
    val description = s"party $newParty"

    PollingUtils
      .pollUntilPersisted(partyManagementService.listParties _)(
        _.exists(_.party == newParty),
        description,
        50.milliseconds,
        500.milliseconds,
        d => d * 2,
        scheduler,
        loggerFactory)
      .map { numberOfAttempts =>
        logger.debug(s"Party $newParty available, read after $numberOfAttempts attempt(s)")
        result
      }(DE)
  }

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val party = if (request.partyIdHint.isEmpty) None else Some(request.partyIdHint)
    val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)
    val submissionId = UUID.randomUUID().toString

    FutureConverters
      .toScala(writeService
        .allocateParty(party, displayName, submissionId))
      .flatMap {
        case SubmissionResult.Acknowledged =>
          //TODO BH get full response from accept/reject party allocation message
          Future.successful(AllocatePartyResponse(
            Some(PartyDetails(party.getOrElse("DUMMY"), displayName.getOrElse(""), true))))
        case r @ SubmissionResult.Overloaded =>
          Future.failed(ErrorFactories.resourceExhausted(r.description))
        case r @ SubmissionResult.InternalError(_) =>
          Future.failed(ErrorFactories.internal(r.reason))
        case r @ SubmissionResult.NotSupported =>
          Future.failed(ErrorFactories.unimplemented(r.description))
      }(DE)
      .flatMap(pollUntilPersisted)(DE)
  }

  private def pollForAllocationResult() = {}

}

object ApiPartyManagementService {
  def createApiService(
      readBackend: IndexPartyManagementService,
      writeBackend: WritePartyService,
      loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: ActorMaterializer)
    : PartyManagementServiceGrpc.PartyManagementService with GrpcApiService =
    new ApiPartyManagementService(readBackend, writeBackend, mat.system.scheduler, loggerFactory)
    with PartyManagementServiceLogging

}
