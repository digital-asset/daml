// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID

import akka.stream.Materializer
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.PartyEntry.{AllocationAccepted, AllocationRejected}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService
}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{SubmissionResult, WritePartyService}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.ServerServiceDefinition

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[apiserver] final class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    transactionService: IndexTransactionsService,
    writeService: WritePartyService,
    materializer: Materializer,
)(implicit loggingContext: LoggingContext)
    extends PartyManagementService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, DE)

  override def getParticipantId(
      request: GetParticipantIdRequest): Future[GetParticipantIdResponse] = {
    partyManagementService
      .getParticipantId()
      .map(pid => GetParticipantIdResponse(pid.toString))(DE)
      .andThen(logger.logErrorsOnCall[GetParticipantIdResponse])(DE)
  }

  private[this] def mapPartyDetails(
      details: com.daml.ledger.api.domain.PartyDetails
  ): PartyDetails =
    PartyDetails(details.party, details.displayName.getOrElse(""), details.isLocal)

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    partyManagementService
      .getParties(request.parties.map(Ref.Party.assertFromString))
      .map(ps => GetPartiesResponse(ps.map(mapPartyDetails)))(DE)
      .andThen(logger.logErrorsOnCall[GetPartiesResponse])(DE)

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] =
    partyManagementService
      .listKnownParties()
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))(DE)
      .andThen(logger.logErrorsOnCall[ListKnownPartiesResponse])(DE)

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    // TODO: This should do proper validation.
    val submissionId = v1.SubmissionId.assertFromString(UUID.randomUUID().toString)
    val party =
      if (request.partyIdHint.isEmpty) None
      else Some(Ref.Party.assertFromString(request.partyIdHint))
    val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)

    transactionService
      .currentLedgerEnd()
      .flatMap { ledgerEndBeforeRequest =>
        FutureConverters
          .toScala(writeService
            .allocateParty(party, displayName, submissionId))
          .flatMap {
            case SubmissionResult.Acknowledged =>
              SynchronousResponse
                .pollUntilPersisted(
                  partyManagementService
                    .partyEntries(ledgerEndBeforeRequest)
                    .collect {
                      case entry @ AllocationAccepted(Some(`submissionId`), _) => entry
                      case entry @ AllocationRejected(`submissionId`, _) => entry
                    },
                  timeToLive = 30.seconds,
                )(materializer)
                .flatMap {
                  case domain.PartyEntry.AllocationAccepted(_, partyDetails) =>
                    Future.successful(
                      AllocatePartyResponse(
                        Some(PartyDetails(
                          partyDetails.party,
                          partyDetails.displayName.getOrElse(""),
                          partyDetails.isLocal,
                        ))))
                  case domain.PartyEntry.AllocationRejected(_, reason) =>
                    Future.failed(ErrorFactories.invalidArgument(reason))
                }(DE)
            case r @ SubmissionResult.Overloaded =>
              Future.failed(ErrorFactories.resourceExhausted(r.description))
            case r @ SubmissionResult.InternalError(_) =>
              Future.failed(ErrorFactories.internal(r.reason))
            case r @ SubmissionResult.NotSupported =>
              Future.failed(ErrorFactories.unimplemented(r.description))
          }(DE)
      }(DE)
      .andThen(logger.logErrorsOnCall[AllocatePartyResponse])(DE)
  }
}

private[apiserver] object ApiPartyManagementService {
  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      transactionsService: IndexTransactionsService,
      writeBackend: WritePartyService,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
    : PartyManagementServiceGrpc.PartyManagementService with GrpcApiService =
    new ApiPartyManagementService(
      partyManagementServiceBackend,
      transactionsService,
      writeBackend,
      mat)

}
