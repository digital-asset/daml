// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID

import akka.actor.Scheduler
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService
}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePartyService}
import com.daml.lf.data.Ref
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.PartyEntry.{AllocationAccepted, AllocationRejected}
import com.daml.ledger.api.domain.{LedgerOffset, PartyEntry}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.ServerServiceDefinition

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

final class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    transactionService: IndexTransactionsService,
    writeService: WritePartyService,
    materializer: Materializer,
    scheduler: Scheduler,
)(implicit logCtx: LoggingContext)
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

  /**
    * Checks invariants and forwards the original result after the party is found to be persisted.
    *
    * @return The result of the party allocation received originally, wrapped in a [[Future]]
    */
  private def pollUntilPersisted(
      submissionId: SubmissionId,
      offset: LedgerOffset.Absolute): Future[PartyEntry] = {
    partyManagementService
      .partyEntries(offset)
      .collect {
        case entry @ AllocationAccepted(Some(`submissionId`), _, _) => entry
        case entry @ AllocationRejected(`submissionId`, _, _) => entry
      }
      .completionTimeout(30.seconds)
      .runWith(Sink.head)(materializer)
  }

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
              pollUntilPersisted(submissionId, ledgerEndBeforeRequest).flatMap {
                case domain.PartyEntry.AllocationAccepted(_, _, partyDetails) =>
                  Future.successful(
                    AllocatePartyResponse(
                      Some(PartyDetails(
                        partyDetails.party,
                        partyDetails.displayName.getOrElse(""),
                        partyDetails.isLocal))))
                case domain.PartyEntry.AllocationRejected(_, _, reason) =>
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

object ApiPartyManagementService {
  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      transactionsService: IndexTransactionsService,
      writeBackend: WritePartyService,
  )(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
      logCtx: LoggingContext)
    : PartyManagementServiceGrpc.PartyManagementService with GrpcApiService =
    new ApiPartyManagementService(
      partyManagementServiceBackend,
      transactionsService,
      writeBackend,
      mat,
      mat.system.scheduler)

}
