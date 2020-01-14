// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services.admin

import java.util.UUID

import akka.actor.Scheduler
import akka.stream.scaladsl.Sink
import akka.stream.Materializer
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService
}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePartyService}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.PartyEntry.{AllocationAccepted, AllocationRejected}
import com.digitalasset.ledger.api.domain.{LedgerOffset, PartyEntry}
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.dec.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.ServerServiceDefinition

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    transactionService: IndexTransactionsService,
    writeService: WritePartyService,
    materializer: Materializer,
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
    // TODO Gerolf: this should do proper validation
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
  }
}

object ApiPartyManagementService {
  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      transactionsService: IndexTransactionsService,
      writeBackend: WritePartyService,
      loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): PartyManagementServiceGrpc.PartyManagementService with GrpcApiService =
    new ApiPartyManagementService(
      partyManagementServiceBackend,
      transactionsService,
      writeBackend,
      mat,
      mat.system.scheduler,
      loggerFactory) with PartyManagementServiceLogging

}
