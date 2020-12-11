// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.Duration
import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.domain.{LedgerOffset, PartyEntry}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
  LedgerEndService
}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePartyService}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiPartyManagementService._
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    transactionService: IndexTransactionsService,
    writeService: WritePartyService,
    managementServiceTimeout: Duration,
    materializer: Materializer,
)(implicit loggingContext: LoggingContext)
    extends PartyManagementService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  // Execute subsequent transforms in the thread of the previous operation.
  private implicit val executionContext: ExecutionContext = DirectExecutionContext

  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(transactionService, writeService, partyManagementService),
    timeToLive = managementServiceTimeout,
  )

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def getParticipantId(
      request: GetParticipantIdRequest
  ): Future[GetParticipantIdResponse] = {
    partyManagementService
      .getParticipantId()
      .map(pid => GetParticipantIdResponse(pid.toString))
      .andThen(logger.logErrorsOnCall[GetParticipantIdResponse])
  }

  private[this] def mapPartyDetails(
      details: com.daml.ledger.api.domain.PartyDetails
  ): PartyDetails =
    PartyDetails(details.party, details.displayName.getOrElse(""), details.isLocal)

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    partyManagementService
      .getParties(request.parties.map(Ref.Party.assertFromString))
      .map(ps => GetPartiesResponse(ps.map(mapPartyDetails)))
      .andThen(logger.logErrorsOnCall[GetPartiesResponse])

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] =
    partyManagementService
      .listKnownParties()
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))
      .andThen(logger.logErrorsOnCall[ListKnownPartiesResponse])

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    // TODO: This should do proper validation.
    def randomSubmissionId(prefix: String): Ref.IdString.LedgerString =
      v1.SubmissionId.assertFromString(s"${prefix}_${UUID.randomUUID().toString}")

    val party =
      if (request.partyIdHint.isEmpty) None
      else Some(Ref.Party.assertFromString(request.partyIdHint))
    val submissionId = randomSubmissionId(prefix = party.getOrElse(""))

    val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)

    synchronousResponse
      .submitAndWait(submissionId, (party, displayName))(executionContext, materializer)
      .map {
        case PartyEntry.AllocationAccepted(_, partyDetails) =>
          AllocatePartyResponse(
            Some(
              PartyDetails(
                partyDetails.party,
                partyDetails.displayName.getOrElse(""),
                partyDetails.isLocal,
              )))
      }
      .andThen(logger.logErrorsOnCall[AllocatePartyResponse])
  }

}

private[apiserver] object ApiPartyManagementService {

  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      transactionsService: IndexTransactionsService,
      writeBackend: WritePartyService,
      managementServiceTimeout: Duration,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
    : PartyManagementServiceGrpc.PartyManagementService with GrpcApiService =
    new ApiPartyManagementService(
      partyManagementServiceBackend,
      transactionsService,
      writeBackend,
      managementServiceTimeout,
      mat,
    )

  private final class SynchronousResponseStrategy(
      ledgerEndService: LedgerEndService,
      writeService: WritePartyService,
      partyManagementService: IndexPartyManagementService,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        (Option[Ref.Party], Option[String]),
        PartyEntry,
        PartyEntry.AllocationAccepted,
      ] {

    override def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]] =
      ledgerEndService.currentLedgerEnd().map(Some(_))

    override def submit(
        submissionId: SubmissionId,
        input: (Option[Ref.Party], Option[String]),
    ): Future[SubmissionResult] = {
      val (party, displayName) = input
      writeService.allocateParty(party, displayName, submissionId).toScala
    }

    override def entries(offset: Option[LedgerOffset.Absolute]): Source[PartyEntry, _] =
      partyManagementService.partyEntries(offset)

    override def accept(
        submissionId: SubmissionId,
    ): PartialFunction[PartyEntry, PartyEntry.AllocationAccepted] = {
      case entry @ PartyEntry.AllocationAccepted(Some(`submissionId`), _) => entry
    }

    override def reject(
        submissionId: SubmissionId,
    ): PartialFunction[PartyEntry, StatusRuntimeException] = {
      case PartyEntry.AllocationRejected(`submissionId`, reason) =>
        ErrorFactories.invalidArgument(reason)
    }
  }

}
