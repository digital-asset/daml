// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.Duration
import java.util.UUID
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.domain.{LedgerOffset, PartyEntry}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
  LedgerEndService,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiPartyManagementService._
import com.daml.platform.apiserver.services.logging
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.telemetry.{DefaultTelemetry, TelemetryContext}
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    transactionService: IndexTransactionsService,
    writeService: state.WritePartyService,
    managementServiceTimeout: Duration,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
    submissionIdGenerator: Option[Ref.Party] => Ref.SubmissionId,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends PartyManagementService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)
  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(
      transactionService,
      writeService,
      partyManagementService,
      errorFactories,
    ),
    timeToLive = managementServiceTimeout,
    errorFactories = errorFactories,
  )

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, executionContext)

  override def getParticipantId(
      request: GetParticipantIdRequest
  ): Future[GetParticipantIdResponse] = {
    logger.info("Getting Participant ID")
    partyManagementService
      .getParticipantId()
      .map(pid => GetParticipantIdResponse(pid.toString))
      .andThen(logger.logErrorsOnCall(errorCodesVersionSwitcher.enableSelfServiceErrorCodes))
  }

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    withEnrichedLoggingContext(logging.partyStrings(request.parties)) { implicit loggingContext =>
      logger.info("Getting parties")
      partyManagementService
        .getParties(request.parties.map(Ref.Party.assertFromString))
        .map(ps => GetPartiesResponse(ps.map(mapPartyDetails)))
        .andThen(logger.logErrorsOnCall(errorCodesVersionSwitcher.enableSelfServiceErrorCodes))
    }

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] = {
    logger.info("Listing known parties")
    partyManagementService
      .listKnownParties()
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))
      .andThen(logger.logErrorsOnCall(errorCodesVersionSwitcher.enableSelfServiceErrorCodes))
  }

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    withEnrichedLoggingContext(logging.partyString(request.partyIdHint)) {
      implicit loggingContext =>
        logger.info("Allocating party")
        implicit val telemetryContext: TelemetryContext =
          DefaultTelemetry.contextFromGrpcThreadLocalContext()
        val validatedPartyIdHint =
          if (request.partyIdHint.isEmpty) {
            Future.successful(None)
          } else {
            Ref.Party
              .fromString(request.partyIdHint)
              .fold(
                error =>
                  Future.failed(
                    ValidationLogger
                      .logFailureWithContext(request, errorFactories.invalidArgument(None)(error))
                  ),
                party => Future.successful(Some(party)),
              )
          }

        validatedPartyIdHint
          .flatMap(partyIdHint => {
            val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)
            synchronousResponse
              .submitAndWait(submissionIdGenerator(partyIdHint), (partyIdHint, displayName))
              .map { case PartyEntry.AllocationAccepted(_, partyDetails) =>
                AllocatePartyResponse(
                  Some(
                    PartyDetails(
                      partyDetails.party,
                      partyDetails.displayName.getOrElse(""),
                      partyDetails.isLocal,
                    )
                  )
                )
              }
          })
          .andThen(logger.logErrorsOnCall(errorCodesVersionSwitcher.enableSelfServiceErrorCodes))
    }

  private[this] def mapPartyDetails(
      details: com.daml.ledger.api.domain.PartyDetails
  ): PartyDetails =
    PartyDetails(
      party = details.party,
      displayName = details.displayName.getOrElse(""),
      isLocal = details.isLocal,
    )

}

private[apiserver] object ApiPartyManagementService {

  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      transactionsService: IndexTransactionsService,
      writeBackend: state.WritePartyService,
      managementServiceTimeout: Duration,
      errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
      submissionIdGenerator: Option[Ref.Party] => Ref.SubmissionId = CreateSubmissionId.withPrefix,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): PartyManagementServiceGrpc.PartyManagementService with GrpcApiService =
    new ApiPartyManagementService(
      partyManagementServiceBackend,
      transactionsService,
      writeBackend,
      managementServiceTimeout,
      errorCodesVersionSwitcher,
      submissionIdGenerator,
    )

  private object CreateSubmissionId {
    // Suffix is `-` followed by a random UUID as a string
    private val SuffixLength: Int = 1 + UUID.randomUUID().toString.length
    private val MaxLength: Int = 255
    private val PrefixMaxLength: Int = MaxLength - SuffixLength

    def withPrefix(maybeParty: Option[Ref.Party]): Ref.SubmissionId = {
      val uuid = UUID.randomUUID().toString
      val raw = maybeParty.fold(uuid)(party => s"${party.take(PrefixMaxLength)}-$uuid")
      Ref.SubmissionId.assertFromString(raw)
    }
  }

  private final class SynchronousResponseStrategy(
      ledgerEndService: LedgerEndService,
      writeService: state.WritePartyService,
      partyManagementService: IndexPartyManagementService,
      errorFactories: ErrorFactories,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        (Option[Ref.Party], Option[String]),
        PartyEntry,
        PartyEntry.AllocationAccepted,
      ] {
    private val logger = ContextualizedLogger.get(getClass)
    private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)

    override def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]] =
      ledgerEndService.currentLedgerEnd().map(Some(_))

    override def submit(
        submissionId: Ref.SubmissionId,
        input: (Option[Ref.Party], Option[String]),
    )(implicit telemetryContext: TelemetryContext): Future[state.SubmissionResult] = {
      val (party, displayName) = input
      writeService.allocateParty(party, displayName, submissionId).toScala
    }

    override def entries(offset: Option[LedgerOffset.Absolute]): Source[PartyEntry, _] =
      partyManagementService.partyEntries(offset)

    override def accept(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PartyEntry, PartyEntry.AllocationAccepted] = {
      case entry @ PartyEntry.AllocationAccepted(Some(`submissionId`), _) => entry
    }

    override def reject(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PartyEntry, StatusRuntimeException] = {
      case PartyEntry.AllocationRejected(`submissionId`, reason) =>
        errorFactories.invalidArgument(None)(reason)
    }
  }

}
