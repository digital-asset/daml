// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.domain.{LedgerOffset, PartyEntry}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.ledger.api.validation.ValidationErrors
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
import com.daml.telemetry.{DefaultTelemetry, TelemetryContext}
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    transactionService: IndexTransactionsService,
    writeService: state.WritePartyService,
    managementServiceTimeout: FiniteDuration,
    submissionIdGenerator: String => Ref.SubmissionId,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends PartyManagementService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(
      transactionService,
      writeService,
      partyManagementService,
    ),
    timeToLive = managementServiceTimeout,
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
      .andThen(logger.logErrorsOnCall[GetParticipantIdResponse])
  }

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    withEnrichedLoggingContext(logging.partyStrings(request.parties)) { implicit loggingContext =>
      logger.info("Getting parties")
      partyManagementService
        .getParties(request.parties.map(Ref.Party.assertFromString))
        .map(ps => GetPartiesResponse(ps.map(mapPartyDetails)))
        .andThen(logger.logErrorsOnCall[GetPartiesResponse])
    }

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] = {
    logger.info("Listing known parties")
    partyManagementService
      .listKnownParties()
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))
      .andThen(logger.logErrorsOnCall[ListKnownPartiesResponse])
  }

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val submissionId = submissionIdGenerator(request.partyIdHint)
    withEnrichedLoggingContext(
      logging.partyString(request.partyIdHint),
      logging.submissionId(submissionId),
    ) { implicit loggingContext =>
      logger.info("Allocating party")
      implicit val telemetryContext: TelemetryContext =
        DefaultTelemetry.contextFromGrpcThreadLocalContext()
      implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)

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
                    .logFailure(request, ValidationErrors.invalidArgument(error))
                ),
              party => Future.successful(Some(party)),
            )
        }

      val submit = for {
        partyIdHint <- validatedPartyIdHint
        displayName = Some(request.displayName).filterNot(_.isEmpty)
        input = (partyIdHint, displayName)
        PartyEntry.AllocationAccepted(_, partyDetails) <- synchronousResponse.submitAndWait(
          submissionId,
          input,
        )
      } yield {
        val details = PartyDetails(
          partyDetails.party,
          partyDetails.displayName.getOrElse(""),
          partyDetails.isLocal,
        )
        AllocatePartyResponse(Some(details))
      }
      submit.andThen(logger.logErrorsOnCall[AllocatePartyResponse])
    }
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
      managementServiceTimeout: FiniteDuration,
      submissionIdGenerator: String => Ref.SubmissionId = CreateSubmissionId.withPrefix,
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
      submissionIdGenerator,
    )

  private object CreateSubmissionId {
    // Suffix is `-` followed by a random UUID as a string
    private val SuffixLength: Int = 1 + UUID.randomUUID().toString.length
    private val MaxLength: Int = 255
    private val PrefixMaxLength: Int = MaxLength - SuffixLength

    def withPrefix(partyHint: String): Ref.SubmissionId =
      augmentSubmissionId(
        Ref.SubmissionId.fromString(partyHint.take(PrefixMaxLength)).getOrElse("")
      )
  }

  private final class SynchronousResponseStrategy(
      ledgerEndService: LedgerEndService,
      writeService: state.WritePartyService,
      partyManagementService: IndexPartyManagementService,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        (Option[Ref.Party], Option[String]),
        PartyEntry,
        PartyEntry.AllocationAccepted,
      ] {
    private val logger = ContextualizedLogger.get(getClass)

    override def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]] =
      ledgerEndService.currentLedgerEnd().map(Some(_))

    override def submit(
        submissionId: Ref.SubmissionId,
        input: (Option[Ref.Party], Option[String]),
    )(implicit
        telemetryContext: TelemetryContext,
        loggingContext: LoggingContext,
    ): Future[state.SubmissionResult] = {
      val (party, displayName) = input
      writeService.allocateParty(party, displayName, submissionId).asScala
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
        ValidationErrors.invalidArgument(reason)(
          new DamlContextualizedErrorLogger(logger, loggingContext, Some(submissionId))
        )
    }
  }

}
