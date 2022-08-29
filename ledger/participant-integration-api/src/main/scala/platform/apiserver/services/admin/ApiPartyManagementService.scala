// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerOffset, ParticipantParty, PartyEntry}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.ledger.api.validation.ValidationErrors
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
  LedgerEndService,
  PartyRecordStore,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.IdString
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiPartyManagementService._
import com.daml.platform.apiserver.services.logging
import com.daml.platform.server.api.validation.FieldValidations
import com.daml.platform.server.api.validation.FieldValidations.{
  optionalString,
  requireEmptyString,
  requireParty,
  requirePresence,
  verifyMetadataAnnotations,
}
import com.daml.telemetry.{DefaultTelemetry, TelemetryContext}
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPartyManagementService private (
                                                                   partyManagementService: IndexPartyManagementService,
                                                                   partyRecordStore: PartyRecordStore,
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
      implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)

      logger.info("Getting parties")
      val parties = request.parties.map(Ref.Party.assertFromString)

      (for {
        ledgerParties <- partyManagementService.getParties(parties)
        apiPartyDetails <- Future.sequence(ledgerParties.map { ledgerPartyDetails =>
          // TODO pbatko: Refactor to make more readable
          partyRecordStore
            .getPartyRecord(ledgerPartyDetails.party)
            .flatMap(handleResult("creating participant party record")(_))
            .map(participantPartyRecord =>
              toProtoPartyDetails(ledgerPartyDetails, participantPartyRecord)
            )
        })
      } yield {
        GetPartiesResponse(apiPartyDetails)
      }).andThen(logger.logErrorsOnCall[GetPartiesResponse])
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

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val submissionId = submissionIdGenerator(request.partyIdHint)
    withEnrichedLoggingContext(
      logging.partyString(request.partyIdHint),
      logging.submissionId(submissionId),
    ) { implicit loggingContext =>
      logger.info("Allocating party")
      implicit val telemetryContext: TelemetryContext =
        DefaultTelemetry.contextFromGrpcThreadLocalContext()
      implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)

//      val validatedPartyIdHint: Either[StatusRuntimeException, Option[IdString.Party]] =
//        if (request.partyIdHint.isEmpty) {
//          Right(None)
//        } else {
//          FieldValidations.requireParty(request.partyIdHint).map(Some(_)
      // TODO pbatko: ValidationLogger.logFailure - try to remove it in a separate PR
//          Ref.Party
//            .fromString(request.partyIdHint)
//            .fold(
//              error =>
//                Future.failed(
//                  ValidationLogger
//                    .logFailure(request, ValidationErrors.invalidArgument(error))
//                ),
//              party => Right(Some(party)),
//            )
//        }

      withValidation {
        for {
          pPartyIdHint <- {
            val x: Either[StatusRuntimeException, Option[IdString.Party]] =
              if (request.partyIdHint.isEmpty) {
                Right(None)
              } else {
                FieldValidations.requireParty(request.partyIdHint).map(Some(_))
              }
            x
          }
          pMetadata = request.localMetadata.getOrElse(
            com.daml.ledger.api.v1.admin.object_meta.ObjectMeta()
          )
          _ <- requireEmptyString(pMetadata.resourceVersion, "user.metadata.resource_version")
          pAnnotations <- verifyMetadataAnnotations(
            pMetadata.annotations,
            "user.metadata.annotations",
          )
        } yield (pPartyIdHint, pAnnotations)
      } { case (partyIdHint, annotations) =>
        val displayName = Some(request.displayName).filterNot(_.isEmpty)
        val input = (partyIdHint, displayName)
        val submit = for {
          PartyEntry.AllocationAccepted(_, partyDetails) <- synchronousResponse.submitAndWait(
            submissionId,
            input,
          )
          partyRecord <- partyRecordStore
            .createPartyRecord(
              ParticipantParty.PartyRecord(
                party = partyDetails.party,
                metadata = domain.ObjectMeta(
                  resourceVersionO = None,
                  annotations = annotations,
                ),
              )
            )
            .flatMap(handleResult("creating participant party record")(_))
        } yield {
          val details = toProtoPartyDetails(
            partyDetails = partyDetails,
            partyRecord = partyRecord,
          )
          AllocatePartyResponse(Some(details))
        }
        submit.andThen(logger.logErrorsOnCall[AllocatePartyResponse])
      }
    }
  }

  // TODO pbatko: Test scenario where partyRecord is absent
  // TODO pbatko: Test scenario where party does not exists
  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] = {
    val submissionId = submissionIdGenerator(request.partyDetails.fold("")(_.party))
    withEnrichedLoggingContext(
      logging.submissionId(submissionId)
    ) { implicit loggingContext =>
      logger.info("Updating a party")
      implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)

      withValidation {
        for {
          pPartyDetails <- requirePresence(
            request.partyDetails,
            fieldName = "party_details",
          )
          pParty <- requireParty(pPartyDetails.party)
          pMetadata = pPartyDetails.localMetadata.getOrElse(
            com.daml.ledger.api.v1.admin.object_meta.ObjectMeta()
          )
          pResourceVersionO <- optionalString(pMetadata.resourceVersion)(Right(_))
          pAnnotations <- verifyMetadataAnnotations(
            pMetadata.annotations,
            "party_details.metadata.annotations",
          )
          pUpdateMask <- requirePresence(request.updateMask, "update_mask")
        } yield (pParty, pResourceVersionO, pAnnotations, pUpdateMask)
      } { case (party, resourceVersionO, annotations, updateMask) =>
        val partyRecordUpdate = UpdateMapper.toParticipantPartyUpdate(
          partyRecord = ParticipantParty.PartyRecord(
            party = party,
            metadata = domain.ObjectMeta(
              // TODO pbatko: Parse resource version
              resourceVersionO = resourceVersionO.map(_.toLong),
              annotations = annotations,
            ),
          ),
          updateMask = updateMask,
        )
        // TODO pbatko: Test update for party for which party record (sic) record doesn't exist

        for {
          updatedPartyRecordResult <- partyRecordStore.updatePartyRecord(
            partyRecordUpdate = partyRecordUpdate,
            ledgerPartyExists = (party: Ref.Party) => {
              partyManagementService.getParties(Seq(party)).map(_.nonEmpty)
            },
          )
          updatedPartyRecord <- handleResult("update participant party record")(
            updatedPartyRecordResult
          )
          ledgerPartyDetails <- partyManagementService
            .getParties(parties = Seq(party))
            .map(_.headOption.getOrElse(sys.error("TODO missing ledger party details")))
        } yield {
          UpdatePartyDetailsResponse(
            Some(
              toProtoPartyDetails(
                partyDetails = ledgerPartyDetails,
                partyRecord = updatedPartyRecord,
              )
            )
          )
        }
      }
    }
  }

  private def handleResult[T](operation: String)(
      result: v2.PartyRecordStore.Result[T]
  )(implicit errorLogger: DamlContextualizedErrorLogger): Future[T] =
    result match {
      // TODO pbatko: Party vs. PartyRecord
      case Left(PartyRecordStore.PartyNotFound(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.ParticipantPartyNotFound
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.PartyRecordNotFound(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.ParticipantPartyNotFound
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.PartyRecordExists(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.ParticipantPartyAlreadyExists
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.ConcurrentPartyUpdate(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.ConcurrentParticipantPartyUpdateDetected
            .Reject(party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.ExplicitMergeUpdateWithDefaultValue(party)) =>
        // TODO pbatko: Map to fitting error code
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.ConcurrentParticipantPartyUpdateDetected
            .Reject(party = party)
            .asGrpcError
        )

      case scala.util.Right(t) =>
        Future.successful(t)
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

  private def toProtoPartyDetails(
      partyDetails: domain.PartyDetails,
      partyRecord: ParticipantParty.PartyRecord,
  ): PartyDetails =
    PartyDetails(
      party = partyDetails.party,
      displayName = partyDetails.displayName.getOrElse(""),
      isLocal = partyDetails.isLocal,
      localMetadata = Some(
        ApiUserManagementService.toProtoObjectMeta(partyRecord.metadata)
      ),
    )

  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      partyRecordStore: PartyRecordStore,
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
      partyRecordStore,
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
