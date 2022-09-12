// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.ParticipantParty.PartyRecord
import com.daml.ledger.api.domain.{LedgerOffset, ObjectMeta, ParticipantParty, PartyEntry}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.ledger.api.v1.{admin => proto_admin}
import com.daml.ledger.api.validation.ValidationErrors
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.PartyRecordStore.Result
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
  LedgerEndService,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiPartyManagementService._
import com.daml.platform.apiserver.services.logging
import com.daml.platform.apiserver.update
import com.daml.platform.apiserver.update.{FieldNames, PartyRecordUpdateMapper, RequestsPaths}
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
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

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
      val partiesEither: Either[StatusRuntimeException, List[Ref.Party]] =
        request.parties.toList.traverse(FieldValidations.requireParty)
      withValidation {
        for {
          parties <- partiesEither
        } yield parties
      } { parties: Seq[Party] =>
        for {
          partyDetailsSeq <- partyManagementService.getParties(parties)
          partyRecordOptions <- fetchPartyRecords(partyDetailsSeq)
        } yield {
          val protoDetails =
            partyDetailsSeq.zip(partyRecordOptions).map { case (details, recordO) =>
              toProtoPartyDetails(details, recordO.map(_.metadata))
            }
          GetPartiesResponse(
            partyDetails = protoDetails
          )
        }
      }.andThen(
        // TODO um-for-hub: Check if this logging is necessary and doesn't duplicate other logging facilities
        logger.logErrorsOnCall[GetPartiesResponse]
      )
    }

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] = {
    logger.info("Listing known parties")
    implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    (for {
      partyDetailsSeq <- partyManagementService.listKnownParties()
      partyRecords <- fetchPartyRecords(partyDetailsSeq)
    } yield {
      val protoDetails = partyDetailsSeq
        .zip(partyRecords)
        .map { case (details, recordO) =>
          toProtoPartyDetails(details, recordO.map(_.metadata))
        }
      ListKnownPartiesResponse(protoDetails)
    }).andThen(logger.logErrorsOnCall[ListKnownPartiesResponse])
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
      implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)
      withValidation {
        for {
          partyIdHintO <- FieldValidations.optionalString(
            request.partyIdHint
          )(FieldValidations.requireParty)
          metadata = request.localMetadata.getOrElse(
            com.daml.ledger.api.v1.admin.object_meta.ObjectMeta()
          )
          _ <- requireEmptyString(
            metadata.resourceVersion,
            RequestsPaths.PartyDetailsPaths.resourceVersion.mkString("."),
          )
          annotations <- verifyMetadataAnnotations(
            metadata.annotations,
            RequestsPaths.PartyDetailsPaths.annotations.mkString("."),
          )
          displayNameO <- FieldValidations.optionalString(request.displayName)(Right(_))
        } yield (partyIdHintO, displayNameO, annotations)
      } { case (partyIdHintO, displayNameO, annotations) =>
        (for {
          allocated <- synchronousResponse.submitAndWait(
            submissionId,
            (partyIdHintO, displayNameO),
          )
          partyRecord <- partyRecordStore
            .createPartyRecord(
              ParticipantParty.PartyRecord(
                party = allocated.partyDetails.party,
                metadata = domain.ObjectMeta(resourceVersionO = None, annotations = annotations),
              )
            )
            .flatMap(handlePartyStoreResult("create party record")(_))
        } yield {
          val details = toProtoPartyDetails(
            partyDetails = allocated.partyDetails,
            metadataO = Some(partyRecord.metadata),
          )
          AllocatePartyResponse(Some(details))
        }).andThen(logger.logErrorsOnCall[AllocatePartyResponse])
      }
    }
  }

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
          partyDetails <- requirePresence(
            request.partyDetails,
            fieldName = FieldNames.UpdatePartyDetailsRequest.partyDetails,
          )
          party <- requireParty(partyDetails.party)
          metadata = partyDetails.localMetadata.getOrElse(proto_admin.object_meta.ObjectMeta())
          resourceVersionNumberO <- optionalString(metadata.resourceVersion)(
            FieldValidations.requireResourceVersion(
              _,
              fieldName = RequestsPaths.PartyDetailsPaths.resourceVersion.mkString("."),
            )
          )
          annotations <- verifyMetadataAnnotations(
            metadata.annotations,
            RequestsPaths.PartyDetailsPaths.annotations.mkString("."),
          )
          updateMask <- requirePresence(
            request.updateMask,
            FieldNames.UpdatePartyDetailsRequest.updateMask,
          )
          partyRecord = ParticipantParty.PartyRecord(
            party = party,
            metadata = domain.ObjectMeta(
              resourceVersionO = resourceVersionNumberO,
              annotations = annotations,
            ),
          )
        } yield (partyRecord, updateMask)
      } { case (partyRecord, updateMask) =>
        for {
          partyRecordUpdate: PartyRecordUpdate <- handleUpdatePathResult(
            party = partyRecord.party,
            PartyRecordUpdateMapper.toUpdate(
              domainObject = partyRecord,
              updateMask = updateMask,
            ),
          )
          _ <-
            if (partyRecordUpdate.isNoUpdate) {
              // TODO pbatko: Document in proto that no-up updates are invalid
              Future.failed(
                LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate
                  .Reject(
                    party = partyRecord.party,
                    reason = "Update request describes a no-up update",
                  )
                  .asGrpcError
              )
            } else {
              Future.successful(())
            }
          updatedPartyRecordResult: Result[PartyRecord] <- partyRecordStore.updatePartyRecord(
            partyRecordUpdate = partyRecordUpdate,
            ledgerPartyExists = (party: Ref.Party) => {
              partyManagementService.getParties(Seq(party)).map(_.nonEmpty)
            },
          )
          updatedPartyRecord: PartyRecord <- handlePartyStoreResult(
            "update participant party record"
          )(updatedPartyRecordResult)
          fetchedPartyDetailsO <- partyManagementService
            .getParties(parties = Seq(partyRecord.party))
            .map(_.headOption)
          fetchedPartyDetails <- fetchedPartyDetailsO match {
            case Some(partyDetails) => Future.successful(partyDetails)
            case None =>
              Future.failed(
                LedgerApiErrors.Admin.PartyManagement.PartyNotFound
                  .Reject(
                    "update party details",
                    party = partyRecord.party,
                  )
                  .asGrpcError
              )
          }
        } yield UpdatePartyDetailsResponse(
          Some(
            toProtoPartyDetails(
              partyDetails = fetchedPartyDetails,
              metadataO = Some(updatedPartyRecord.metadata),
            )
          )
        )
      }
    }
  }

  private def fetchPartyRecords(
      partyDetails: List[domain.PartyDetails]
  )(implicit errorLogger: DamlContextualizedErrorLogger): Future[List[Option[PartyRecord]]] = {
    Future.sequence(partyDetails.map { partyDetails =>
      partyRecordStore
        .getPartyRecordO(party = partyDetails.party)
        .flatMap(handlePartyStoreResult(operation = "retrieve party record")(_))
    })
  }

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  private def handleUpdatePathResult[T](party: Ref.Party, result: update.Result[T])(implicit
      errorLogger: DamlContextualizedErrorLogger
  ): Future[T] =
    result match {
      case Left(e: update.UpdatePathError) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate
            .Reject(party, reason = e.getReason)
            .asGrpcError
        )
      case scala.util.Right(t) =>
        Future.successful(t)
    }

  private def handlePartyStoreResult[T](operation: String)(
      result: v2.PartyRecordStore.Result[T]
  )(implicit errorLogger: DamlContextualizedErrorLogger): Future[T] =
    result match {
      // TODO pbatko: Party vs. PartyRecord
      case Left(PartyRecordStore.PartyNotFound(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.PartyNotFound
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.PartyRecordNotFound(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.PartyRecordNotFound
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

      case Left(e @ PartyRecordStore.MaxAnnotationsSizeExceeded(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate
            .Reject(party = party, reason = e.getReason)
            .asGrpcError
        )

      case scala.util.Right(t) =>
        Future.successful(t)
    }

}

private[apiserver] object ApiPartyManagementService {

  private def toProtoPartyDetails(
      partyDetails: domain.PartyDetails,
      metadataO: Option[ObjectMeta],
  ): PartyDetails =
    PartyDetails(
      party = partyDetails.party,
      displayName = partyDetails.displayName.getOrElse(""),
      isLocal = partyDetails.isLocal,
      localMetadata = Some(
        // TODO pbatko: Do not usu stuff from User service
        ApiUserManagementService.toProtoObjectMeta(metadataO.getOrElse(ObjectMeta.empty))
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
