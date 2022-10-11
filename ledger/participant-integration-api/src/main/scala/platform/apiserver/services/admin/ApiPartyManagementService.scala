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
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
  LedgerEndService,
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
import com.daml.platform.apiserver.update.PartyRecordUpdateMapper
import com.daml.platform.localstore.api.{PartyDetailsUpdate, PartyRecordStore, PartyRecordUpdate}
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
      implicit val errorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)
      logger.info("Getting parties")
      withValidation {
        for {
          parties <- request.parties.toList.traverse(FieldValidations.requireParty)
        } yield parties
      } { parties: Seq[Party] =>
        for {
          partyDetailsSeq <- partyManagementService.getParties(parties)
          partyRecordOptions <- fetchPartyRecords(partyDetailsSeq)
        } yield {
          val protoDetails =
            partyDetailsSeq.zip(partyRecordOptions).map { case (details, recordO) =>
              toProtoPartyDetails(partyDetails = details, metadataO = recordO.map(_.metadata))
            }
          GetPartiesResponse(partyDetails = protoDetails)
        }
      }.andThen(
        // TODO um-for-hub: Check if this logging is necessary and doesn't duplicate error interceptor
        logger.logErrorsOnCall[GetPartiesResponse]
      )
    }

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] = {
    logger.info("Listing known parties")
    implicit val errorLogger: DamlContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    (for {
      partyDetailsSeq <- partyManagementService.listKnownParties()
      partyRecords <- fetchPartyRecords(partyDetailsSeq)
    } yield {
      val protoDetails = partyDetailsSeq.zip(partyRecords).map { case (details, recordO) =>
        toProtoPartyDetails(partyDetails = details, metadataO = recordO.map(_.metadata))
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
      implicit val errorLogger: DamlContextualizedErrorLogger =
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
            "party_details.local_metadata.resource_version",
          )
          annotations <- verifyMetadataAnnotations(
            metadata.annotations,
            allowEmptyValues = false,
            "party_details.local_metadata.annotations",
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
            .flatMap(handlePartyRecordStoreResult("creating a party record")(_))
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
      implicit val errorLogger: DamlContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(logger, loggingContext, None)
      withValidation {
        for {
          partyDetails <- requirePresence(
            request.partyDetails,
            "party_details",
          )
          party <- requireParty(partyDetails.party)
          metadata = partyDetails.localMetadata.getOrElse(proto_admin.object_meta.ObjectMeta())
          resourceVersionNumberO <- optionalString(metadata.resourceVersion)(
            FieldValidations.requireResourceVersion(
              _,
              "party_details.local_metadata",
            )
          )
          annotations <- verifyMetadataAnnotations(
            metadata.annotations,
            allowEmptyValues = true,
            "party_details.local_metadata.annotations",
          )
          updateMask <- requirePresence(
            request.updateMask,
            "update_mask",
          )
          displayNameO <- FieldValidations.optionalString(partyDetails.displayName)(Right(_))
          partyRecord = ParticipantParty.PartyDetails(
            party = party,
            displayName = displayNameO,
            isLocal = partyDetails.isLocal,
            metadata = domain.ObjectMeta(
              resourceVersionO = resourceVersionNumberO,
              annotations = annotations,
            ),
          )
        } yield (partyRecord, updateMask)
      } { case (partyRecord, updateMask) =>
        for {
          partyDetailsUpdate: PartyDetailsUpdate <- handleUpdatePathResult(
            party = partyRecord.party,
            PartyRecordUpdateMapper.toUpdate(
              domainObject = partyRecord,
              updateMask = updateMask,
            ),
          )
          fetchedPartyDetailsO <- partyManagementService
            .getParties(parties = Seq(partyRecord.party))
            .map(_.headOption)
          fetchedPartyDetails <- fetchedPartyDetailsO match {
            case Some(partyDetails) => Future.successful(partyDetails)
            case None =>
              Future.failed(
                LedgerApiErrors.Admin.PartyManagement.PartyNotFound
                  .Reject(
                    operation = "updating a party record",
                    party = partyRecord.party,
                  )
                  .asGrpcError
              )
          }
          partyRecordUpdate: PartyRecordUpdate <- {
            if (partyDetailsUpdate.isLocalUpdate.exists(_ != fetchedPartyDetails.isLocal)) {
              Future.failed(
                LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest
                  .Reject(
                    party = partyRecord.party,
                    reason =
                      s"Update request attempted to modify not-modifiable 'is_local' attribute",
                  )
                  .asGrpcError
              )
              // TODO um-for-hub: Investigate why empty sting display name is represented as `""` rather than `None` in com.daml.ledger.api.domain.PartyDetails.displayName: Option[String]
            } else if (
              partyDetailsUpdate.displayNameUpdate.exists(
                _ != (if (fetchedPartyDetails.displayName == Some("")) None
                      else fetchedPartyDetails.displayName)
              )
            ) {
              Future.failed(
                LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest
                  .Reject(
                    party = partyRecord.party,
                    reason =
                      s"Update request attempted to modify not-modifiable 'display_name' attribute update: ${partyDetailsUpdate.displayNameUpdate}, fetched: ${fetchedPartyDetails.displayName}",
                  )
                  .asGrpcError
              )
            } else {
              Future.successful(
                PartyRecordUpdate(
                  party = partyDetailsUpdate.party,
                  metadataUpdate = partyDetailsUpdate.metadataUpdate,
                )
              )
            }
          }
          updatedPartyRecordResult <- partyRecordStore.updatePartyRecord(
            partyRecordUpdate = partyRecordUpdate,
            ledgerPartyExists = (party: Ref.Party) => {
              // TODO um-for-hub: Consider changing it to Future.successful(true)
              partyManagementService.getParties(Seq(party)).map(_.nonEmpty)
            },
          )
          updatedPartyRecord: PartyRecord <- handlePartyRecordStoreResult(
            "updating a participant party record"
          )(updatedPartyRecordResult)
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
  )(implicit errorLogger: DamlContextualizedErrorLogger): Future[List[Option[PartyRecord]]] =
    // TODO um-for-hub: Consider fetching all party records in a single DB call
    partyDetails.foldLeft(Future.successful(List.empty[Option[PartyRecord]])) {
      (axF: Future[List[Option[PartyRecord]]], partyDetails: domain.PartyDetails) =>
        for {
          ax <- axF
          next <- partyRecordStore
            .getPartyRecordO(party = partyDetails.party)
            .flatMap(handlePartyRecordStoreResult(operation = "retrieving a party record")(_))
        } yield ax :+ next
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
          LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest
            .Reject(party, reason = e.getReason)
            .asGrpcError
        )
      case Right(t) =>
        Future.successful(t)
    }

  private def handlePartyRecordStoreResult[T](operation: String)(
      result: PartyRecordStore.Result[T]
  )(implicit errorLogger: DamlContextualizedErrorLogger): Future[T] =
    result match {
      case Left(PartyRecordStore.PartyNotFound(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.PartyNotFound
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.PartyRecordNotFoundFatal(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.InternalPartyRecordNotFound
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.PartyRecordExistsFatal(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.InternalPartyRecordAlreadyExists
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.ConcurrentPartyUpdate(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.ConcurrentPartyDetailsUpdateDetected
            .Reject(party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.MaxAnnotationsSizeExceeded(party)) =>
        Future.failed(
          LedgerApiErrors.Admin.PartyManagement.MaxPartyAnnotationsSizeExceeded
            .Reject(party = party)
            .asGrpcError
        )

      case Right(t) =>
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
      localMetadata = Some(Utils.toProtoObjectMeta(metadataO.getOrElse(ObjectMeta.empty))),
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
