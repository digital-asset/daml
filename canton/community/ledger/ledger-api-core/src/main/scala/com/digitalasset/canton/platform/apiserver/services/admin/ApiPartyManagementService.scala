// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta as ProtoObjectMeta
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  AllocatePartyResponse,
  GetParticipantIdRequest,
  GetParticipantIdResponse,
  GetPartiesRequest,
  GetPartiesResponse,
  ListKnownPartiesRequest,
  ListKnownPartiesResponse,
  PartyDetails as ProtoPartyDetails,
  PartyManagementServiceGrpc,
  UpdatePartyDetailsRequest,
  UpdatePartyDetailsResponse,
  UpdatePartyIdentityProviderRequest,
  UpdatePartyIdentityProviderResponse,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderId,
  ObjectMeta,
  ParticipantOffset,
  PartyDetails,
}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.api.validation.ValueValidator.requirePresence
import com.digitalasset.canton.ledger.error.groups.{
  AuthorizationChecksErrors,
  PartyManagementServiceErrors,
  RequestValidationErrors,
}
import com.digitalasset.canton.ledger.localstore.api.{
  PartyDetailsUpdate,
  PartyRecord,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.digitalasset.canton.ledger.participant.state.index.v2.*
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementService.*
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.platform.apiserver.update
import com.digitalasset.canton.platform.apiserver.update.PartyRecordUpdateMapper
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

private[apiserver] final class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    identityProviderExists: IdentityProviderExists,
    partyRecordStore: PartyRecordStore,
    transactionService: IndexTransactionsService,
    writeService: state.WritePartyService,
    managementServiceTimeout: FiniteDuration,
    submissionIdGenerator: String => Ref.SubmissionId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
) extends PartyManagementService
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(
      writeService,
      partyManagementService,
      loggerFactory,
    ),
    loggerFactory,
  )

  override def close(): Unit = synchronousResponse.close()

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, executionContext)

  override def getParticipantId(
      request: GetParticipantIdRequest
  ): Future[GetParticipantIdResponse] = {
    implicit val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())

    logger.info("Getting Participant ID.")
    partyManagementService
      .getParticipantId()
      .map(pid => GetParticipantIdResponse(pid.toString))
  }

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    withEnrichedLoggingContext(telemetry)(
      logging.partyStrings(request.parties)
    ) { implicit loggingContext =>
      implicit val errorLoggingContext: ErrorLoggingContext =
        new ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)
      logger.info(s"Getting parties, ${loggingContext.serializeFiltered("parties")}.")
      withValidation {
        for {
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
          parties <- request.parties.toList.traverse(requireParty)
        } yield (parties, identityProviderId)
      } { case (parties: Seq[Party], identityProviderId: IdentityProviderId) =>
        for {
          partyDetailsSeq <- partyManagementService.getParties(parties)
          partyRecordOptions <- fetchPartyRecords(partyDetailsSeq)
        } yield {
          val protoDetails =
            partyDetailsSeq
              .zip(partyRecordOptions)
              .map(blindAndConvertToProto(identityProviderId))
          GetPartiesResponse(partyDetails = protoDetails)
        }
      }
    }

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] = {
    implicit val loggingContext =
      LoggingContextWithTrace(loggerFactory, telemetry)

    logger.info("Listing known parties.")
    withValidation {
      optionalIdentityProviderId(
        request.identityProviderId,
        "identity_provider_id",
      )
    } { identityProviderId =>
      for {
        partyDetailsSeq <- partyManagementService.listKnownParties()
        partyRecords <- fetchPartyRecords(partyDetailsSeq)
      } yield {
        val protoDetails = partyDetailsSeq
          .zip(partyRecords)
          .map(blindAndConvertToProto(identityProviderId))
        ListKnownPartiesResponse(protoDetails)
      }
    }
  }

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val submissionId = submissionIdGenerator(request.partyIdHint)
    withEnrichedLoggingContext(telemetry)(
      logging.partyString(request.partyIdHint),
      logging.submissionId(submissionId),
    ) { implicit loggingContext =>
      logger.info(
        s"Allocating party, ${loggingContext.serializeFiltered("submissionId", "parties")}."
      )
      implicit val errorLoggingContext: ErrorLoggingContext =
        new ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)

      withValidation {
        for {
          partyIdHintO <- optionalString(
            request.partyIdHint
          )(requireParty)
          metadata = request.localMetadata.getOrElse(ProtoObjectMeta())
          _ <- requireEmptyString(
            metadata.resourceVersion,
            "party_details.local_metadata.resource_version",
          )
          annotations <- verifyMetadataAnnotations(
            metadata.annotations,
            allowEmptyValues = false,
            "party_details.local_metadata.annotations",
          )
          displayNameO <- optionalString(request.displayName)(Right(_))
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
        } yield (partyIdHintO, displayNameO, annotations, identityProviderId)
      } { case (partyIdHintO, displayNameO, annotations, identityProviderId) =>
        (for {
          _ <- identityProviderExistsOrError(identityProviderId)
          ledgerEndbeforeRequest <- transactionService.currentLedgerEnd().map(Some(_))
          allocated <- synchronousResponse.submitAndWait(
            submissionId,
            (partyIdHintO, displayNameO),
            ledgerEndbeforeRequest,
            managementServiceTimeout,
          )
          _ <- verifyPartyIsNonExistentOrInIdp(
            identityProviderId,
            allocated.partyDetails.party,
          )
          partyRecord <- partyRecordStore
            .createPartyRecord(
              PartyRecord(
                party = allocated.partyDetails.party,
                metadata = domain.ObjectMeta(resourceVersionO = None, annotations = annotations),
                identityProviderId = identityProviderId,
              )
            )
            .flatMap(handlePartyRecordStoreResult("creating a party record")(_))
        } yield {
          val details = toProtoPartyDetails(
            partyDetails = allocated.partyDetails,
            metadataO = Some(partyRecord.metadata),
            identityProviderId = Some(identityProviderId),
          )
          AllocatePartyResponse(Some(details))
        })
      }
    }
  }

  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] = {
    val submissionId = submissionIdGenerator(request.partyDetails.fold("")(_.party))
    withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionId)
    ) { implicit loggingContext =>
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext(logger, loggingContext)
      withValidation {
        for {
          partyDetails <- requirePresence(
            request.partyDetails,
            "party_details",
          )
          party <- requireParty(partyDetails.party)
          metadata = partyDetails.localMetadata.getOrElse(ProtoObjectMeta())
          resourceVersionNumberO <- optionalString(metadata.resourceVersion)(
            requireResourceVersion(
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
          displayNameO <- optionalString(partyDetails.displayName)(Right(_))
          identityProviderId <- optionalIdentityProviderId(
            partyDetails.identityProviderId,
            "identity_provider_id",
          )
          partyRecord = PartyDetails(
            party = party,
            displayName = displayNameO,
            isLocal = partyDetails.isLocal,
            metadata = domain.ObjectMeta(
              resourceVersionO = resourceVersionNumberO,
              annotations = annotations,
            ),
            identityProviderId = identityProviderId,
          )
        } yield (partyRecord, updateMask)
      } { case (partyRecord, updateMask) =>
        for {
          _ <- identityProviderExistsOrError(partyRecord.identityProviderId)
          _ = logger.info(
            s"Updating party: ${request.getPartyDetails.party}, ${loggingContext.serializeFiltered("submissionId")}."
          )
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
                PartyManagementServiceErrors.PartyNotFound
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
                PartyManagementServiceErrors.InvalidUpdatePartyDetailsRequest
                  .Reject(
                    party = partyRecord.party,
                    reason =
                      s"Update request attempted to modify not-modifiable 'is_local' attribute",
                  )
                  .asGrpcError
              )
            } else if (
              partyDetailsUpdate.displayNameUpdate.exists(_ != fetchedPartyDetails.displayName)
            ) {
              Future.failed(
                PartyManagementServiceErrors.InvalidUpdatePartyDetailsRequest
                  .Reject(
                    party = partyRecord.party,
                    reason =
                      s"Update request attempted to modify not-modifiable 'display_name' attribute update: ${partyDetailsUpdate.displayNameUpdate}, fetched: ${fetchedPartyDetails.displayName}",
                  )
                  .asGrpcError
              )
            } else {
              // NOTE: In the current implementation (as of 2022.10.13) a no-op update request
              // will still cause an update of the resourceVersion's value.
              Future.successful(
                PartyRecordUpdate(
                  party = partyDetailsUpdate.party,
                  metadataUpdate = partyDetailsUpdate.metadataUpdate,
                  identityProviderId = partyRecord.identityProviderId,
                )
              )
            }
          }
          _ <- verifyPartyIsNonExistentOrInIdp(
            partyRecordUpdate.identityProviderId,
            partyRecordUpdate.party,
          )
          updatedPartyRecordResult <- partyRecordStore.updatePartyRecord(
            partyRecordUpdate = partyRecordUpdate,
            ledgerPartyIsLocal = fetchedPartyDetailsO.exists(_.isLocal),
          )
          updatedPartyRecord: PartyRecord <- handlePartyRecordStoreResult(
            "updating a participant party record"
          )(updatedPartyRecordResult)
        } yield UpdatePartyDetailsResponse(
          Some(
            toProtoPartyDetails(
              partyDetails = fetchedPartyDetails,
              metadataO = Some(updatedPartyRecord.metadata),
              identityProviderId = Some(updatedPartyRecord.identityProviderId),
            )
          )
        )
      }
    }
  }

  override def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderRequest
  ): Future[UpdatePartyIdentityProviderResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)

    logger.info("Updating party identity provider.")
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)
    withValidation {
      for {
        party <- requireParty(request.party)
        sourceIdentityProviderId <- optionalIdentityProviderId(
          request.sourceIdentityProviderId,
          "source_identity_provider_id",
        )
        targetIdentityProviderId <- optionalIdentityProviderId(
          request.targetIdentityProviderId,
          "target_identity_provider_id",
        )
      } yield (party, sourceIdentityProviderId, targetIdentityProviderId)
    } { case (party, sourceIdentityProviderId, targetIdentityProviderId) =>
      for {
        _ <- identityProviderExistsOrError(sourceIdentityProviderId)
        _ <- identityProviderExistsOrError(targetIdentityProviderId)
        fetchedPartyDetailsO <- partyManagementService
          .getParties(parties = Seq(party))
          .map(_.headOption)
        _ <- fetchedPartyDetailsO match {
          case Some(_) => Future.unit
          case None =>
            Future.failed(
              PartyManagementServiceErrors.PartyNotFound
                .Reject(
                  operation = "updating party's identity provider",
                  party = party,
                )
                .asGrpcError
            )
        }
        result <- partyRecordStore
          .updatePartyRecordIdp(
            party = party,
            ledgerPartyIsLocal = fetchedPartyDetailsO.exists(_.isLocal),
            sourceIdp = sourceIdentityProviderId,
            targetIdp = targetIdentityProviderId,
          )
          .flatMap(handlePartyRecordStoreResult("updating party's identity provider"))
          .map(_ => UpdatePartyIdentityProviderResponse())
      } yield result
    }
  }

  // Check if party either doesn't exist or exists and belongs to the requested Identity Provider
  private def verifyPartyIsNonExistentOrInIdp(
      identityProviderId: IdentityProviderId,
      party: Ref.Party,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[Unit] =
    partyRecordStore.getPartyRecordO(party).flatMap {
      case Right(Some(party)) if party.identityProviderId != identityProviderId =>
        Future.failed(
          AuthorizationChecksErrors.PermissionDenied
            .Reject(
              s"Party $party belongs to an identity provider that differs from the one specified in the request"
            )
            .asGrpcError
        )
      case _ => Future.unit
    }

  private def fetchPartyRecords(
      partyDetails: List[IndexerPartyDetails]
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[List[Option[PartyRecord]]] =
    // Future optimization: Fetch party records from the DB in a batched fashion rather than one-by-one.
    partyDetails.foldLeft(Future.successful(List.empty[Option[PartyRecord]])) {
      (axF: Future[List[Option[PartyRecord]]], partyDetails: IndexerPartyDetails) =>
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
      errorLogger: ContextualizedErrorLogger
  ): Future[T] =
    result match {
      case Left(e: update.UpdatePathError) =>
        Future.failed(
          PartyManagementServiceErrors.InvalidUpdatePartyDetailsRequest
            .Reject(party, reason = e.getReason)
            .asGrpcError
        )
      case Right(t) =>
        Future.successful(t)
    }

  private def handlePartyRecordStoreResult[T](operation: String)(
      result: PartyRecordStore.Result[T]
  )(implicit errorLogger: ContextualizedErrorLogger): Future[T] =
    result match {
      case Left(PartyRecordStore.PartyNotFound(party)) =>
        Future.failed(
          PartyManagementServiceErrors.PartyNotFound
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.PartyRecordNotFoundFatal(party)) =>
        Future.failed(
          PartyManagementServiceErrors.InternalPartyRecordNotFound
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.PartyRecordExistsFatal(party)) =>
        Future.failed(
          PartyManagementServiceErrors.InternalPartyRecordAlreadyExists
            .Reject(operation, party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.ConcurrentPartyUpdate(party)) =>
        Future.failed(
          PartyManagementServiceErrors.ConcurrentPartyDetailsUpdateDetected
            .Reject(party = party)
            .asGrpcError
        )

      case Left(PartyRecordStore.MaxAnnotationsSizeExceeded(party)) =>
        Future.failed(
          PartyManagementServiceErrors.MaxPartyAnnotationsSizeExceeded
            .Reject(party = party)
            .asGrpcError
        )

      case Right(t) =>
        Future.successful(t)
    }

  private def identityProviderExistsOrError(
      id: IdentityProviderId
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[Unit] =
    identityProviderExists(id)
      .flatMap { idpExists =>
        if (idpExists)
          Future.successful(())
        else
          Future.failed(
            RequestValidationErrors.InvalidArgument
              .Reject(s"Provided identity_provider_id $id has not been found.")
              .asGrpcError
          )
      }
}

private[apiserver] object ApiPartyManagementService {

  def blindAndConvertToProto(
      identityProviderId: IdentityProviderId
  ): ((IndexerPartyDetails, Option[PartyRecord])) => ProtoPartyDetails = {
    case (details, recordO) if recordO.map(_.identityProviderId).contains(identityProviderId) =>
      toProtoPartyDetails(
        partyDetails = details,
        metadataO = recordO.map(_.metadata),
        recordO.map(_.identityProviderId),
      )
    case (details, _) if identityProviderId == IdentityProviderId.Default =>
      // For the Default IDP, `isLocal` flag is delivered as is.
      toProtoPartyDetails(partyDetails = details, metadataO = None, identityProviderId = None)
    case (details, _) =>
      // Expose the party, but blind the identity provider and report it as non-local.
      toProtoPartyDetails(
        partyDetails = details.copy(isLocal = false),
        metadataO = None,
        identityProviderId = None,
      )
  }

  private def toProtoPartyDetails(
      partyDetails: IndexerPartyDetails,
      metadataO: Option[ObjectMeta],
      identityProviderId: Option[IdentityProviderId],
  ): ProtoPartyDetails =
    ProtoPartyDetails(
      party = partyDetails.party,
      displayName = partyDetails.displayName.getOrElse(""),
      isLocal = partyDetails.isLocal,
      localMetadata = Some(Utils.toProtoObjectMeta(metadataO.getOrElse(ObjectMeta.empty))),
      identityProviderId = identityProviderId.map(_.toRequestString).getOrElse(""),
    )

  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      identityProviderExists: IdentityProviderExists,
      partyRecordStore: PartyRecordStore,
      transactionsService: IndexTransactionsService,
      writeBackend: state.WritePartyService,
      managementServiceTimeout: FiniteDuration,
      submissionIdGenerator: String => Ref.SubmissionId = CreateSubmissionId.withPrefix,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): PartyManagementServiceGrpc.PartyManagementService & GrpcApiService =
    new ApiPartyManagementService(
      partyManagementServiceBackend,
      identityProviderExists,
      partyRecordStore,
      transactionsService,
      writeBackend,
      managementServiceTimeout,
      submissionIdGenerator,
      telemetry,
      loggerFactory,
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
      writeService: state.WritePartyService,
      partyManagementService: IndexPartyManagementService,
      val loggerFactory: NamedLoggerFactory,
  ) extends SynchronousResponse.Strategy[
        (Option[Ref.Party], Option[String]),
        PartyEntry,
        PartyEntry.AllocationAccepted,
      ]
      with NamedLogging {

    override def submit(
        submissionId: Ref.SubmissionId,
        input: (Option[Ref.Party], Option[String]),
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[state.SubmissionResult] = {
      val (party, displayName) = input
      writeService.allocateParty(party, displayName, submissionId).asScala
    }

    override def entries(offset: Option[ParticipantOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[PartyEntry, ?] =
      partyManagementService.partyEntries(offset)

    override def accept(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PartyEntry, PartyEntry.AllocationAccepted] = {
      case entry @ PartyEntry.AllocationAccepted(Some(`submissionId`), _) => entry
    }

    override def reject(
        submissionId: Ref.SubmissionId
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): PartialFunction[PartyEntry, StatusRuntimeException] = {
      case PartyEntry.AllocationRejected(`submissionId`, reason) =>
        ValidationErrors.invalidArgument(reason)(
          LedgerErrorLoggingContext(
            logger,
            loggingContext.toPropertiesMap,
            loggingContext.traceContext,
            submissionId,
          )
        )
    }
  }

}
