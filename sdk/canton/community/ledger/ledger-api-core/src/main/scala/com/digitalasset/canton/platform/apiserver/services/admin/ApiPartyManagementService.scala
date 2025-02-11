// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta as ProtoObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v2.admin.party_management_service.{
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
  UpdatePartyIdentityProviderIdRequest,
  UpdatePartyIdentityProviderIdResponse,
}
import com.daml.logging.LoggingContext
import com.daml.platform.v1.page_tokens.ListPartiesPageTokenPayload
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.AuthorizationChecksErrors
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.api.validation.ValueValidator.requirePresence
import com.digitalasset.canton.ledger.api.{IdentityProviderId, ObjectMeta, PartyDetails}
import com.digitalasset.canton.ledger.error.groups.{
  PartyManagementServiceErrors,
  RequestValidationErrors,
}
import com.digitalasset.canton.ledger.localstore.api.{
  PartyDetailsUpdate,
  PartyRecord,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementService.*
import com.digitalasset.canton.platform.apiserver.services.admin.PartyAllocation
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.platform.apiserver.services.tracking.StreamTracker
import com.digitalasset.canton.platform.apiserver.update
import com.digitalasset.canton.platform.apiserver.update.PartyRecordUpdateMapper
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import io.grpc.Status.Code.ALREADY_EXISTS
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import io.opentelemetry.api.trace.Tracer
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

private[apiserver] final class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    identityProviderExists: IdentityProviderExists,
    maxPartiesPageSize: PositiveInt,
    partyRecordStore: PartyRecordStore,
    updateService: IndexUpdateService,
    syncService: state.PartySyncService,
    managementServiceTimeout: FiniteDuration,
    submissionIdGenerator: CreateSubmissionId,
    telemetry: Telemetry,
    partyAllocationTracker: PartyAllocation.Tracker,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    tracer: Tracer,
) extends PartyManagementService
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  override def close(): Unit = partyAllocationTracker.close()

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
      .map(GetParticipantIdResponse.apply)
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
    withValidation(
      for {
        fromExcl <- decodePartyFromPageToken(request.pageToken)
        _ <- Either.cond(
          request.pageSize >= 0,
          request.pageSize,
          RequestValidationErrors.InvalidArgument
            .Reject("Page size must be non-negative")
            .asGrpcError,
        )
        _ <- Either.cond(
          request.pageSize <= maxPartiesPageSize.value,
          request.pageSize,
          RequestValidationErrors.InvalidArgument
            .Reject(s"Page size must not exceed the server's maximum of $maxPartiesPageSize")
            .asGrpcError,
        )
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
        pageSize =
          if (request.pageSize == 0) maxPartiesPageSize.value
          else request.pageSize
      } yield {
        (fromExcl, pageSize, identityProviderId)
      }
    ) { case (fromExcl, pageSize, identityProviderId) =>
      for {
        partyDetailsSeq <- partyManagementService.listKnownParties(fromExcl, pageSize)
        partyRecords <- fetchPartyRecords(partyDetailsSeq)
      } yield {
        val protoDetails = partyDetailsSeq
          .zip(partyRecords)
          .map(blindAndConvertToProto(identityProviderId))
        val lastParty =
          if (partyDetailsSeq.sizeIs < pageSize) None else partyDetailsSeq.lastOption.map(_.party)
        ListKnownPartiesResponse(protoDetails, encodeNextPageToken(lastParty))
      }
    }
  }

  implicit object PartyAllocationErrors extends StreamTracker.Errors[PartyAllocation.TrackerKey] {
    import com.digitalasset.canton.ledger.error.CommonErrors

    def timedOut(key: PartyAllocation.TrackerKey)(implicit
        errorLogger: ContextualizedErrorLogger
    ): StatusRuntimeException =
      CommonErrors.RequestTimeOut
        .Reject(
          s"Timed out while awaiting item corresponding to ${key.submissionId}.",
          definiteAnswer = false,
        )
        .asGrpcError

    def duplicated(
        key: PartyAllocation.TrackerKey
    )(implicit errorLogger: ContextualizedErrorLogger): StatusRuntimeException =
      CommonErrors.RequestAlreadyInFlight
        .Reject(requestId = key.submissionId)
        .asGrpcError
  }

  private def generatePartyName: Ref.Party = {
    import java.util.UUID
    Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString}")
  }

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    withEnrichedLoggingContext(telemetry)(
      logging.partyString(request.partyIdHint)
    ) { implicit loggingContext =>
      logger.info(
        s"Allocating party, ${loggingContext.serializeFiltered("submissionId", "parties")}."
      )
      implicit val errorLoggingContext: ErrorLoggingContext =
        new ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)
      import com.digitalasset.canton.config.NonNegativeFiniteDuration

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
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
        } yield (partyIdHintO, annotations, identityProviderId)
      } { case (partyIdHintO, annotations, identityProviderId) =>
        val partyName = partyIdHintO.getOrElse(generatePartyName)
        val trackerKey = submissionIdGenerator(partyName)
        withEnrichedLoggingContext(telemetry)(logging.submissionId(trackerKey.submissionId)) {
          implicit loggingContext =>
            for {
              _ <- identityProviderExistsOrError(identityProviderId)
              ledgerEndbeforeRequest <- updateService.currentLedgerEnd()
              allocated <- partyAllocationTracker
                .track(
                  trackerKey,
                  NonNegativeFiniteDuration(managementServiceTimeout),
                ) { _ =>
                  FutureUnlessShutdown {
                    for {
                      result <- syncService
                        .allocateParty(partyName, trackerKey.submissionId)
                        .toScalaUnwrapped
                      _ <- checkSubmissionResult(result)
                    } yield UnlessShutdown.unit
                  }
                }
                .transform(alreadyExistsError(trackerKey.submissionId, loggingContext))
              _ <- verifyPartyIsNonExistentOrInIdp(
                identityProviderId,
                allocated.partyDetails.party,
              )
              partyRecord <- partyRecordStore
                .createPartyRecord(
                  PartyRecord(
                    party = allocated.partyDetails.party,
                    metadata = ObjectMeta(resourceVersionO = None, annotations = annotations),
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
            }
        }
      }
    }

  private def checkSubmissionResult(r: state.SubmissionResult) = r match {
    case state.SubmissionResult.Acknowledged =>
      Future.successful(())
    case synchronousError: state.SubmissionResult.SynchronousError =>
      Future.failed(synchronousError.exception)
  }

  private def alreadyExistsError[R](
      submissionId: Ref.SubmissionId,
      loggingContext: LoggingContextWithTrace,
  )(r: Try[R]) = r match {
    case Failure(e: StatusRuntimeException) if e.getStatus.getCode == ALREADY_EXISTS =>
      Failure(
        ValidationErrors.invalidArgument(e.getStatus.getDescription)(
          LedgerErrorLoggingContext(
            logger,
            loggingContext.toPropertiesMap,
            loggingContext.traceContext,
            submissionId,
          )
        )
      )
    case x => x
  }

  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] = {
    val submissionId = submissionIdGenerator(request.partyDetails.fold("")(_.party)).submissionId
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
          identityProviderId <- optionalIdentityProviderId(
            partyDetails.identityProviderId,
            "identity_provider_id",
          )
          partyRecord = PartyDetails(
            party = party,
            isLocal = partyDetails.isLocal,
            metadata = ObjectMeta(
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
              apiObject = partyRecord,
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
      request: UpdatePartyIdentityProviderIdRequest
  ): Future[UpdatePartyIdentityProviderIdResponse] = {
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
          .map(_ => UpdatePartyIdentityProviderIdResponse())
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
      isLocal = partyDetails.isLocal,
      localMetadata = Some(Utils.toProtoObjectMeta(metadataO.getOrElse(ObjectMeta.empty))),
      identityProviderId = identityProviderId.map(_.toRequestString).getOrElse(""),
    )

  def createApiService(
      partyManagementServiceBackend: IndexPartyManagementService,
      identityProviderExists: IdentityProviderExists,
      maxPartiesPageSize: PositiveInt,
      partyRecordStore: PartyRecordStore,
      updateService: IndexUpdateService,
      writeBackend: state.PartySyncService,
      managementServiceTimeout: FiniteDuration,
      submissionIdGenerator: CreateSubmissionId,
      telemetry: Telemetry,
      partyAllocationTracker: PartyAllocation.Tracker,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      tracer: Tracer,
  ): PartyManagementServiceGrpc.PartyManagementService & GrpcApiService =
    new ApiPartyManagementService(
      partyManagementServiceBackend,
      identityProviderExists,
      maxPartiesPageSize,
      partyRecordStore,
      updateService,
      writeBackend,
      managementServiceTimeout,
      submissionIdGenerator,
      telemetry,
      partyAllocationTracker,
      loggerFactory,
    )

  def decodePartyFromPageToken(pageToken: String)(implicit
      loggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[Ref.Party]] =
    if (pageToken.isEmpty) {
      Right(None)
    } else {
      val bytes = pageToken.getBytes(StandardCharsets.UTF_8)
      for {
        decodedBytes <- Try[Array[Byte]](Base64.getUrlDecoder.decode(bytes)).toEither.left
          .map(_ => invalidPageToken("failed base64 decoding"))
        tokenPayload <- Try[ListPartiesPageTokenPayload] {
          ListPartiesPageTokenPayload.parseFrom(decodedBytes)
        }.toEither.left
          .map(_ => invalidPageToken("failed proto decoding"))
        party <- Ref.Party
          .fromString(tokenPayload.partyIdLowerBoundExcl)
          .map(Some(_))
          .left
          .map(_ => invalidPageToken("invalid party string in the token"))
      } yield {
        party
      }
    }

  private def invalidPageToken(details: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    errorLogger.info(s"Invalid page token: $details")
    RequestValidationErrors.InvalidArgument
      .Reject("Invalid page token")
      .asGrpcError
  }

  def encodeNextPageToken(token: Option[Party]): String =
    token
      .map { id =>
        val bytes = Base64.getUrlEncoder.encode(
          ListPartiesPageTokenPayload(partyIdLowerBoundExcl = id).toByteArray
        )
        new String(bytes, StandardCharsets.UTF_8)
      }
      .getOrElse("")

  trait CreateSubmissionId {
    def apply(partyIdHint: String): PartyAllocation.TrackerKey
  }

  object CreateSubmissionId {
    import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel

    def forParticipant(participantId: Ref.ParticipantId) = new CreateSubmissionId() {
      override def apply(partyIdHint: String): PartyAllocation.TrackerKey =
        PartyAllocation.TrackerKey.of(partyIdHint, participantId, AuthorizationLevel.Submission)
    }

    def fixedForTests(const: String) = new CreateSubmissionId() {
      override def apply(partyIdHint: String): PartyAllocation.TrackerKey =
        PartyAllocation.TrackerKey.forTests(Ref.SubmissionId.assertFromString(const))
    }

  }
}
