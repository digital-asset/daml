// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.syntax.either.*
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta as ProtoObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.AllocateExternalPartyRequest.SignedTransaction
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  AllocateExternalPartyResponse,
  AllocatePartyRequest,
  AllocatePartyResponse,
  GenerateExternalPartyTopologyRequest,
  GenerateExternalPartyTopologyResponse,
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
import com.daml.nonempty.NonEmpty
import com.daml.platform.v1.page_tokens.ListPartiesPageTokenPayload
import com.daml.tracing.Telemetry
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.auth.AuthorizationChecksErrors
import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.v30.{SigningKeyScheme, SigningKeyUsage}
import com.digitalasset.canton.crypto.{Signature, SigningKeysWithThreshold, SigningPublicKey, v30}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.FieldValidator.{requireParty, *}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.api.validation.ValueValidator.requirePresence
import com.digitalasset.canton.ledger.api.validation.{CryptoValidator, ValidationErrors}
import com.digitalasset.canton.ledger.api.{
  IdentityProviderId,
  ObjectMeta,
  PartyDetails,
  User,
  UserRight,
}
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.error.groups.{
  PartyManagementServiceErrors,
  RequestValidationErrors,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore.UserInfo
import com.digitalasset.canton.ledger.localstore.api.{
  ObjectMetaUpdate,
  PartyDetailsUpdate,
  PartyRecord,
  PartyRecordStore,
  PartyRecordUpdate,
  UserManagementStore,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.Observation
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.ledger.participant.state.index.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementService.*
import com.digitalasset.canton.platform.apiserver.services.admin.AuthenticatedUserContextResolver.AuthenticatedUserContext
import com.digitalasset.canton.platform.apiserver.services.admin.{
  PartyAllocation,
  PendingPartyAllocations,
}
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.platform.apiserver.services.tracking.StreamTracker
import com.digitalasset.canton.platform.apiserver.update
import com.digitalasset.canton.platform.apiserver.update.PartyRecordUpdateMapper
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyTransaction.PositiveTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.daml.lf.data.Ref
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
    userManagementStore: UserManagementStore,
    identityProviderExists: IdentityProviderExists,
    maxPartiesPageSize: PositiveInt,
    maxSelfAllocatedParties: NonNegativeInt,
    partyRecordStore: PartyRecordStore,
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
    with NamedLogging
    with AuthenticatedUserContextResolver {

  private val pendingPartyAllocations = new PendingPartyAllocations()

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
        ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)
      logger.info(s"Getting parties, ${loggingContext.serializeFiltered("parties")}.")
      withValidation {
        for {
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
          parties <- request.parties.toList.traverse(requireParty)
        } yield (parties, identityProviderId)
      } { case (parties: Seq[Ref.Party], identityProviderId: IdentityProviderId) =>
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

  private def parsePartyFilter(
      unsafeString: String
  )(implicit traceContext: TraceContext): Either[StatusRuntimeException, Option[String185]] = if (
    unsafeString.isEmpty
  )
    Either.right(None)
  else
    (for {
      validated <- Ref.Party.fromString(unsafeString)
      limited <- String185.create(validated)
    } yield Some(limited)).leftMap(err =>
      RequestValidationErrors.InvalidField.Reject("filterString", err).asGrpcError
    )

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] = {
    implicit val loggingContext =
      LoggingContextWithTrace(loggerFactory, telemetry)
    val ListKnownPartiesRequest(pageToken, pageSize, identityProviderId, filterString) = request
    logger.info("Listing known parties.")
    withValidation(
      for {
        fromExcl <- decodePartyFromPageToken(pageToken)
        _ <- Either.cond(
          pageSize >= 0,
          pageSize,
          RequestValidationErrors.InvalidArgument
            .Reject("Page size must be non-negative")
            .asGrpcError,
        )
        _ <- Either.cond(
          pageSize <= maxPartiesPageSize.value,
          pageSize,
          RequestValidationErrors.InvalidArgument
            .Reject(s"Page size must not exceed the server's maximum of $maxPartiesPageSize")
            .asGrpcError,
        )
        identityProviderId <- optionalIdentityProviderId(
          identityProviderId,
          "identity_provider_id",
        )
        pageSizeOrDefault =
          if (pageSize == 0) maxPartiesPageSize.value
          else pageSize
        filterStringParsed <- parsePartyFilter(filterString)
      } yield {
        (fromExcl, pageSizeOrDefault, identityProviderId, filterStringParsed)
      }
    ) { case (fromExcl, pageSizeOrDefault, identityProviderId, filterStringParsed) =>
      for {
        partyDetailsSeq <- partyManagementService.listKnownParties(
          fromExcl,
          filterStringParsed,
          pageSizeOrDefault,
        )
        partyRecords <- fetchPartyRecords(partyDetailsSeq)
      } yield {
        val protoDetails = partyDetailsSeq
          .zip(partyRecords)
          .map(blindAndConvertToProto(identityProviderId))
        val lastParty =
          if (partyDetailsSeq.sizeIs < pageSizeOrDefault) None
          else partyDetailsSeq.lastOption.map(_.party)
        ListKnownPartiesResponse(protoDetails, encodeNextPageToken(lastParty))
      }
    }
  }

  implicit object PartyAllocationErrors extends StreamTracker.Errors[PartyAllocation.TrackerKey] {
    import com.digitalasset.canton.ledger.error.CommonErrors

    def timedOut(key: PartyAllocation.TrackerKey)(implicit
        errorLogger: ErrorLoggingContext
    ): StatusRuntimeException =
      CommonErrors.RequestTimeOut
        .Reject(
          s"Timed out while awaiting item corresponding to ${key.submissionId}.",
          definiteAnswer = false,
        )
        .asGrpcError

    def duplicated(
        key: PartyAllocation.TrackerKey
    )(implicit errorLogger: ErrorLoggingContext): StatusRuntimeException =
      CommonErrors.RequestAlreadyInFlight
        .Reject(
          requestId = key.submissionId,
          details = s"Party ${key.partyId} is in the process of being allocated on this node.",
        )
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
        ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)
      // Retrieving the authenticated user context from the thread-local context
      val authenticatedUserContextF: Future[AuthenticatedUserContext] =
        resolveAuthenticatedUserContext
      import com.digitalasset.canton.config.NonNegativeFiniteDuration

      withValidation {
        for {
          partyIdHintO <- optionalString(
            request.partyIdHint
          )(requireParty)
          metadata = request.localMetadata.getOrElse(
            ProtoObjectMeta(
              resourceVersion = "",
              annotations = Map.empty,
            )
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
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
          synchronizerIdO <- optionalSynchronizerId(request.synchronizerId, "synchronizer_id")
          userId <- optionalUserId(request.userId, "user_id")
          partyName = partyIdHintO.getOrElse(generatePartyName)
          partyId <- UniqueIdentifier
            .create(partyName, syncService.participantId.namespace)
            .map(PartyId(_))
            .leftMap(err => invalidArgument(s"Invalid party ID: $err"))
        } yield (partyId, annotations, identityProviderId, synchronizerIdO, userId)
      } { case (partyId, annotations, identityProviderId, synchronizerIdO, userId) =>
        val trackerKey = submissionIdGenerator(partyId.toLf, AuthorizationLevel.Submission)
        withEnrichedLoggingContext(logging.submissionId(trackerKey.submissionId)) {
          implicit loggingContext =>
            pendingPartyAllocations.withUser(userId) { outstandingCalls =>
              for {
                _ <- identityProviderExistsOrError(identityProviderId)
                userInfo <- getUserIfUserSpecified(userId, identityProviderId)
                _ <- checkUserLimitsIfUserSpecified(
                  userInfo.map(_.rights),
                  outstandingCalls,
                  authenticatedUserContextF,
                )
                allocated <- partyAllocationTracker
                  .track(
                    trackerKey,
                    NonNegativeFiniteDuration(managementServiceTimeout),
                  ) { _ =>
                    for {
                      result <- syncService.allocateParty(
                        partyId,
                        trackerKey.submissionId,
                        synchronizerIdO,
                        externalPartyOnboardingDetails = None,
                      )
                      _ <- checkSubmissionResult(result)
                    } yield ()
                  }
                  .transform(alreadyExistsError(trackerKey.submissionId, loggingContext))
                _ <- verifyPartyIsNonExistentOrInIdp(
                  identityProviderId,
                  allocated.partyDetails.party,
                )
                existingPartyRecord <- partyRecordStore.getPartyRecordO(
                  allocated.partyDetails.party
                )
                partyRecord <- updateOrCreatePartyRecord(
                  existingPartyRecord,
                  allocated.partyDetails.party,
                  identityProviderId,
                  annotations,
                )
                _ <- updateUserInfoIfUserSpecified(
                  allocated.partyDetails.party,
                  userInfo.map(_.user),
                )
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
    }

  private def updateUserInfoIfUserSpecified(
      party: Ref.Party,
      user: Option[User],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] =
    user.fold(Future.successful(())) { u =>
      userManagementStore
        .grantRights(
          u.id,
          Set(UserRight.CanActAs(party)),
          u.identityProviderId,
        )
        .map(_.left.map {
          case UserManagementStore.UserNotFound(id) =>
            UserManagementStore.UserDeletedWhileUpdating(id)
          case other => other
        })
        .flatMap(Utils.handleResult("granting user rights for a new party"))
        .map(_ => ())
    }

  private def updateOrCreatePartyRecord(
      existingPartyRecord: PartyRecordStore.Result[Option[PartyRecord]],
      party: Ref.Party,
      identityProviderId: IdentityProviderId,
      annotations: Map[String, String],
  )(implicit loggingContext: LoggingContextWithTrace): Future[PartyRecord] =
    if (existingPartyRecord.exists(_.nonEmpty)) {
      partyRecordStore
        .updatePartyRecord(
          PartyRecordUpdate(
            party = party,
            identityProviderId = identityProviderId,
            metadataUpdate = ObjectMetaUpdate(
              resourceVersionO = None,
              annotationsUpdateO = Some(annotations),
            ),
          ),
          ledgerPartyIsLocal = true,
        )
        .flatMap(handlePartyRecordStoreResult("updating a party record")(_))
    } else {
      partyRecordStore
        .createPartyRecord(
          PartyRecord(
            party = party,
            metadata = ObjectMeta(resourceVersionO = None, annotations = annotations),
            identityProviderId = identityProviderId,
          )
        )
        .flatMap(handlePartyRecordStoreResult("creating a party record")(_))
    }

  private def checkSubmissionResult(r: state.SubmissionResult) = r match {
    case state.SubmissionResult.Acknowledged => FutureUnlessShutdown.unit
    case synchronousError: state.SubmissionResult.SynchronousError =>
      FutureUnlessShutdown.failed(synchronousError.exception)
  }

  private def alreadyExistsError[R](
      submissionId: Ref.SubmissionId,
      loggingContext: LoggingContextWithTrace,
  )(r: Try[R]) = r match {
    case Failure(e: StatusRuntimeException) if e.getStatus.getCode == ALREADY_EXISTS =>
      Failure(
        ValidationErrors.invalidArgument(e.getStatus.getDescription)(
          ErrorLoggingContext.withExplicitCorrelationId(
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
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContext)
    withValidation {
      for {
        partyDetails <- requirePresence(
          request.partyDetails,
          "party_details",
        )
        party <- requireParty(partyDetails.party)
        metadata = partyDetails.localMetadata.getOrElse(
          ProtoObjectMeta(
            resourceVersion = "",
            annotations = Map.empty,
          )
        )
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
                  reason = s"Update request attempted to modify not-modifiable 'is_local' attribute",
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
      errorLoggingContext: ErrorLoggingContext,
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
      errorLogger: ErrorLoggingContext
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
  )(implicit errorLogger: ErrorLoggingContext): Future[T] =
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

  private def getUserIfUserSpecified(
      userId: Option[Ref.UserId],
      identityProviderId: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[UserInfo]] =
    userId.fold[Future[Option[UserInfo]]](Future.successful(None))(
      userManagementStore
        .getUserInfo(_, identityProviderId)
        .flatMap(result => Utils.handleResult("checking user's existence")(result).map(Some(_)))
    )

  private def checkUserLimitsIfUserSpecified(
      userRights: Option[Set[UserRight]],
      outstandingCalls: Int,
      authenticatedUserContextF: Future[AuthenticatedUserContext],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] =
    userRights match {
      case None => Future.unit
      case Some(rights) =>
        for {
          authenticatedUserContext <- authenticatedUserContextF
          resultingRightsCount = rights.flatMap(_.getParty).size + outstandingCalls
          _ <-
            if (
              authenticatedUserContext.isRegularUser && resultingRightsCount > maxSelfAllocatedParties.unwrap
            )
              Future.failed(
                AuthorizationChecksErrors.PermissionDenied
                  .Reject(s"User quota of party allocations exhausted")
                  .asGrpcError
              )
            else
              Future.unit
        } yield ()
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
          Future.unit
        else
          Future.failed(
            RequestValidationErrors.InvalidArgument
              .Reject(s"Provided identity_provider_id $id has not been found.")
              .asGrpcError
          )
      }

  private def parseSignedTransaction(
      protocolVersion: ProtocolVersion,
      signedTransaction: SignedTransaction,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[
    StatusRuntimeException,
    (PositiveTopologyTransaction, List[Signature]),
  ] =
    for {
      transaction <- TopologyTransaction
        .fromByteString(
          // TODO(i27619): We may be able to not validate the protocol version here
          // depending on the trust we put in the input
          // Note that pinning to a protocol version makes it not possible to use transactions
          // generated with an earlier protocol version (e.g in between synchronizer updates)
          ProtocolVersionValidation(protocolVersion),
          signedTransaction.transaction,
        )
        .leftMap(error =>
          ValidationErrors.invalidField(
            "onboarding_transactions.transaction",
            s"Invalid transaction: ${error.message}",
          )
        )
      positiveTransaction <- transaction
        .selectOp[TopologyChangeOp.Replace]
        .toRight(
          ValidationErrors.invalidField(
            "onboarding_transactions.transaction",
            s"Onboarding topology transactions must be Replace operations",
          )
        )
      _ <- Either.cond(
        positiveTransaction.serial == PositiveInt.one,
        (),
        ValidationErrors.invalidField(
          "onboarding_transactions.transaction.serial",
          "Onboarding transaction serial must be 1",
        ),
      )
      signatures <- signedTransaction.signatures.toList.traverse(
        CryptoValidator.validateSignature(_, "onboarding_transaction.signatures")
      )
    } yield (positiveTransaction, signatures)

  override def allocateExternalParty(
      request: AllocateExternalPartyRequest
  ): Future[AllocateExternalPartyResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(telemetry)(this.loggingContext)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)
    import com.digitalasset.canton.config.NonNegativeFiniteDuration

    withValidation {
      for {
        synchronizerId <- requireSynchronizerId(request.synchronizer, "synchronizer")
          .orElse(
            // Take a physical synchronizer ID too
            requirePhysicalSynchronizerId(request.synchronizer, "synchronizer").map(_.logical)
          )
        protocolVersion <- syncService
          .protocolVersionForSynchronizerId(synchronizerId)
          .toRight(
            ValidationErrors.invalidArgument(
              s"This node is not connected to the requested synchronizer $synchronizerId."
            )
          )
        transactionsWithSignatures <- request.onboardingTransactions.toList.traverse(
          parseSignedTransaction(protocolVersion, _)
        )
        signedTransactionsNE <- NonEmpty
          .from(transactionsWithSignatures)
          .toRight(
            ValidationErrors
              .invalidField("onboarding_transactions.transactions", "Transactions field is empty")
          )
        parsedMultiSignatures <- request.multiHashSignatures.toList.traverse(
          CryptoValidator.validateSignature(_, "multi_hash_signatures.signatures")
        )
        _ = logger.debug(
          s"External party allocation input transactions:\n ${signedTransactionsNE.map(_._1).mkString("\n")}"
        )
        cantonParticipantId = this.syncService.participantId
        externalPartyDetails <- ExternalPartyOnboardingDetails
          .create(signedTransactionsNE, parsedMultiSignatures, protocolVersion, cantonParticipantId)
          .leftMap(ValidationErrors.invalidArgument(_))
      } yield (synchronizerId, externalPartyDetails)
    } { case (synchronizerId, externalPartyOnboardingDetails) =>
      val hostingParticipantsString = externalPartyOnboardingDetails.hostingParticipants
        .map { case HostingParticipant(participantId, permission, _onboarding) =>
          s"$participantId -> $permission"
        }
        .mkString("[", ", ", "]")
      val signingKeysString =
        externalPartyOnboardingDetails.optionallySignedPartyToParticipant.mapping.partySigningKeysWithThreshold
          .map { case SigningKeysWithThreshold(keys, threshold) =>
            s" and ${keys.size} signing keys with threshold ${threshold.value}"
          }
          .getOrElse("")
      logger.info(
        s"Allocating external party ${externalPartyOnboardingDetails.partyId.toProtoPrimitive} on" +
          s" $hostingParticipantsString with confirmation threshold ${externalPartyOnboardingDetails.confirmationThreshold.value}" + signingKeysString
      )
      val trackerKey =
        submissionIdGenerator(
          externalPartyOnboardingDetails.partyId.toLf,
          authorizationLevel =
            if (externalPartyOnboardingDetails.isConfirming) AuthorizationLevel.Confirmation
            else Observation,
        )
      withEnrichedLoggingContext(telemetry)(logging.submissionId(trackerKey.submissionId)) {
        implicit loggingContext =>
          def allocateFn = for {
            result <- syncService.allocateParty(
              externalPartyOnboardingDetails.partyId,
              trackerKey.submissionId,
              Some(synchronizerId),
              Some(externalPartyOnboardingDetails),
            )
            _ <- checkSubmissionResult(result)
          } yield ()

          // Only track the party if we expect it to be fully authorized
          // Otherwise the party won't be fully onboarded here so this would time out
          val partyIdF =
            if (externalPartyOnboardingDetails.fullyAllocatesParty) {
              partyAllocationTracker
                .track(
                  trackerKey,
                  NonNegativeFiniteDuration(managementServiceTimeout),
                )(_ => allocateFn)
                .map(_.partyDetails.party)
            } else {
              allocateFn
                .map(_ => externalPartyOnboardingDetails.partyId.toProtoPrimitive)
                .failOnShutdownTo(
                  CommonErrors.ServiceNotRunning.Reject("PartyManagementService").asGrpcError
                )
            }
          partyIdF
            .map(AllocateExternalPartyResponse.apply)
            .transform(alreadyExistsError(trackerKey.submissionId, loggingContext))
      }
    }
  }

  override def generateExternalPartyTopology(
      request: GenerateExternalPartyTopologyRequest
  ): Future[GenerateExternalPartyTopologyResponse] = {
    import io.scalaland.chimney.dsl.*
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, LoggingContextWithTrace(telemetry))
    val GenerateExternalPartyTopologyRequest(
      synchronizerIdP,
      partyHint,
      publicKeyO,
      localParticipantObservationOnly,
      otherConfirmingParticipantUids,
      confirmationThreshold,
      observingParticipantUids,
    ) = request

    val participantId = syncService.participantId

    val availableConfirmers =
      (if (localParticipantObservationOnly) 0 else 1) + otherConfirmingParticipantUids.size

    val response = for {
      publicKeyP <- ProtoConverter.required("public_key", publicKeyO).leftMap(_.message)
      publicKeyT <- publicKeyP
        .intoPartial[v30.SigningPublicKey]
        .withFieldConst(_.scheme, SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED)
        .withFieldConst(
          _.usage,
          Seq(
            SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE,
            SigningKeyUsage.SIGNING_KEY_USAGE_PROOF_OF_OWNERSHIP,
            SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL,
          ),
        )
        .withFieldRenamed(_.keyData, _.publicKey)
        .transform
        .asEither
        .leftMap(_.asErrorPathMessages.map { case (p, e) => s"$p: $e" }.mkString(", "))
      pubKey <- SigningPublicKey
        .fromProtoV30(publicKeyT)
        .leftMap(_.message)
      namespace = Namespace(pubKey.fingerprint)
      synchronizerIdWithVersion <- UniqueIdentifier
        .fromProtoPrimitive_(synchronizerIdP)
        .map(SynchronizerId(_))
        .leftMap(_.message)
        .flatMap(synchronizerId =>
          syncService
            .protocolVersionForSynchronizerId(synchronizerId)
            .toRight(s"Unknown or not connected synchronizer $synchronizerId")
            .map((synchronizerId, _))
        )
      _ <- Either.cond(partyHint.nonEmpty, (), "Party hint is empty")
      _ <- UniqueIdentifier.verifyValidString(partyHint).leftMap(x => "party_hint: " + x)
      uid <- UniqueIdentifier.create(partyHint, namespace)
      _ <- Either.cond(confirmationThreshold >= 0, (), "Negative confirmation threshold observed")
      confirmingPids <- otherConfirmingParticipantUids.toList
        .traverse(UniqueIdentifier.fromProtoPrimitive_)
        .leftMap(_.message)
      observingPids <- observingParticipantUids.toList
        .traverse(UniqueIdentifier.fromProtoPrimitive_)
        .leftMap(_.message)
      _ <- Either.cond(
        !confirmingPids.contains(participantId.uid),
        (),
        s"This participant node ($participantId) is also listed in 'otherConfirmingParticipantUids'." +
          s" By sending the request to this node, it is de facto a hosting node and must not be listed in 'otherConfirmingParticipantUids'.",
      )
      _ <- Either.cond(
        !observingPids.contains(participantId.uid),
        (),
        s"This participant node ($participantId) is also listed in 'observingParticipantUids'." +
          s" By sending the request to this node, it is de facto a hosting node and must not be listed in 'observingParticipantUids'.",
      )
      allParticipantIds = (confirmingPids ++ observingPids)
      _ <- Either.cond(
        allParticipantIds.distinct.sizeIs == allParticipantIds.size,
        (), {
          val duplicates =
            allParticipantIds.groupBy(identity).collect { case (x, ys) if ys.sizeIs > 1 => x }
          s"The following participant IDs are referenced multiple times in the request: ${duplicates
              .mkString(", ")}." +
            s" Please ensure all IDs are referenced only once" +
            s" across 'otherConfirmingParticipantUids' and 'observingParticipantUids' fields."
        },
      )
      _ <- Either.cond(
        confirmationThreshold <= availableConfirmers,
        (),
        "Confirmation threshold exceeds number of confirming participants",
      )
      threshold =
        if (confirmationThreshold == 0) availableConfirmers
        else confirmationThreshold
      party = PartyId(uid)
      p2p <- PartyToParticipant.create(
        party,
        threshold = PositiveInt.tryCreate(threshold),
        HostingParticipant(
          participantId,
          if (localParticipantObservationOnly) ParticipantPermission.Observation
          else ParticipantPermission.Confirmation,
        ) +: (confirmingPids.map(uid =>
          HostingParticipant(ParticipantId(uid), ParticipantPermission.Confirmation)
        ) ++ observingPids.map(uid =>
          HostingParticipant(ParticipantId(uid), ParticipantPermission.Observation)
        )),
        partySigningKeysWithThreshold = Some(
          SigningKeysWithThreshold.tryCreate(
            keys = NonEmpty.mk(Seq, pubKey),
            threshold = PositiveInt.one,
          )
        ),
      )
    } yield {
      val (_synchronizerId, protocolVersion) = synchronizerIdWithVersion
      val transactions =
        NonEmpty
          .mk(List, p2p)
          .map(mapping =>
            TopologyTransaction(
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.one,
              mapping = mapping,
              protocolVersion = protocolVersion,
            )
          )

      GenerateExternalPartyTopologyResponse(
        partyId = party.toProtoPrimitive,
        publicKeyFingerprint = pubKey.fingerprint.toProtoPrimitive,
        topologyTransactions = transactions.map(_.toByteString),
        multiHash = MultiTransactionSignature
          .computeCombinedHash(
            transactions.map(_.hash).toSet,
            syncService.hashOps,
          )
          .getCryptographicEvidence,
      )

    }
    response match {
      case Left(err) =>
        Future.failed(
          RequestValidationErrors.InvalidArgument
            .Reject(err)
            .asGrpcError
        )
      case Right(resp) => Future.successful(resp)
    }
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
      userManagementStore: UserManagementStore,
      identityProviderExists: IdentityProviderExists,
      maxPartiesPageSize: PositiveInt,
      maxSelfAllocatedParties: NonNegativeInt,
      partyRecordStore: PartyRecordStore,
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
      userManagementStore,
      identityProviderExists,
      maxPartiesPageSize,
      maxSelfAllocatedParties,
      partyRecordStore,
      writeBackend,
      managementServiceTimeout,
      submissionIdGenerator,
      telemetry,
      partyAllocationTracker,
      loggerFactory,
    )

  def decodePartyFromPageToken(pageToken: String)(implicit
      loggingContext: ErrorLoggingContext
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
      errorLogger: ErrorLoggingContext
  ): StatusRuntimeException = {
    errorLogger.info(s"Invalid page token: $details")
    RequestValidationErrors.InvalidArgument
      .Reject("Invalid page token")
      .asGrpcError
  }

  def encodeNextPageToken(token: Option[Ref.Party]): String =
    token
      .map { id =>
        val bytes = Base64.getUrlEncoder.encode(
          ListPartiesPageTokenPayload(partyIdLowerBoundExcl = id).toByteArray
        )
        new String(bytes, StandardCharsets.UTF_8)
      }
      .getOrElse("")

  trait CreateSubmissionId {
    def apply(
        partyId: LfPartyId,
        authorizationLevel: AuthorizationLevel,
    ): PartyAllocation.TrackerKey
  }

  object CreateSubmissionId {
    import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel

    def forParticipant(participantId: Ref.ParticipantId) = new CreateSubmissionId() {
      override def apply(
          partyId: LfPartyId,
          authorizationLevel: AuthorizationLevel,
      ): PartyAllocation.TrackerKey =
        PartyAllocation.TrackerKey(
          partyId,
          participantId,
          AuthorizationEvent.Added(authorizationLevel),
        )
    }
  }
}
