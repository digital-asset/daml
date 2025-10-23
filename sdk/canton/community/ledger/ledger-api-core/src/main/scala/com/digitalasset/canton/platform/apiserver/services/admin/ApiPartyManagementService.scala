// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.auth.AuthorizationChecksErrors
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.v30.{SigningKeyScheme, SigningKeyUsage}
import com.digitalasset.canton.crypto.{Signature, SigningPublicKey, v30}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.ledger.api.validation.ValueValidator.requirePresence
import com.digitalasset.canton.ledger.api.validation.{CryptoValidator, ValidationErrors}
import com.digitalasset.canton.ledger.api.{IdentityProviderId, ObjectMeta, PartyDetails}
import com.digitalasset.canton.ledger.error.CommonErrors
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
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.platform.apiserver.services.tracking.StreamTracker
import com.digitalasset.canton.platform.apiserver.update
import com.digitalasset.canton.platform.apiserver.update.PartyRecordUpdateMapper
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
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
    identityProviderExists: IdentityProviderExists,
    maxPartiesPageSize: PositiveInt,
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
        ErrorLoggingContext(logger, loggingContext.toPropertiesMap, loggingContext.traceContext)
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
        } yield (partyIdHintO, annotations, identityProviderId)
      } { case (partyIdHintO, annotations, identityProviderId) =>
        val partyName = partyIdHintO.getOrElse(generatePartyName)
        val trackerKey = submissionIdGenerator(partyName, AuthorizationLevel.Submission)
        withEnrichedLoggingContext(telemetry)(logging.submissionId(trackerKey.submissionId)) {
          implicit loggingContext =>
            for {
              _ <- identityProviderExistsOrError(identityProviderId)
              allocated <- partyAllocationTracker
                .track(
                  trackerKey,
                  NonNegativeFiniteDuration(managementServiceTimeout),
                ) { _ =>
                  for {
                    result <- syncService.allocateParty(
                      partyName,
                      trackerKey.submissionId,
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
    val submissionId = submissionIdGenerator(
      request.partyDetails.fold("")(_.party),
      AuthorizationLevel.Submission,
    ).submissionId
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
        synchronizerId <- requireSynchronizerId(request.synchronizer, "synchronizer_id")
        protocolVersion <- syncService
          .protocolVersionForSynchronizerId(synchronizerId)
          .toRight(ValidationErrors.invalidArgument("No valid synchronizer found."))
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
        partyName <- requireParty(externalPartyDetails.partyHint)
      } yield (partyName, externalPartyDetails)
    } { case (partyName, externalPartyOnboardingDetails) =>
      val hostingParticipantsString = externalPartyOnboardingDetails.hostingParticipants
        .map { case HostingParticipant(participantId, permission) =>
          s"$participantId -> $permission"
        }
        .mkString("[", ", ", "]")
      val signingKeysString = externalPartyOnboardingDetails.signedPartyToKeyMappingTransaction
        .map { p2k =>
          s" and ${p2k.mapping.signingKeys.length} signing keys with threshold ${p2k.mapping.threshold.value}"
        }
        .getOrElse("")
      logger.info(
        s"Allocating external party ${externalPartyOnboardingDetails.partyId.toProtoPrimitive} on" +
          s" $hostingParticipantsString with confirmation threshold ${externalPartyOnboardingDetails.confirmationThreshold.value}" + signingKeysString
      )
      val trackerKey =
        submissionIdGenerator(
          partyName,
          authorizationLevel =
            if (externalPartyOnboardingDetails.isConfirming) AuthorizationLevel.Confirmation
            else Observation,
        )
      withEnrichedLoggingContext(telemetry)(logging.submissionId(trackerKey.submissionId)) {
        implicit loggingContext =>
          def allocateFn = for {
            result <- syncService.allocateParty(
              partyName,
              trackerKey.submissionId,
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
                .failOnShutdownTo(CommonErrors.ServerIsShuttingDown.Reject().asGrpcError)
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
      otherConfirmingParticipantIds,
      confirmationThreshold,
      observingParticipantIds,
    ) = request

    val participantId = syncService.participantId

    val availableConfirmers =
      (if (localParticipantObservationOnly) 0 else 1) + otherConfirmingParticipantIds.size

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
      confirmingPids <- otherConfirmingParticipantIds.toList
        .traverse(UniqueIdentifier.fromProtoPrimitive_)
        .leftMap(_.message)
      observingPids <- observingParticipantIds.toList
        .traverse(UniqueIdentifier.fromProtoPrimitive_)
        .leftMap(_.message)
      allParticipantIds =
        (confirmingPids ++ observingPids).map(ParticipantId(_)) :+ participantId
      _ <- Either.cond(
        allParticipantIds.distinct.sizeIs == allParticipantIds.size,
        (), {
          val duplicate =
            allParticipantIds.groupBy(identity).collect { case (x, ys) if ys.sizeIs > 1 => x }
          s"Duplicate participant ids $duplicate. They need to be unique and only in one category."
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
      nsd <- NamespaceDelegation.create(namespace, pubKey, CanSignAllMappings)
      p2k <- PartyToKeyMapping.create(
        party,
        threshold = PositiveInt.one,
        signingKeys = NonEmpty.mk(Seq, pubKey),
      )
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
      )
    } yield {
      val (_synchronizerId, protocolVersion) = synchronizerIdWithVersion
      val transactions =
        NonEmpty
          .mk(List, nsd, p2k, p2p)
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
      identityProviderExists: IdentityProviderExists,
      maxPartiesPageSize: PositiveInt,
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
      identityProviderExists,
      maxPartiesPageSize,
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
        partyIdHint: String,
        authorizationLevel: AuthorizationLevel,
    ): PartyAllocation.TrackerKey
  }

  object CreateSubmissionId {
    import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel

    def forParticipant(participantId: Ref.ParticipantId) = new CreateSubmissionId() {
      override def apply(
          partyIdHint: String,
          authorizationLevel: AuthorizationLevel,
      ): PartyAllocation.TrackerKey =
        PartyAllocation.TrackerKey.of(
          partyIdHint,
          participantId,
          AuthorizationEvent.Added(authorizationLevel),
        )
    }

    def fixedForTests(const: String) = new CreateSubmissionId() {
      override def apply(
          partyIdHint: String,
          authorizationLevel: AuthorizationLevel,
      ): PartyAllocation.TrackerKey =
        PartyAllocation.TrackerKey.forTests(Ref.SubmissionId.assertFromString(const))
    }

  }
}
