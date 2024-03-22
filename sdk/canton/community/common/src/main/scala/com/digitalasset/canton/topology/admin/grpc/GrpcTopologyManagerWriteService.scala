// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, CryptoPublicStoreError}
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey, SigningPublicKey}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v0.DomainParametersChangeAuthorization.Parameters
import com.digitalasset.canton.topology.admin.v0.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError}

import scala.concurrent.{ExecutionContext, Future}

final class GrpcTopologyManagerWriteService[T <: CantonError](
    manager: TopologyManager[T],
    cryptoPublicStore: CryptoPublicStore,
    protocolVersion: ProtocolVersion,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TopologyManagerWriteServiceGrpc.TopologyManagerWriteService
    with NamedLogging {

  import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*

  private def process(
      authDataPO: Option[AuthorizationData],
      elementE: Either[CantonError, TopologyMapping],
  )(implicit traceContext: TraceContext): Future[AuthorizationSuccess] = {
    val authDataE = for {
      authDataP <- ProtoConverter.required("authorization", authDataPO)
      AuthorizationData(changeP, signedByP, replaceExistingP, forceChangeP) = authDataP
      change <- TopologyChangeOp.fromProtoV0(changeP)
      fingerprint <- (if (signedByP.isEmpty) None
                      else Some(Fingerprint.fromProtoPrimitive(signedByP))).sequence
    } yield (change, fingerprint, replaceExistingP, forceChangeP)

    val authorizationSuccess = for {
      authData <- EitherT.fromEither[FutureUnlessShutdown](
        authDataE.leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      element <- EitherT.fromEither[FutureUnlessShutdown](elementE)
      (op, fingerprint, replace, force) = authData
      tx <- manager
        .genTransaction(op, element, protocolVersion)
        .leftWiden[CantonError]
        .mapK(FutureUnlessShutdown.outcomeK)
      success <- manager
        .authorize(tx, fingerprint, protocolVersion, force = force, replaceExisting = replace)
        .leftWiden[CantonError]
    } yield new AuthorizationSuccess(success.getCryptographicEvidence)

    CantonGrpcUtil.mapErrNewEUS(authorizationSuccess)
  }

  override def authorizePartyToParticipant(
      request: PartyToParticipantAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val item = for {
      party <- UniqueIdentifier.fromProtoPrimitive(request.party, "party")
      side <- RequestSide.fromProtoEnum(request.side)
      participantId <- ParticipantId
        .fromProtoPrimitive(request.participant, "participant")
      permission <- ParticipantPermission
        .fromProtoEnum(request.permission)
    } yield PartyToParticipant(side, PartyId(party), participantId, permission)
    process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
  }

  override def authorizeOwnerToKeyMapping(
      request: OwnerToKeyMappingAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val itemEitherT: EitherT[Future, CantonError, OwnerToKeyMapping] = for {
      owner <- KeyOwner
        .fromProtoPrimitive(request.keyOwner, "keyOwner")
        .leftMap(ProtoDeserializationFailure.Wrap(_))
        .toEitherT[Future]
      keyId <- EitherT.fromEither[Future](
        Fingerprint
          .fromProtoPrimitive(request.fingerprintOfKey)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      key <- parseKeyInStoreResponse[PublicKey](keyId, cryptoPublicStore.publicKey(keyId))
    } yield OwnerToKeyMapping(owner, key)
    for {
      item <- itemEitherT.value
      result <- process(request.authorization, item)
    } yield result
  }

  private def parseKeyInStoreResponse[K <: PublicKey](
      fp: Fingerprint,
      result: EitherT[Future, CryptoPublicStoreError, Option[K]],
  )(implicit traceContext: TraceContext): EitherT[Future, CantonError, K] = {
    result
      .leftMap(TopologyManagerError.InternalError.CryptoPublicError(_))
      .subflatMap(_.toRight(TopologyManagerError.PublicKeyNotInStore.Failure(fp): CantonError))
  }

  private def getSigningPublicKey(
      fingerprint: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CantonError, SigningPublicKey] = {
    parseKeyInStoreResponse(fingerprint, cryptoPublicStore.signingKey(fingerprint))
  }

  private def parseFingerprint(
      proto: String
  )(implicit traceContext: TraceContext): EitherT[Future, CantonError, Fingerprint] =
    EitherT
      .fromEither[Future](Fingerprint.fromProtoPrimitive(proto))
      .leftMap(ProtoDeserializationFailure.Wrap(_))

  override def authorizeNamespaceDelegation(
      request: NamespaceDelegationAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val item = for {
      fp <- parseFingerprint(request.fingerprintOfAuthorizedKey)
      key <- getSigningPublicKey(fp)
      namespaceFingerprint <- parseFingerprint(request.namespace)
    } yield NamespaceDelegation(
      Namespace(namespaceFingerprint),
      key,
      request.isRootDelegation || request.namespace == request.fingerprintOfAuthorizedKey,
    )
    item.value.flatMap(itemE => process(request.authorization, itemE))
  }

  override def authorizeIdentifierDelegation(
      request: IdentifierDelegationAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val item = for {
      uid <- UniqueIdentifier
        .fromProtoPrimitive(request.identifier, "identifier")
        .leftMap(ProtoDeserializationFailure.Wrap(_))
        .toEitherT[Future]
      fp <- parseFingerprint(request.fingerprintOfAuthorizedKey)
      key <- getSigningPublicKey(fp)
    } yield IdentifierDelegation(uid, key)
    item.value.flatMap(itemE => process(request.authorization, itemE))
  }

  /** Adds a signed topology transaction to the Authorized store
    */
  override def addSignedTopologyTransaction(
      request: SignedTopologyTransactionAddition
  ): Future[AdditionSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val pvv = ProtocolVersionValidation.unless(manager.isAuthorizedStore)(protocolVersion)
    for {
      parsed <- mapErrNew(
        EitherT
          .fromEither[Future](
            SignedTopologyTransaction.fromByteString(pvv)(request.serialized)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      _ <- mapErrNewEUS(
        manager.add(parsed, force = true, replaceExisting = true, allowDuplicateMappings = true)
      )
    } yield AdditionSuccess()
  }

  override def authorizeParticipantDomainState(
      request: ParticipantDomainStateAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val item = for {
      side <- RequestSide.fromProtoEnum(request.side)
      domain <- DomainId.fromProtoPrimitive(request.domain, "domain")
      participant <- ParticipantId
        .fromProtoPrimitive(request.participant, "participant")
      permission <- ParticipantPermission.fromProtoEnum(request.permission)
      trustLevel <- TrustLevel.fromProtoEnum(request.trustLevel)
    } yield ParticipantState(side, domain, participant, permission, trustLevel)
    process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
  }

  override def authorizeMediatorDomainState(
      request: MediatorDomainStateAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val item = for {
      side <- RequestSide.fromProtoEnum(request.side)
      domain <- DomainId.fromProtoPrimitive(request.domain, "domain")
      uid <- UniqueIdentifier
        .fromProtoPrimitive(request.mediator, "mediator")
    } yield MediatorDomainState(side, domain, MediatorId(uid))
    process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
  }

  /** Authorizes a new package vetting transaction */
  override def authorizeVettedPackages(
      request: VettedPackagesAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val item = for {
      uid <- UniqueIdentifier
        .fromProtoPrimitive(request.participant, "participant")
        .leftMap(ProtoDeserializationFailure.Wrap(_))
      packageIds <- request.packageIds
        .traverse(LfPackageId.fromString)
        .leftMap(err =>
          ProtoDeserializationFailure.Wrap(
            ProtoDeserializationError.ValueConversionError("package_ids", err)
          )
        )
    } yield VettedPackages(ParticipantId(uid), packageIds)
    process(request.authorization, item)
  }

  /** Authorizes a new domain parameters change transaction */
  override def authorizeDomainParametersChange(
      request: DomainParametersChangeAuthorization
  ): Future[AuthorizationSuccess] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val item = for {
      uid <- UniqueIdentifier
        .fromProtoPrimitive(request.domain, "domain")

      domainParameters <- request.parameters match {
        case Parameters.Empty => Left(ProtoDeserializationError.FieldNotSet("domainParameters"))
        case Parameters.ParametersV0(parametersV0) =>
          DynamicDomainParameters.fromProtoV0(parametersV0)
        case Parameters.ParametersV1(parametersV1) =>
          DynamicDomainParameters.fromProtoV1(parametersV1)
        case Parameters.ParametersV2(parametersV2) =>
          DynamicDomainParameters.fromProtoV2(parametersV2)
      }

    } yield DomainParametersChange(DomainId(uid), domainParameters)

    process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
  }

}
