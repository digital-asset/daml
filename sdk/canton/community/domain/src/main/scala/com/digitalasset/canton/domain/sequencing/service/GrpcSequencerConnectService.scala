// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.api.v30.SequencerConnect
import com.digitalasset.canton.domain.api.v30.SequencerConnect.GetDomainParametersResponse.Parameters
import com.digitalasset.canton.domain.api.v30.SequencerConnect.{
  GetDomainIdRequest,
  GetDomainIdResponse,
  GetDomainParametersRequest,
  GetDomainParametersResponse,
  RegisterOnboardingTopologyTransactionsResponse,
}
import com.digitalasset.canton.domain.api.v30 as proto
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyMapping}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

/** Sequencer connect service on gRPC
  */
class GrpcSequencerConnectService(
    domainId: DomainId,
    sequencerId: SequencerId,
    staticDomainParameters: StaticDomainParameters,
    domainTopologyManager: DomainTopologyManager,
    cryptoApi: DomainSyncCryptoClient,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends proto.SequencerConnectServiceGrpc.SequencerConnectService
    with GrpcHandshakeService
    with NamedLogging {

  protected val serverProtocolVersion: ProtocolVersion = staticDomainParameters.protocolVersion

  def getDomainId(request: GetDomainIdRequest): Future[GetDomainIdResponse] =
    Future.successful(
      GetDomainIdResponse(
        domainId = domainId.toProtoPrimitive,
        sequencerId = sequencerId.toProtoPrimitive,
      )
    )

  def getDomainParameters(
      request: GetDomainParametersRequest
  ): Future[GetDomainParametersResponse] = {
    val response = staticDomainParameters.protoVersion.v match {
      case 30 => Future.successful(Parameters.ParametersV1(staticDomainParameters.toProtoV30))
      case unsupported =>
        Future.failed(
          new IllegalStateException(
            s"Unsupported Proto version $unsupported for static domain parameters"
          )
        )
    }

    response.map(GetDomainParametersResponse(_))
  }

  def verifyActive(
      request: proto.SequencerConnect.VerifyActiveRequest
  ): Future[proto.SequencerConnect.VerifyActiveResponse] = {
    import proto.SequencerConnect.VerifyActiveResponse
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val resultF = for {
      participant <- EitherT.fromEither[Future](getParticipantFromGrpcContext())
      isActive <- EitherT(
        cryptoApi.ips.currentSnapshotApproximation
          .isParticipantActive(participant)
          .map(_.asRight[String])
      )
    } yield VerifyActiveResponse.Success(isActive)

    resultF
      .fold[VerifyActiveResponse.Value](
        reason => VerifyActiveResponse.Value.Failure(VerifyActiveResponse.Failure(reason)),
        success =>
          VerifyActiveResponse.Value.Success(VerifyActiveResponse.Success(success.isActive)),
      )
      .map(VerifyActiveResponse(_))
  }

  override def handshake(
      request: SequencerConnect.HandshakeRequest
  ): Future[SequencerConnect.HandshakeResponse] =
    Future.successful {
      SequencerConnect.HandshakeResponse(
        Some(
          handshake(
            request.getHandshakeRequest.clientProtocolVersions,
            request.getHandshakeRequest.minimumProtocolVersion,
          )
        )
      )
    }

  override def registerOnboardingTopologyTransactions(
      request: SequencerConnect.RegisterOnboardingTopologyTransactionsRequest
  ): Future[SequencerConnect.RegisterOnboardingTopologyTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val resultET = for {
      member <- EitherT.fromEither[Future](
        IdentityContextHelper.getCurrentStoredMember.toRight(
          invalidRequest("Unable to find member id in gRPC context")
        )
      )
      isKnown <- EitherT.right(
        cryptoApi.ips.headSnapshot.isMemberKnown(member)
      )
      _ <- EitherTUtil.condUnitET[Future](
        !isKnown,
        failedPrecondition(s"Member $member is already known on the domain"),
      )
      transactions <- CantonGrpcUtil.mapErrNew(
        request.topologyTransactions
          .traverse(
            SignedTopologyTransaction
              .fromProtoV30(ProtocolVersionValidation(serverProtocolVersion), _)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )

      _ <- checkForOnlyOnboardingTransactions(member, transactions)
      _ <- CantonGrpcUtil.mapErrNewETUS(
        domainTopologyManager
          .add(transactions, ForceFlags.all, expectFullAuthorization = false)
      )
    } yield RegisterOnboardingTopologyTransactionsResponse.defaultInstance

    EitherTUtil.toFuture(resultET)
  }

  private def checkForOnlyOnboardingTransactions(
      member: Member,
      transactions: Seq[GenericSignedTopologyTransaction],
  ): EitherT[Future, StatusRuntimeException, Unit] = {
    val unexpectedUids = transactions.filter(_.mapping.maybeUid.exists(_ != member.uid))
    val unexpectedNamespaces = transactions.filter(_.mapping.namespace != member.namespace)

    val expectedMappings =
      if (member.code == ParticipantId.Code) TopologyStore.initialParticipantDispatchingSet
      else Set(TopologyMapping.Code.NamespaceDelegation, TopologyMapping.Code.OwnerToKeyMapping)
    val submittedMappings = transactions.map(_.mapping.code).toSet
    val unexpectedMappings = submittedMappings -- expectedMappings
    val missingMappings = expectedMappings -- submittedMappings

    val unexpectedProposals = transactions.filter(_.isProposal)

    val resultET = for {
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedMappings.isEmpty,
        s"Unexpected topology mappings for onboarding $member: $unexpectedMappings",
      )
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedUids.isEmpty,
        s"Mappings for unexpected UIDs for onboarding $member: $unexpectedUids",
      )
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedNamespaces.isEmpty,
        s"Mappings for unexpected namespaces for onboarding $member: $unexpectedNamespaces",
      )
      _ <- EitherTUtil.condUnitET[Future](
        missingMappings.isEmpty,
        s"Missing mappings for onboarding $member: $missingMappings",
      )
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedProposals.isEmpty,
        s"Unexpected proposals for onboarding $member: $missingMappings",
      )
    } yield ()

    resultET.leftMap(invalidRequest)
  }

  private def invalidRequest(message: String): StatusRuntimeException =
    Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException()

  private def failedPrecondition(message: String): StatusRuntimeException =
    Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException()

  /*
   Note: we only get the participantId from the context; we have no idea
   whether the member is authenticated or not.
   */
  private def getParticipantFromGrpcContext(): Either[String, ParticipantId] =
    IdentityContextHelper.getCurrentStoredMember
      .toRight("Unable to find participant id in gRPC context")
      .flatMap {
        case participantId: ParticipantId => Right(participantId)
        case member => Left(s"Expecting participantId ; found $member")
      }
}
