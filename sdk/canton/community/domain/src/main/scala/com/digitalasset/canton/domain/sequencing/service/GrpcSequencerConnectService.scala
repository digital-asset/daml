// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.api.v30.SequencerConnect
import com.digitalasset.canton.domain.api.v30.SequencerConnect.GetDomainParametersResponse.Parameters
import com.digitalasset.canton.domain.api.v30.SequencerConnect.{
  GetDomainIdRequest,
  GetDomainIdResponse,
  GetDomainParametersRequest,
  GetDomainParametersResponse,
}
import com.digitalasset.canton.domain.api.v30 as proto
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.topology.{DomainId, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Sequencer connect service on gRPC
  */
class GrpcSequencerConnectService(
    domainId: DomainId,
    sequencerId: SequencerId,
    staticDomainParameters: StaticDomainParameters,
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
