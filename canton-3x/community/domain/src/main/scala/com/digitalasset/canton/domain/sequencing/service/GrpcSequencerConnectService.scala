// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.api.v0.SequencerConnect.GetDomainParameters.Response.Parameters
import com.digitalasset.canton.domain.api.v0.SequencerConnect.{GetDomainId, GetDomainParameters}
import com.digitalasset.canton.domain.api.v0 as proto
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.protocol.VerifyActiveResponse
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

  def getDomainId(request: GetDomainId.Request): Future[GetDomainId.Response] =
    Future.successful(
      GetDomainId
        .Response(
          domainId = domainId.toProtoPrimitive,
          sequencerId = sequencerId.toProtoPrimitive,
        )
    )

  def getDomainParameters(
      request: GetDomainParameters.Request
  ): Future[GetDomainParameters.Response] = {
    val response = staticDomainParameters.protoVersion.v match {
      case 1 => Future.successful(Parameters.ParametersV1(staticDomainParameters.toProtoV1))
      case unsupported =>
        Future.failed(
          new IllegalStateException(
            s"Unsupported Proto version $unsupported for static domain parameters"
          )
        )
    }

    response.map(GetDomainParameters.Response(_))
  }

  def verifyActive(
      request: proto.SequencerConnect.VerifyActive.Request
  ): Future[proto.SequencerConnect.VerifyActive.Response] = {
    import proto.SequencerConnect.VerifyActive
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
      .fold[VerifyActive.Response.Value](
        reason => VerifyActive.Response.Value.Failure(VerifyActive.Failure(reason)),
        success => VerifyActive.Response.Value.Success(VerifyActive.Success(success.isActive)),
      )
      .map(VerifyActive.Response(_))
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
