// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.SequencerConnectClient.{
  DomainClientBootstrapInfo,
  Error,
}
import com.digitalasset.canton.common.domain.grpc.GrpcSequencerConnectClient
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnection}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}

import scala.concurrent.{ExecutionContextExecutor, Future}

trait SequencerConnectClient extends NamedLogging with AutoCloseable {

  def getDomainClientBootstrapInfo(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, DomainClientBootstrapInfo]

  def getDomainParameters(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, StaticDomainParameters]

  def handshake(
      domainAlias: DomainAlias,
      request: HandshakeRequest,
      dontWarnOnDeprecatedPV: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, HandshakeResponse]

  def isActive(participantId: ParticipantId, waitForActive: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Boolean]

  def getAgreement(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Option[ServiceAgreement]]

  protected def handleVerifyActiveResponse(
      response: v0.SequencerConnect.VerifyActive.Response
  ): Either[Error, Boolean] = response.value match {
    case v0.SequencerConnect.VerifyActive.Response.Value.Success(success) => Right(success.isActive)
    case v0.SequencerConnect.VerifyActive.Response.Value.Failure(failure) =>
      Left(Error.DeserializationFailure(failure.reason))
    case v0.SequencerConnect.VerifyActive.Response.Value.Empty =>
      Left(Error.InvalidResponse("Missing response from VerifyActive"))
  }
}

object SequencerConnectClient {

  type Builder =
    SequencerConnection => EitherT[Future, Error, SequencerConnectClient]

  sealed trait Error {
    def message: String
  }
  object Error {
    final case class DeserializationFailure(err: String) extends Error {
      def message: String = s"Unable to deserialize proto: $err"
    }
    final case class InvalidState(message: String) extends Error
    final case class InvalidResponse(message: String) extends Error
    final case class Transport(message: String) extends Error
  }

  def apply(
      sequencerConnection: SequencerConnection,
      timeouts: ProcessingTimeout,
      traceContextPropagation: TracingConfig.Propagation,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextExecutor
  ): EitherT[Future, Error, SequencerConnectClient] = {
    for {
      client <- sequencerConnection match {
        case connection: GrpcSequencerConnection =>
          EitherT.rightT[Future, Error](
            new GrpcSequencerConnectClient(
              connection,
              timeouts,
              traceContextPropagation,
              loggerFactory,
            )
          )
      }
    } yield client
  }

  final case class DomainClientBootstrapInfo(domainId: DomainId, sequencerId: SequencerId)
}
