// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.common.domain.SequencerConnectClient
import com.digitalasset.canton.common.domain.SequencerConnectClient.{
  DomainClientBootstrapInfo,
  Error,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.api.v30.SequencerConnect
import com.digitalasset.canton.domain.api.v30.SequencerConnect.GetDomainParametersResponse.Parameters
import com.digitalasset.canton.domain.api.v30.SequencerConnect.VerifyActiveRequest
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, ClientChannelBuilder}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{
  Member,
  ParticipantId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Success}
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, SynchronizerAlias}
import io.grpc.ClientInterceptors

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

class GrpcSequencerConnectClient(
    sequencerConnection: GrpcSequencerConnection,
    val timeouts: ProcessingTimeout,
    traceContextPropagation: TracingConfig.Propagation,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectClient
    with NamedLogging
    with FlagCloseable {

  private val clientChannelBuilder = ClientChannelBuilder(loggerFactory)
  private val builder =
    sequencerConnection.mkChannelBuilder(clientChannelBuilder, traceContextPropagation)

  override def getDomainClientBootstrapInfo(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, DomainClientBootstrapInfo] =
    for {
      _ <- CantonGrpcUtil
        .checkCantonApiInfo(
          synchronizerAlias.unwrap,
          CantonGrpcUtil.ApiName.SequencerPublicApi,
          builder,
          logger,
          timeouts.network,
          onShutdownRunner = this,
          None,
        )
        .leftMap(err => Error.Transport(err))
      response <- CantonGrpcUtil
        .sendSingleGrpcRequest(
          serverName = synchronizerAlias.unwrap,
          requestDescription = "get synchronizer id and sequencer id",
          channelBuilder = builder,
          stubFactory = v30.SequencerConnectServiceGrpc.stub,
          timeout = timeouts.network.unwrap,
          logger = logger,
          logPolicy = CantonGrpcUtil.SilentLogPolicy,
          onShutdownRunner = this,
          retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
          token = None,
        )(_.getSynchronizerId(v30.SequencerConnect.GetSynchronizerIdRequest()))
        .leftMap(err => Error.Transport(err.toString))

      synchronizerId = SynchronizerId
        .fromProtoPrimitive(response.synchronizerId, "synchronizerId")
        .leftMap[Error](err => Error.DeserializationFailure(err.toString))

      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](synchronizerId)

      sequencerId = UniqueIdentifier
        .fromProtoPrimitive(response.sequencerUid, "sequencerUid")
        .leftMap[Error](err => Error.DeserializationFailure(err.toString))
        .map(SequencerId(_))

      sequencerId <- EitherT.fromEither[FutureUnlessShutdown](sequencerId)
    } yield DomainClientBootstrapInfo(synchronizerId, sequencerId)

  override def getDomainParameters(
      synchronizerIdentifier: String
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, StaticDomainParameters] = for {
    responseP <- CantonGrpcUtil
      .sendSingleGrpcRequest(
        serverName = synchronizerIdentifier,
        requestDescription = "get domain parameters",
        channelBuilder = builder,
        stubFactory = v30.SequencerConnectServiceGrpc.stub,
        timeout = timeouts.network.unwrap,
        logger = logger,
        logPolicy = CantonGrpcUtil.SilentLogPolicy,
        onShutdownRunner = this,
        retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
        token = None,
      )(_.getDomainParameters(v30.SequencerConnect.GetDomainParametersRequest()))
      .leftMap(err => Error.Transport(err.toString))

    domainParametersE = GrpcSequencerConnectClient
      .toStaticDomainParameters(responseP)
      .leftMap[Error](err => Error.DeserializationFailure(err.toString))

    domainParameters <- EitherT.fromEither[FutureUnlessShutdown](domainParametersE)

  } yield domainParameters

  override def getSynchronizerId(
      synchronizerIdentifier: String
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Error, SynchronizerId] =
    for {
      responseP <- CantonGrpcUtil
        .sendSingleGrpcRequest(
          serverName = synchronizerIdentifier,
          requestDescription = "get synchronizer id",
          channelBuilder = builder,
          stubFactory = v30.SequencerConnectServiceGrpc.stub,
          timeout = timeouts.network.unwrap,
          logger = logger,
          logPolicy = CantonGrpcUtil.SilentLogPolicy,
          onShutdownRunner = this,
          retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
          token = None,
        )(_.getSynchronizerId(v30.SequencerConnect.GetSynchronizerIdRequest()))
        .leftMap(err => Error.Transport(err.toString))

      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(responseP.synchronizerId, "synchronizer_id")
          .leftMap[Error](err => Error.DeserializationFailure(err.toString))
      )
    } yield synchronizerId

  override def handshake(
      synchronizerAlias: SynchronizerAlias,
      request: HandshakeRequest,
      dontWarnOnDeprecatedPV: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, HandshakeResponse] =
    for {
      responseP <- CantonGrpcUtil
        .sendSingleGrpcRequest(
          serverName = synchronizerAlias.unwrap,
          requestDescription = "handshake",
          channelBuilder = builder,
          stubFactory = v30.SequencerConnectServiceGrpc.stub,
          timeout = timeouts.network.unwrap,
          logger = logger,
          logPolicy = CantonGrpcUtil.SilentLogPolicy,
          onShutdownRunner = this,
          retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
          token = None,
        )(_.handshake(request.toProtoV30))
        .leftMap(err => Error.Transport(err.toString))

      handshakeResponse <- EitherT.fromEither[FutureUnlessShutdown](
        HandshakeResponse
          .fromProtoV30(responseP)
          .leftMap[Error](err => Error.DeserializationFailure(err.toString))
      )
      _ = if (handshakeResponse.serverProtocolVersion.isDeprecated && !dontWarnOnDeprecatedPV)
        DeprecatedProtocolVersion.WarnSequencerClient(
          synchronizerAlias,
          handshakeResponse.serverProtocolVersion,
        )
    } yield handshakeResponse

  def isActive(
      participantId: ParticipantId,
      synchronizerAlias: SynchronizerAlias,
      waitForActive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Boolean] = {
    val interceptor = new SequencerConnectClientInterceptor(participantId, loggerFactory)

    // retry in case of failure. Also if waitForActive is true, retry if response is negative
    implicit val success: Success[Either[Error, Boolean]] =
      retry.Success {
        case Left(_) => false
        case Right(value) => value || !waitForActive
      }

    def verifyActive(): EitherT[FutureUnlessShutdown, Error, Boolean] =
      CantonGrpcUtil
        .sendSingleGrpcRequest(
          serverName = synchronizerAlias.unwrap,
          requestDescription = "verify active",
          channelBuilder = builder,
          stubFactory = channel =>
            v30.SequencerConnectServiceGrpc
              .stub(ClientInterceptors.intercept(channel, interceptor)),
          timeout = timeouts.network.unwrap,
          logger = logger,
          logPolicy = CantonGrpcUtil.SilentLogPolicy,
          onShutdownRunner = this,
          retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
          token = None,
        )(_.verifyActive(VerifyActiveRequest()))
        .leftMap(err => Error.Transport(err.toString))
        .subflatMap(handleVerifyActiveResponse)

    // The verify active check within the sequencer connect service uses the sequenced topology state.
    // The retry logic was previously used as the "auto approve identity registration strategy"
    // did not wait for the registration to complete before signalling success. While this has
    // been improved in the meantime, we've left this retry logic in the code in order to support
    // future, less synchronous implementations.
    val interval = 500.millis
    val maxRetries: Int = timeouts.verifyActive.retries(interval)
    EitherT(
      retry
        .Pause(logger, this, maxRetries, interval, "verify active")
        .unlessShutdown(verifyActive().value, AllExceptionRetryPolicy)
    )
  }

  override def registerOnboardingTopologyTransactions(
      synchronizerAlias: SynchronizerAlias,
      member: Member,
      topologyTransactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Error, Unit] = {
    val interceptor = new SequencerConnectClientInterceptor(member, loggerFactory)
    CantonGrpcUtil
      .sendSingleGrpcRequest(
        serverName = synchronizerAlias.unwrap,
        requestDescription = "register-onboarding-topology-transactions",
        channelBuilder = builder,
        stubFactory = channel =>
          v30.SequencerConnectServiceGrpc.stub(ClientInterceptors.intercept(channel, interceptor)),
        timeout = timeouts.network.unwrap,
        logger = logger,
        logPolicy = CantonGrpcUtil.SilentLogPolicy,
        onShutdownRunner = this,
        retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
        token = None,
      )(
        _.registerOnboardingTopologyTransactions(
          SequencerConnect.RegisterOnboardingTopologyTransactionsRequest(
            topologyTransactions.map(_.toProtoV30)
          )
        )
      )
      .bimap(err => Error.Transport(err.status.toString), _ => ())

  }
}

object GrpcSequencerConnectClient {
  private def toStaticDomainParameters(
      response: v30.SequencerConnect.GetDomainParametersResponse
  ): ParsingResult[StaticDomainParameters] = response.parameters match {
    case Parameters.Empty =>
      Left(ProtoDeserializationError.FieldNotSet("GetDomainParameters.parameters"))
    case Parameters.ParametersV1(parametersV1) => StaticDomainParameters.fromProtoV30(parametersV1)
  }
}
