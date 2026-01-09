// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, SequencerApiClientConfig}
import com.digitalasset.canton.console.{LocalInstanceReference, LocalSequencerReference}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.OnShutdownRunner.PureOnShutdownRunner
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{
  ClientChannelBuilder,
  GrpcClient,
  GrpcManagedChannel,
}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import io.grpc.*

import java.util.concurrent.Executor
import scala.concurrent.{ExecutionContext, Future}

object SequencerTestHelper {

  def createChannel(
      clientConfig: SequencerApiClientConfig,
      loggerFactory: NamedLoggerFactory,
      executor: Executor,
  ): ManagedChannel = {
    // Create the gRPC channel for the synchronizer da sequencer/public API
    val channelBuilder = ClientChannelBuilder(loggerFactory)
    val daEndpoint = Endpoint(clientConfig.address, clientConfig.port)

    channelBuilder
      .create(
        NonEmpty(Seq, daEndpoint),
        useTls = false,
        executor,
        traceContextPropagation = Propagation.Enabled,
      )
      .build()
  }

  def createChannel(
      sequencer: LocalSequencerReference,
      loggerFactory: NamedLoggerFactory,
      executor: Executor,
  ): ManagedChannel =
    createChannel(sequencer.config.publicApi.clientConfig, loggerFactory, executor)

  def closeChannel(channel: ManagedChannel, logger: TracedLogger, name: String): Unit =
    if (channel != null) {
      LifeCycle.close(LifeCycle.toCloseableChannel(channel, logger, name))(logger)
    }

  def requestToken(
      channel: ManagedChannel,
      synchronizerId: PhysicalSynchronizerId,
      memberId: Member,
      crypto: SynchronizerCrypto,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationToken] = {
    val config = AuthenticationTokenManagerConfig(retries =
      NonNegativeInt.zero
    ) // Disable retries to speedup negative tests.

    val tokenProvider = new AuthenticationTokenProvider(
      synchronizerId,
      memberId,
      crypto,
      // enabled dev-support as otherwise nightly dev-pv-test fails
      ProtocolVersionCompatibility.supportedProtocols(
        includeAlphaVersions = protocolVersion.isAlpha,
        includeBetaVersions = protocolVersion.isBeta,
        release = ReleaseVersion.current,
      ),
      config,
      metricsO = None,
      metricsContext = MetricsContext.Empty,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    val logger = loggerFactory.getTracedLogger(this.getClass)
    val grpcChannel = GrpcManagedChannel(
      "sequencer-authentication-channel",
      channel,
      new PureOnShutdownRunner(logger),
      logger,
    )

    tokenProvider
      .generateToken(
        Endpoint("test", Port.tryCreate(1234)),
        GrpcClient.create(grpcChannel, new SequencerAuthenticationServiceStub(_)),
      )
      .map(_.token)
  }

  def mkCallCredentials(
      synchronizerId: PhysicalSynchronizerId,
      memberId: Member,
      token: AuthenticationToken,
  ): CallCredentials = new CallCredentials {
    override def applyRequestMetadata(
        requestInfo: CallCredentials.RequestInfo,
        appExecutor: Executor,
        applier: CallCredentials.MetadataApplier,
    ): Unit = {
      val metadata = new Metadata()
      metadata.put(Constant.MEMBER_ID_METADATA_KEY, memberId.toProtoPrimitive)
      metadata.put(Constant.AUTH_TOKEN_METADATA_KEY, token)
      metadata.put(Constant.SYNCHRONIZER_ID_METADATA_KEY, synchronizerId.toProtoPrimitive)
      applier.apply(metadata)
    }

    override def thisUsesUnstableApi(): Unit = {}
  }

  def mkSubmissionRequest(sender: Member): SubmissionRequest =
    SubmissionRequest.tryCreate(
      sender,
      MessageId.randomMessageId(),
      Batch.empty(BaseTest.testedProtocolVersion),
      CantonTimestamp.MaxValue,
      topologyTimestamp = None,
      aggregationRule = None,
      submissionCost = None,
      BaseTest.testedProtocolVersion,
    )

  def sendSubmissionRequest(
      sequencerServiceStub: SequencerServiceStub,
      sender: Member,
  )(implicit ec: ExecutionContext): Future[Unit] =
    sendSubmissionRequest(sequencerServiceStub, mkSubmissionRequest(sender))

  def signSubmissionRequest(
      request: SubmissionRequest,
      participant: LocalInstanceReference,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Signature] = {
    import org.scalatest.EitherValues.*
    import org.scalatest.OptionValues.*
    val cryptoPrivateStore = participant.crypto.cryptoPrivateStore.toExtended.getOrElse(
      throw new RuntimeException(s"Crypto private store does not implement all necessary methods")
    )
    for {
      keys <- cryptoPrivateStore
        .listPrivateKeys(KeyPurpose.Signing, encrypted = false)
        .value
      someKey = keys.value.lastOption.value.id
      hash = participant.crypto.pureCrypto
        .digest(HashPurpose.SubmissionRequestSignature, request.getCryptographicEvidence)
      signatureE <- participant.crypto.privateCrypto
        .sign(hash, someKey, SigningKeyUsage.ProtocolOnly)
        .value
    } yield signatureE.value
  }

  def sendSubmissionRequest(
      sequencerServiceStub: SequencerServiceStub,
      submissionRequest: SubmissionRequest,
      signature: Signature = Signature.noSignature,
  )(implicit ec: ExecutionContext): Future[Unit] = {

    val signedContent = SignedContent(
      submissionRequest,
      signature,
      None,
      BaseTest.testedProtocolVersion,
    )
    val response = sequencerServiceStub
      .sendAsync(v30.SendAsyncRequest(signedContent.toByteString))

    response.map(_ => ())
  }
}
