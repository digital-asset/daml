// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{GrpcClient, GrpcManagedChannel}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub
import com.digitalasset.canton.sequencing.authentication.grpc.{
  AuthenticationTokenWithExpiry,
  SequencerClientTokenAuthentication,
}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth.ChannelTokenFetcher
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.AbstractStub
import io.grpc.{ManagedChannel, Status}

import scala.concurrent.ExecutionContext

/** Auth helpers for the [[GrpcSequencerClientTransport]] when dealing with our custom
  * authentication tokens.
  */
class GrpcSequencerClientAuth(
    synchronizerId: PhysicalSynchronizerId,
    member: Member,
    crypto: SynchronizerCrypto,
    channelPerEndpoint: NonEmpty[Map[Endpoint, ManagedChannel]],
    supportedProtocolVersions: Seq[ProtocolVersion],
    tokenManagerConfig: AuthenticationTokenManagerConfig,
    clock: Clock,
    metricsO: Option[SequencerConnectionPoolMetrics],
    metricsContext: MetricsContext,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  private val grpcChannelPerEndpoint: NonEmpty[Map[Endpoint, GrpcManagedChannel]] =
    channelPerEndpoint.transform { (endpoint, channel) =>
      GrpcManagedChannel(
        s"grpc-client-auth-$endpoint",
        channel,
        this,
        logger,
      )
    }

  private val tokenProvider =
    new AuthenticationTokenProvider(
      synchronizerId,
      member,
      crypto,
      supportedProtocolVersions,
      tokenManagerConfig,
      metricsO,
      metricsContext,
      timeouts,
      loggerFactory,
    )

  private val obtainTokenPerEndpoint: NonEmpty[Map[Endpoint, ChannelTokenFetcher]] =
    grpcChannelPerEndpoint.transform { (endpoint, channel) =>
      new ChannelTokenFetcher(tokenProvider, endpoint, channel)
    }

  private val clientAuthentication =
    SequencerClientTokenAuthentication(
      synchronizerId,
      member,
      obtainTokenPerEndpoint,
      tokenProvider.isClosing,
      tokenManagerConfig,
      clock,
      loggerFactory,
    )

  def logout()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit] =
    grpcChannelPerEndpoint.forgetNE.toSeq.parTraverse_ { case (endpoint, channel) =>
      val authenticationClient =
        GrpcClient.create(channel, new SequencerAuthenticationServiceStub(_))
      tokenProvider.logout(endpoint, authenticationClient)
    }

  /** Wrap a grpc client with components to appropriately perform authentication */
  def apply[S <: AbstractStub[S]](client: S): S =
    clientAuthentication(client)

  override protected def onClosed(): Unit =
    LifeCycle.close(tokenProvider)(logger)
}

object GrpcSequencerClientAuth {

  trait TokenFetcher {
    def apply(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry]
  }

  final class ChannelTokenFetcher(
      tokenProvider: AuthenticationTokenProvider,
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  ) extends TokenFetcher {

    override def apply(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] = {
      val authenticationClient =
        GrpcClient.create(channel, new SequencerAuthenticationServiceStub(_))
      tokenProvider.generateToken(endpoint, authenticationClient)
    }
  }
}
