// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{GrpcClient, GrpcManagedChannel}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub
import com.digitalasset.canton.sequencing.authentication.grpc.SequencerClientTokenAuthentication
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.AbstractStub
import io.grpc.{ManagedChannel, Status}

import scala.concurrent.ExecutionContext

/** Auth helpers for the [[GrpcSequencerClientTransport]] when dealing with our custom authentication tokens. */
class GrpcSequencerClientAuth(
    synchronizerId: SynchronizerId,
    member: Member,
    crypto: Crypto,
    channelPerEndpoint: NonEmpty[Map[Endpoint, ManagedChannel]],
    supportedProtocolVersions: Seq[ProtocolVersion],
    tokenManagerConfig: AuthenticationTokenManagerConfig,
    clock: Clock,
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
      timeouts,
      loggerFactory,
    )

  def logout()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit] =
    grpcChannelPerEndpoint.forgetNE.toSeq.parTraverse_ { case (_, channel) =>
      val authenticationClient =
        GrpcClient.create(channel, new SequencerAuthenticationServiceStub(_))
      tokenProvider.logout(authenticationClient)
    }

  /** Wrap a grpc client with components to appropriately perform authentication */
  def apply[S <: AbstractStub[S]](client: S): S = {
    val obtainTokenPerEndpoint = grpcChannelPerEndpoint.transform { (_, channel) =>
      val authenticationClient =
        GrpcClient.create(channel, new SequencerAuthenticationServiceStub(_))
      implicit tc: TraceContext => tokenProvider.generateToken(authenticationClient)
    }
    val clientAuthentication = SequencerClientTokenAuthentication(
      synchronizerId,
      member,
      obtainTokenPerEndpoint,
      tokenProvider.isClosing,
      tokenManagerConfig,
      clock,
      loggerFactory,
    )
    clientAuthentication(client)
  }

  override protected def onClosed(): Unit =
    LifeCycle.close(tokenProvider)(logger)
}
