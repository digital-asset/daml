// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.domain.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableChannel
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.authentication.grpc.{
  SequencerClientNoAuthentication,
  SequencerClientTokenAuthentication,
}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{
  AuthenticatedMember,
  DomainId,
  Member,
  UnauthenticatedMemberId,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ManagedChannel
import io.grpc.stub.AbstractStub

import scala.concurrent.ExecutionContext

/** Auth helpers for the [[GrpcSequencerClientTransport]] when dealing with our custom authentication tokens. */
class GrpcSequencerClientAuth(
    domainId: DomainId,
    member: Member,
    crypto: Crypto,
    channelPerEndpoint: NonEmpty[Map[Endpoint, ManagedChannel]],
    supportedProtocolVersions: Seq[ProtocolVersion],
    tokenManagerConfig: AuthenticationTokenManagerConfig,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  private val tokenProvider =
    new AuthenticationTokenProvider(
      domainId,
      member,
      crypto,
      supportedProtocolVersions,
      tokenManagerConfig,
      timeouts,
      loggerFactory,
    )

  /** Wrap a grpc client with components to appropriately perform authentication */
  def apply[S <: AbstractStub[S]](client: S): S = {
    val obtainTokenPerEndpoint = channelPerEndpoint.transform { case (_, channel) =>
      val authenticationClient = new SequencerAuthenticationServiceStub(channel)
      (tc: TraceContext) =>
        TraceContextGrpc.withGrpcContext(tc) {
          tokenProvider.generateToken(authenticationClient)
        }
    }
    val clientAuthentication = member match {
      case unauthenticatedMember: UnauthenticatedMemberId =>
        new SequencerClientNoAuthentication(domainId, unauthenticatedMember)
      case authenticatedMember: AuthenticatedMember =>
        SequencerClientTokenAuthentication(
          domainId,
          authenticatedMember,
          obtainTokenPerEndpoint,
          tokenProvider.isClosing,
          tokenManagerConfig,
          clock,
          loggerFactory,
        )
    }
    clientAuthentication(client)
  }

  override protected def onClosed(): Unit = {
    Lifecycle.close(
      tokenProvider +:
        channelPerEndpoint.toList.map { case (endpoint, channel) =>
          new CloseableChannel(channel, logger, s"grpc-client-auth-$endpoint")
        }: _*
    )(logger)
  }
}
