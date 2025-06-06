// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.authentication

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder.createChannelBuilder
import com.digitalasset.canton.networking.grpc.GrpcManagedChannel
import com.digitalasset.canton.sequencing.authentication.grpc.SequencerClientTokenAuthentication
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth.ChannelTokenFetcher
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.authentication.ServerAuthenticatingServerFilter.ServerAuthenticatingSimpleForwardingServerCall
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, Status}

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.util.Try

private[bftordering] class ServerAuthenticatingServerFilter(
    synchronizerId: PhysicalSynchronizerId,
    member: Member,
    crypto: SynchronizerCrypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ServerInterceptor
    with NamedLogging
    with FlagCloseableAsync {

  private val tokenProvider =
    new AuthenticationTokenProvider(
      synchronizerId,
      member,
      crypto,
      supportedProtocolVersions,
      config,
      timeouts,
      loggerFactory,
    )

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      requestHeaders: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    Option(requestHeaders.get(AddEndpointHeaderClientInterceptor.ENDPOINT_METADATA_KEY)) match {
      case Some(endpoint) =>
        logger.debug(s"Found endpoint header: $endpoint")
        implicit val executor: Executor = (command: Runnable) => ec.execute(command)
        val authenticationServiceChannel = GrpcManagedChannel(
          s"server-authenticationServiceChannel-${endpoint.address}:${endpoint.port}",
          createChannelBuilder(endpoint.endpointConfig).build(),
          this,
          loggerFactory.getTracedLogger(getClass),
        )
        next.startCall(
          new ServerAuthenticatingSimpleForwardingServerCall(
            call,
            tokenProvider,
            authenticationServiceChannel,
            synchronizerId,
            member,
            timeouts,
            loggerFactory,
          ),
          requestHeaders,
        )
      case _ =>
        logger.error("No authenticated endpoint header found")
        call.close(Status.INTERNAL, new Metadata())
        new ServerCall.Listener[ReqT] {}
    }
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq(SyncCloseable("tokenProvider.close()", tokenProvider.close()))
}

object ServerAuthenticatingServerFilter {

  private class ServerAuthenticatingSimpleForwardingServerCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      tokenProvider: AuthenticationTokenProvider,
      authenticationServiceChannel: GrpcManagedChannel,
      synchronizerId: PhysicalSynchronizerId,
      member: Member,
      timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext)
      extends SimpleForwardingServerCall[ReqT, RespT](call)
      with NamedLogging {

    override def sendHeaders(responseHeaders: Metadata): Unit = {
      implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

      val tokenFetcher =
        new ChannelTokenFetcher(tokenProvider, authenticationServiceChannel)

      logger.debug("Retrieving token to authenticate P2P server")

      Try(
        timeouts.network
          .awaitUS("tokenFetcher")( // Unfortunately, headers must be set synchronously
            tokenFetcher.apply
              .fold(
                error => logger.warn(s"Failed to fetch P2P server authentication token: $error"),
                tokenWithExpiry =>
                  SequencerClientTokenAuthentication
                    .authenticationMetadata(
                      synchronizerId,
                      member,
                      tokenWithExpiry.token,
                      into = responseHeaders,
                    )
                    .discard,
              )
          ) match {
          case UnlessShutdown.AbortedDueToShutdown =>
            logger.info(
              "Aborted due to shutdown while trying to fetch P2P server authentication token"
            )
          case _ =>
            logger.debug(
              "Successfully retrieved token to authenticate P2P server and added authentication headers"
            )
        }
      ).fold(
        logger.warn("Timed out while trying to fetch P2P server authentication token", _),
        _ => (),
      )

      logger.debug("Closing the authentication service channel")
      authenticationServiceChannel.close()

      logger.debug("Sending server response headers")
      super.sendHeaders(responseHeaders)
    }
  }
}
