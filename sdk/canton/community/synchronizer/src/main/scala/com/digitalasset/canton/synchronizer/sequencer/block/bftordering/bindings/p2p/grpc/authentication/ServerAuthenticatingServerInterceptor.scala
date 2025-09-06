// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication

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
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder.createChannelBuilder
import com.digitalasset.canton.networking.grpc.GrpcManagedChannel
import com.digitalasset.canton.sequencing.authentication.grpc.SequencerClientTokenAuthentication
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth.ChannelTokenFetcher
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.ServerAuthenticatingServerInterceptor.ServerAuthenticatingSimpleForwardingServerCall
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.*
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.util.Try

private[bftordering] class ServerAuthenticatingServerInterceptor(
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
        logger.debug(s"Found endpoint header and adding it to the gRPC server context: $endpoint")
        implicit val executor: Executor = (command: Runnable) => ec.execute(command)
        val authenticationServiceChannel = GrpcManagedChannel(
          s"server-authenticationServiceChannel-$endpoint",
          createChannelBuilder(endpoint.endpointConfig).build(),
          this,
          loggerFactory.getTracedLogger(getClass),
        )
        // Add the endpoint info sent by the client (`externalAddress`) to the context, so we can use it to find
        //  a potentially existing outgoing connection rather than accepting the incoming one;
        //  if not found, we'll accept the incoming one, associate it with the endpoint
        //  and won't create an outgoing one when we need to send.
        val contextWithEndpoint =
          Context
            .current()
            .withValue(
              ServerAuthenticatingServerInterceptor.peerEndpointContextKey,
              Some(endpoint),
            )
        logger.debug("Intercepting incoming P2P call: performing P2P server authentication")
        Contexts.interceptCall(
          contextWithEndpoint,
          new ServerAuthenticatingSimpleForwardingServerCall(
            call,
            tokenProvider,
            endpoint,
            authenticationServiceChannel,
            synchronizerId,
            member,
            timeouts,
            loggerFactory,
          ),
          requestHeaders,
          next,
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

object ServerAuthenticatingServerInterceptor {

  val peerEndpointContextKey: Context.Key[Option[P2PEndpoint]] =
    Context
      .keyWithDefault[Option[P2PEndpoint]]("bft-orderer-p2p-peer-endpoint", None)

  private class ServerAuthenticatingSimpleForwardingServerCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      tokenProvider: AuthenticationTokenProvider,
      p2pEndpoint: P2PEndpoint,
      authenticationServiceChannel: GrpcManagedChannel,
      synchronizerId: PhysicalSynchronizerId,
      member: Member,
      timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext)
      extends SimpleForwardingServerCall[ReqT, RespT](call)
      with NamedLogging {

    override def sendHeaders(responseHeaders: Metadata): Unit = {
      implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

      val tokenFetcher =
        new ChannelTokenFetcher(
          tokenProvider,
          Endpoint(p2pEndpoint.address, p2pEndpoint.port),
          authenticationServiceChannel,
        )

      logger.debug("Retrieving token to authenticate P2P server")

      Try(
        timeouts.network
          .awaitUS("tokenFetcher")( // Unfortunately, headers must be set synchronously
            tokenFetcher.apply
              .fold(
                error => logger.warn(s"Failed to fetch P2P server authentication token: $error"),
                { tokenWithExpiry =>
                  logger.debug("Adding P2P server authentication token to response headers")
                  SequencerClientTokenAuthentication
                    .authenticationMetadata(
                      synchronizerId,
                      member,
                      tokenWithExpiry.token,
                      into = responseHeaders,
                    )
                    .discard
                },
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
