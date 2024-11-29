// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  OnShutdownRunner,
  RunOnShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcClient, GrpcManagedChannel}
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint
import com.digitalasset.canton.sequencing.client.transports.{
  GrpcClientTransportHelpers,
  GrpcSequencerClientAuth,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Context.CancellableContext
import io.grpc.{CallOptions, ManagedChannel}

import scala.concurrent.ExecutionContext

/** GRPC operations for a client to create and close sequencer channels.
  */
private[channel] final class SequencerChannelClientTransport(
    channel: ManagedChannel,
    clientAuth: GrpcSequencerClientAuth,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends GrpcClientTransportHelpers
    with FlagCloseable
    with NamedLogging {

  private val managedChannel: GrpcManagedChannel =
    GrpcManagedChannel("grpc-sequencer-channel-transport", channel, this, logger)

  private val grpcClient: GrpcClient[v30.SequencerChannelServiceGrpc.SequencerChannelServiceStub] =
    GrpcClient.create(
      managedChannel,
      channel =>
        clientAuth(
          new v30.SequencerChannelServiceGrpc.SequencerChannelServiceStub(
            channel,
            options = CallOptions.DEFAULT,
          )
        ),
    )

  locally {
    // Make sure that the authentication channels are closed before the transport channel
    // Otherwise the forced shutdownNow() on the transport channel will time out if the authentication interceptor
    // is in a retry loop to refresh the token
    import TraceContext.Implicits.Empty.*
    managedChannel.runOnShutdown_(new RunOnShutdown() {
      override def name: String = "grpc-sequencer-transport-shutdown-auth"
      override def done: Boolean = clientAuth.isClosing
      override def run(): Unit = clientAuth.close()
    })
  }

  /** Issue the GRPC request to connect to a sequencer channel.
    */
  def connectToSequencerChannel(
      channelEndpointFactory: (
          CancellableContext,
          OnShutdownRunner,
      ) => Either[String, SequencerChannelClientEndpoint]
  )(implicit traceContext: TraceContext): Either[String, SequencerChannelClientEndpoint] =
    CantonGrpcUtil
      .bidirectionalStreamingRequest(grpcClient, channelEndpointFactory)(_.observer)(
        _.connectToSequencerChannel(_)
      )
      .map { case (channelEndpoint, requestObserver) =>
        // Only now that the GRPC request has provided the "request" gRPC StreamObserver,
        // pass on the request observer to the channel endpoint. This enables the sequencer
        // channel endpoint to send messages via the sequencer channel.
        channelEndpoint.setRequestObserver(requestObserver)
        channelEndpoint
      }

  /** Ping the sequencer channel service to check if the sequencer supports channels.
    */
  def ping()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val sendAtMostOnce = retryPolicy(retryOnUnavailable = false)
    val response = CantonGrpcUtil.sendGrpcRequest(grpcClient, "sequencer-channel")(
      _.ping(v30.PingRequest()),
      requestDescription = "ping",
      timeout = timeouts.network.duration,
      logger = logger,
      retryPolicy = sendAtMostOnce,
    )
    response.bimap(_.toString, _ => ())

  }
}
