// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableChannel
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencing.client.transports.{
  GrpcClientTransportHelpers,
  GrpcSequencerClientAuth,
}
import com.digitalasset.canton.tracing.TraceContext
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

  private val grpcStub: v30.SequencerChannelServiceGrpc.SequencerChannelServiceStub = clientAuth(
    new v30.SequencerChannelServiceGrpc.SequencerChannelServiceStub(
      channel,
      options = CallOptions.DEFAULT,
    )
  )

  /** Issue the GRPC request to connect to a sequencer channel.
    *
    * The call is made directly against the GRPC stub as trace context propagation and error handling
    * in the bidirectionally streaming request is performed by implementations of the
    * [[SequencerChannelProtocolProcessor]].
    */
  @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
  def connectToSequencerChannel(
      channelEndpoint: SequencerChannelClientEndpoint
  )(implicit traceContext: TraceContext): Unit = {
    val requestObserver =
      grpcStub.connectToSequencerChannel(channelEndpoint.observer)

    // Only now that the GRPC request has provided the "request" GRPC StreamObserver,
    // pass on the request observer to the channel endpoint. This enables the sequencer
    // channel endpoint to send messages via the sequencer channel.
    channelEndpoint.setRequestObserver(requestObserver)
  }

  /** Ping the sequencer channel service to check if the sequencer supports channels.
    */
  def ping()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val sendAtMostOnce = retryPolicy(retryOnUnavailable = false)
    val response = CantonGrpcUtil.sendGrpcRequest(grpcStub, "sequencer-channel")(
      stub => stub.ping(v30.PingRequest()),
      requestDescription = "ping",
      timeout = timeouts.network.duration,
      logger = logger,
      logPolicy = noLoggingShutdownErrorsLogPolicy,
      onShutdownRunner = this,
      retryPolicy = sendAtMostOnce,
    )
    response.bimap(_.toString, _ => ())

  }

  override protected def onClosed(): Unit =
    Lifecycle.close(
      clientAuth,
      new CloseableChannel(channel, logger, "grpc-sequencer-channel-transport"),
    )(logger)
}
