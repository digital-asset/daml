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

/** GRPC operations for a client to interact with sequencer channels. */
final class SequencerChannelClientTransport(
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

  def ping()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val sendAtMostOnce = retryPolicy(retryOnUnavailable = false)
    val response =
      CantonGrpcUtil.sendGrpcRequest(grpcStub, "sequencer-channel")(
        stub => stub.ping(v30.PingRequest()),
        requestDescription = "ping",
        timeout = timeouts.network.duration,
        logger = logger,
        logPolicy = noLoggingShutdownErrorsLogPolicy,
        retryPolicy = sendAtMostOnce,
      )
    response
      .bimap(_.toString, _ => ())
      .mapK(FutureUnlessShutdown.outcomeK)

  }

  override protected def onClosed(): Unit =
    Lifecycle.close(
      clientAuth,
      new CloseableChannel(channel, logger, "grpc-sequencer-transport"),
    )(logger)
}
