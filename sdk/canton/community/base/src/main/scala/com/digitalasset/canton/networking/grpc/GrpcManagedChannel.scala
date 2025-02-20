// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.lifecycle.LifeCycle.FastCloseableChannel
import com.digitalasset.canton.lifecycle.{LifeCycle, OnShutdownRunner, RunOnShutdown}
import com.digitalasset.canton.logging.TracedLogger
import io.grpc.ManagedChannel

/** Bundles a gRPC managed channel together with the shutdown runner of the component the channel
  * belongs to
  */
final case class GrpcManagedChannel(
    name: String,
    channel: ManagedChannel,
    associatedShutdownRunner: OnShutdownRunner,
    logger: TracedLogger,
) extends AutoCloseable
    with OnShutdownRunner {

  locally {
    // Immediately force-close this channel when the associated shutdown runner starts to be closed
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
    associatedShutdownRunner.runOnShutdown_(
      new RunOnShutdown() {
        override val name = s"GrpcManagedChannel-${GrpcManagedChannel.this.name}-shutdown"
        override def done: Boolean = isClosing
        override def run(): Unit = close()
      }
    )
  }

  override protected def onFirstClose(): Unit =
    LifeCycle.close(new FastCloseableChannel(channel, logger, name))(logger)

  override def close(): Unit = super.close()

  def handle: GrpcManagedChannelHandle = new GrpcManagedChannelHandle(channel)
}

class GrpcManagedChannelHandle(private val channel: ManagedChannel) {
  override def toString: String = channel.toString
}
