// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.lifecycle.{LifeCycle, OnShutdownRunner}
import com.digitalasset.canton.logging.TracedLogger
import io.grpc.ManagedChannel

/** Bundles a gRPC managed channel together with the shutdown runner of the component the channel belongs to */
final case class GrpcManagedChannel(
    name: String,
    channel: ManagedChannel,
    associatedShutdownRunner: OnShutdownRunner,
    override protected val logger: TracedLogger,
) extends AutoCloseable
    with OnShutdownRunner {

  // TODO(#21278): Change this so that the channel is force-closed immediately
  override protected def onFirstClose(): Unit =
    LifeCycle.close(LifeCycle.toCloseableChannel(channel, logger, name))(logger)

  override def close(): Unit = super.close()

  def handle: GrpcManagedChannelHandle = new GrpcManagedChannelHandle(channel)
}

class GrpcManagedChannelHandle(private val channel: ManagedChannel) {
  override def toString: String = channel.toString
}
