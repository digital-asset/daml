// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.lifecycle.HasRunOnClosing
import io.grpc.ManagedChannel
import io.grpc.stub.AbstractStub

/** Bundles a service stub together with the channel it is using */
final class GrpcClient[Svc <: AbstractStub[Svc]] private (
    private[grpc] val channel: GrpcManagedChannel,
    val service: Svc,
) {
  def hasRunOnClosing: HasRunOnClosing = channel
}

object GrpcClient {
  def create[Svc <: AbstractStub[Svc]](
      channel: GrpcManagedChannel,
      serviceFactory: ManagedChannel => Svc,
  ): GrpcClient[Svc] =
    new GrpcClient(channel, serviceFactory(channel.channel))
}
