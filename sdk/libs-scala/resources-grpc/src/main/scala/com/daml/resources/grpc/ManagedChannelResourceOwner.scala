// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

private[grpc] final class ManagedChannelResourceOwner[Context: HasExecutionContext](
    builder: ManagedChannelBuilder[_],
    shutdownTimeout: FiniteDuration,
) extends AbstractResourceOwner[Context, ManagedChannel] {
  override def acquire()(implicit context: Context): Resource[Context, ManagedChannel] =
    ReleasableResource(Future(builder.build())) { channel =>
      Future {
        channel.shutdown()
        channel.awaitTermination(shutdownTimeout.length, shutdownTimeout.unit)
        ()
      }
    }
}
