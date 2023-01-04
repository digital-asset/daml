// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

private[grpc] final class ServerResourceOwner[Context: HasExecutionContext](
    builder: ServerBuilder[_],
    shutdownTimeout: FiniteDuration,
) extends AbstractResourceOwner[Context, Server] {
  override def acquire()(implicit context: Context): Resource[Context, Server] =
    ReleasableResource(Future(builder.build().start())) { server =>
      Future {
        // Ask to shutdown gracefully, but wait for termination for the specified timeout.
        val done = server.shutdown().awaitTermination(shutdownTimeout.length, shutdownTimeout.unit)
        if (!done) {
          // If the server could not be shut down gracefully in time, ask to terminate immediately
          server.shutdownNow().awaitTermination()
        }
      }
    }
}
