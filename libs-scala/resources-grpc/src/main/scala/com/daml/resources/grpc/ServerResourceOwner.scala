// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.grpc

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, Resource}
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

private[grpc] final class ServerResourceOwner[Context: HasExecutionContext](
    builder: ServerBuilder[_],
    shutdownTimeout: FiniteDuration,
) extends AbstractResourceOwner[Context, Server] {
  override def acquire()(implicit context: Context): Resource[Context, Server] =
    Resource[Context].apply(Future(builder.build().start())) { server =>
      Future {
        server.shutdown()
        server.awaitTermination(shutdownTimeout.length, shutdownTimeout.unit)
        ()
      }
    }
}
