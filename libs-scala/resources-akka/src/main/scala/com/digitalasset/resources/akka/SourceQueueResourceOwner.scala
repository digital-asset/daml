// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.stream.Materializer
import akka.stream.scaladsl.{RunnableGraph, SourceQueue, SourceQueueWithComplete}
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

class SourceQueueResourceOwner[T, Context: HasExecutionContext](
    queueGraph: RunnableGraph[SourceQueueWithComplete[T]]
)(implicit
    materializer: Materializer
) extends AbstractResourceOwner[Context, SourceQueue[T]] {
  override def acquire()(implicit context: Context): Resource[Context, SourceQueue[T]] =
    ReleasableResource(Future(queueGraph.run())) { queue =>
      queue.complete()
      queue.watchCompletion().map(_ => ())
    }
}
