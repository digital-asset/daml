// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.stream.{BoundedSourceQueue, Materializer}
import akka.stream.scaladsl.RunnableGraph
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

class BoundedSourceQueueResourceOwner[T, Context: HasExecutionContext](
    queueGraph: RunnableGraph[BoundedSourceQueue[T]]
)(implicit
    materializer: Materializer
) extends AbstractResourceOwner[Context, BoundedSourceQueue[T]] {
  override def acquire()(implicit context: Context): Resource[Context, BoundedSourceQueue[T]] =
    ReleasableResource(Future(queueGraph.run())) { queue =>
      Future(queue.complete())
    }
}
