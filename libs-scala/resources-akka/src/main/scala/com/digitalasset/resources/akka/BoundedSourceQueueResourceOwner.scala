// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.stream.{BoundedSourceQueue, Materializer}
import akka.stream.scaladsl.RunnableGraph
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

class BoundedSourceQueueResourceOwner[T, E, Context: HasExecutionContext](
    queueGraph: RunnableGraph[T],
    toSourceQueue: T => BoundedSourceQueue[E],
    toDone: T => Future[Unit],
)(implicit
    materializer: Materializer
) extends AbstractResourceOwner[Context, T] {
  override def acquire()(implicit context: Context): Resource[Context, T] =
    ReleasableResource(Future(queueGraph.run())) { value =>
      toSourceQueue(value).complete()
      toDone(value)
    }
}
