// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.pekko

import org.apache.pekko.stream.{BoundedSourceQueue, Materializer}
import org.apache.pekko.stream.scaladsl.RunnableGraph
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

final class BoundedSourceQueueResourceOwner[Mat, T, Context: HasExecutionContext](
    queueGraph: RunnableGraph[Mat],
    toSourceQueue: Mat => BoundedSourceQueue[T],
    toDone: Mat => Future[Unit],
)(implicit
    materializer: Materializer
) extends AbstractResourceOwner[Context, Mat] {
  override def acquire()(implicit context: Context): Resource[Context, Mat] =
    ReleasableResource(Future(queueGraph.run())) { value =>
      toSourceQueue(value).complete()
      toDone(value)
    }
}
