// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.utils

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait ConcurrencyLimiter {
  def execute[T](task: () => Future[T]): Future[T]
}

class NoConcurrencyLimiter extends ConcurrencyLimiter {
  override def execute[T](task: () => Future[T]): Future[T] = task()
}

class QueueBasedConcurrencyLimiter(
    parallelism: Int,
    executionContext: ExecutionContext,
) extends ConcurrencyLimiter {
  assert(parallelism > 0)

  type Task = () => Unit
  private val waiting = mutable.Queue[Task]()
  private var running: Int = 0

  override def execute[T](task: () => Future[T]): Future[T] = synchronized {
    val result = enqueueTask(task)
    startTasks()
    result
  }

  private def enqueueTask[T](task: () => Future[T]): Future[T] = synchronized {
    val promise = Promise[T]()

    // Note: tasks are started sequentially (see startTasks), but they can finish concurrently.
    // Only the block withing `andThen` needs to be synchronized.
    val waitingTask = () => {
      running = running + 1
      task()
        .andThen { case result =>
          synchronized {
            running = running - 1
            promise.tryComplete(result)
            startTasks()
          }
        }(executionContext)
      ()
    }

    waiting.enqueue(waitingTask)
    promise.future
  }

  private def startTasks(): Unit = synchronized {
    while (running < parallelism && waiting.nonEmpty) {
      val head = waiting.dequeue()
      head()
    }
  }
}
