// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.utils

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait ConcurrencyLimiter {
  def execute[T](task: => Future[T]): Future[T]
}

class NoConcurrencyLimiter extends ConcurrencyLimiter {
  override def execute[T](task: => Future[T]): Future[T] = task
}

class QueueBasedConcurrencyLimiter(
    parallelism: Int,
    executionContext: ExecutionContext,
) extends ConcurrencyLimiter {
  assert(parallelism > 0)

  type Task = () => Unit
  private val waiting = mutable.Queue[Task]()
  private var running: Int = 0

  override def execute[T](task: => Future[T]): Future[T] = synchronized {
    val promise = Promise[T]()

    val waitingTask = () => {
      task
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
    startTasks()

    promise.future
  }

  private def startTasks(): Unit = synchronized {
    while (running < parallelism && waiting.nonEmpty) {
      val head = waiting.dequeue()
      running = running + 1
      head()
    }
  }
}
