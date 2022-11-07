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

/** @param parentO - used to define a hierarchy (a tree) of concurrency limiters such that we can
  *               express constraints like: for 5 local limiters each running at most 3 concurrent tasks
  *               set a global limit of at most 10 concurrent in total.
  *                If a parent is defined, then tasks will run on parent's execution context.
  */
class QueueBasedConcurrencyLimiter(
    parallelism: Int,
    executionContext: ExecutionContext,
    parentO: Option[ConcurrencyLimiter] = None,
) extends ConcurrencyLimiter {
  assert(parallelism > 0)

  private type Task = () => Future[_]
  private val waiting = mutable.Queue[Task]()
  private var running: Int = 0

  override def execute[T](task: => Future[T]): Future[T] = synchronized {
    val promise = Promise[T]()

    val waitingTask: () => Future[_] = () => {
      task
        .andThen { case result =>
          synchronized {
            running = running - 1
            promise.tryComplete(result)
            startTasks()
          }
        }(executionContext)
      promise.future
    }

    waiting.enqueue(waitingTask)
    startTasks()

    promise.future
  }

  private def startTasks(): Unit = synchronized {
    while (running < parallelism && waiting.nonEmpty) {
      val head = waiting.dequeue()
      running = running + 1
      parentO match {
        case Some(parent) =>
          parent.execute(head())
        case None => {
          head()
        }
      }
    }
  }
}
