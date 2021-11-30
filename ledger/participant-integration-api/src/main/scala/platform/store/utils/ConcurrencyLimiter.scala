// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.utils

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait ConcurrencyLimiter[T] {
  def execute(task: () => Future[T]): Future[T]
}

class NoConcurrencyLimiter[T] extends ConcurrencyLimiter[T] {
  override def execute(task: () => Future[T]): Future[T] = task()
}

class QueueBasedConcurrencyLimiter[T](
    parallelism: Int,
    executionContext: ExecutionContext,
) extends ConcurrencyLimiter[T] {
  assert(parallelism > 0)

  case class WaitingTask(
      task: () => Future[T],
      promise: Promise[T] = Promise[T](),
  )
  private val waiting = mutable.Queue[WaitingTask]()
  private var running: Int = 0

  override def execute(task: () => Future[T]): Future[T] = synchronized {
    if (running >= parallelism) {
      val waitingTask = WaitingTask(task)
      waiting.enqueue(waitingTask)
      waitingTask.promise.future
    } else {
      startTask(task())
    }
  }

  private def startTask(task: Future[T]): Future[T] = synchronized {
    running = running + 1
    task.andThen { case _ => finishTask() }(executionContext)
  }

  private def finishTask(): Unit = synchronized {
    running = running - 1
    while (running < parallelism && waiting.nonEmpty) {
      val head = waiting.dequeue()
      startTask(
        head.task().andThen { case result => head.promise.tryComplete(result) }(executionContext)
      )
    }
  }
}
