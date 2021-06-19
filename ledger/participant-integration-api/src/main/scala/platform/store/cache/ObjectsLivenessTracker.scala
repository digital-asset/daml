// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.util.{Timer, TimerTask}

import scala.annotation.tailrec
import scala.ref.{PhantomReference, ReferenceQueue}

private[cache] class ObjectsLivenessTracker[T <: AnyRef](
    incrementCounter: () => Unit,
    decrementCounter: () => Unit,
    referenceQueue: ReferenceQueue[T],
) {
  private[cache] val refs = java.util.Collections.newSetFromMap(
    new java.util.concurrent.ConcurrentHashMap[PhantomReference[T], java.lang.Boolean]
  )

  private val timer = new Timer("buffer-liveness-tracker", true)
  timer.scheduleAtFixedRate(
    new TimerTask {
      override def run(): Unit = {

        @tailrec
        def pollAndClear(): Unit = {
          val maybeHead = referenceQueue.poll
          maybeHead.foreach { case ref: PhantomReference[_] =>
            ref.clear()
            refs.remove(ref)
            decrementCounter()
          }
          if (maybeHead.nonEmpty) pollAndClear()
        }

        pollAndClear()
      }
    },
    100L,
    100L,
  )

  def track(obj: T): Unit = {
    incrementCounter()
    val _ = refs.add(new PhantomReference(obj, referenceQueue))
  }
}

private[cache] object ObjectsLivenessTracker {
  def apply[T <: AnyRef](
      incrementCounter: () => Unit,
      decrementCounter: () => Unit,
  ): ObjectsLivenessTracker[T] =
    new ObjectsLivenessTracker(incrementCounter, decrementCounter, new ReferenceQueue[T])
}
