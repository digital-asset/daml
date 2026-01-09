// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.digitalasset.canton.time.Clock
import org.apache.pekko.actor.{Cancellable, Scheduler}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

object SchedulerTestUtil {
  def mockScheduler(clock: Clock) =
    new Scheduler {

      override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit
          executor: ExecutionContext
      ): Cancellable = {
        val cancelled = new AtomicBoolean(false)
        val cancellable = new Cancellable {
          override def cancel(): Boolean = cancelled.compareAndSet(false, true)
          override def isCancelled: Boolean = cancelled.get()
        }
        val _ = clock.scheduleAfter(
          _ => if (!cancelled.get()) runnable.run(),
          delay.toJava,
        )
        cancellable
      }

      override def schedule(
          initialDelay: FiniteDuration,
          interval: FiniteDuration,
          runnable: Runnable,
      )(implicit executor: ExecutionContext): Cancellable = ???
      override def maxFrequency: Double = 42
    }
}
