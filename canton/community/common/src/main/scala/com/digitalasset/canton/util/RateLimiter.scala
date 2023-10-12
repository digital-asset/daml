// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveDouble}

import java.util.concurrent.atomic.AtomicReference

/** Utility class that allows clients to keep track of a rate limit.
  *
  * The decay rate limiter keeps track of the current rate, allowing temporary
  * bursts. This allows temporary bursts at the risk of overloading the system too quickly.
  *
  * Clients need to tell an instance whenever they intend to start a new task.
  * The instance will inform the client whether the task can be executed while still meeting the rate limit.
  *
  * Guarantees:
  * <ul>
  * <li>Maximum burst size: if `checkAndUpdateRate` is called `n` times in parallel, at most `max 1, maxTasksPerSecond * maxBurstFactor` calls may return `true`.</li>
  * <li>Average rate: if `checkAndUpdateRate` is called at a rate of at least `maxTasksPerSecond` during `n` seconds,
  *     then the number of calls that return `true` divided by `n` is roughly `maxTasksPerSecond` .*
  * </ul>
  *
  * @param maxTasksPerSecond the maximum number of tasks per second
  * @param maxBurstFactor ratio of max tasks per second when the throtteling should start to kick in
  */
class RateLimiter(
    val maxTasksPerSecond: NonNegativeNumeric[Double],
    maxBurstFactor: PositiveDouble,
    nanoTime: => Long = System.nanoTime(),
) {

  private val currentState_ = new AtomicReference[State](State(approvedLastTask = false, 0, 0.0))
  val maxBurst: Double = maxBurstFactor.value * maxTasksPerSecond.value

  private case class State(
      approvedLastTask: Boolean,
      lastUpdateNanos: Long,
      approvedTasks: Double,
  ) {
    def update(now: Long): State = {
      // determine the time elapsed since we submitted last time
      val deltaNanos = now - lastUpdateNanos
      // determine the fractional number of commands that we were allowed to submit in that period
      val adjust = maxTasksPerSecond.value * deltaNanos.toDouble / 1e9
      // remove that number from the "approvedTasks"
      val newApprovedTasks = Math.max(0, approvedTasks - adjust)
      // if the newApprovedTasks is below the maxBurst value, approve the request and increment the approvedTasks
      // this allows bursts of up to "maxBurst" and thereafter enforces a strictly continuous rate limit
      if (newApprovedTasks < maxBurst) {
        State(approvedLastTask = true, now, newApprovedTasks + 1)
      } else State(approvedLastTask = false, now, newApprovedTasks)
    }
  }

  /** Call this before starting a new task.
    * @return whether the tasks can be executed while still meeting the rate limit
    */
  final def checkAndUpdateRate(): Boolean = {
    val now = nanoTime
    currentState_.updateAndGet(_.update(now)).approvedLastTask
  }

}
