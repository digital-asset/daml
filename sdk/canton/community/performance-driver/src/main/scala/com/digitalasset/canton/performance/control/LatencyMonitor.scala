// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.control

import java.time.Instant
import scala.Ordered.orderingToOrdered
import scala.collection.concurrent.TrieMap

class LatencyMonitor(rate: SubmissionRate) {

  private val started = TrieMap[String, Instant]()

  def schedule(commandId: String): Unit =
    started += (commandId -> Instant.now)

  def failed(commandId: String): Unit =
    started.remove(commandId).foreach { tm =>
      // add "long" failures to latency so that we start to throttle
      // but don't bias the measurements on "fast" failures
      val now = Instant.now()
      val latency = now.toEpochMilli - tm.toEpochMilli
      if (rate.latencyMs < latency.toDouble) {
        rate.latencyObservation(latency)
      }
    }

  def observedTransaction(commandId: String): Unit =
    started.remove(commandId).foreach { tm =>
      val now = Instant.now()
      require(now >= tm, s"Trying to archive future contract $tm. Now: $now.")
      val latency = now.toEpochMilli - tm.toEpochMilli
      rate.latencyObservation(latency)
    }
}
