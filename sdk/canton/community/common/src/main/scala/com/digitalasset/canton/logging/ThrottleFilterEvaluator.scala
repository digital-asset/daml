// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.boolex.{EvaluationException, EventEvaluatorBase}
import ch.qos.logback.core.filter.EvaluatorFilter
import ch.qos.logback.core.spi.FilterReply

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent
import scala.concurrent.duration.*

/** Throttles logging events, so that no more than `maxLoggedOverPeriod` unique events are logged over `expiryPeriod`.
  * De-duplicates logging events over `expiryPeriod.`
  * Used to ensure that the interactive console remains usable even when the system is in a bad state and logging many errors.
  * Inspired by https://github.com/RotterdamLogisticsLab/logback-throttle/blob/master/src/main/kotlin/com/portofrotterdam/dbs/public/logback/throttle/Throttle.kt
  */
class ThrottleFilterEvaluator extends EvaluatorFilter[ILoggingEvent] {

  private val DEFAULT_EXPIRY_PERIOD = 30.seconds.toMillis
  private val DEFAULT_LOGGED_OVER_PERIOD = 30L

  val expiryPeriod: AtomicLong = new AtomicLong(DEFAULT_EXPIRY_PERIOD)
  val maxLoggedOverPeriod: AtomicLong = new AtomicLong(DEFAULT_LOGGED_OVER_PERIOD)

  // Map from log messages to the expiry time (in epoch millis) when the message will be removed from the cache
  // Use a concurrent map to prevent any state corruption from concurrent accesses
  val messageCache: concurrent.TrieMap[String, Long] = concurrent.TrieMap[String, Long]()

  // Allow configuration in the format
  //  <filter class="com.digitalasset.canton.logging.ThrottleFilterEvaluator">
  //    <expiryPeriod>10000</expiryPeriod>
  //    <maxLoggedOverPeriod>5</maxLoggedOverPeriod>
  //  </filter>
  def setExpiryPeriod(newValue: Long): Unit = { expiryPeriod.set(newValue) }
  def setMaxLoggedOverPeriod(newValue: Long): Unit = { maxLoggedOverPeriod.set(newValue) }

  private val mine: EventEvaluatorBase[ILoggingEvent] = new EventEvaluatorBase[ILoggingEvent]() {

    @throws[NullPointerException]
    @throws[EvaluationException]
    override def evaluate(event: ILoggingEvent): Boolean = {
      val now = Instant.now.toEpochMilli

      // The following logic is deliberately racy to avoid contention.
      // Race conditions may:
      // - Allow the max log rate to be exceeded
      // - Allow duplicate log messages to be logged within the expiryPeriod
      // - Cause "legitimate" log messages to be filtered out
      // However, the errors introduced will be minor. As this filter is intended to improve user experience in a
      // noisy console, the potential for small errors should not be a big problem.

      messageCache.filterInPlace({ case (_, expiryTime) => expiryTime > now })

      if (messageCache.size >= maxLoggedOverPeriod.get()) {
        // Drop the message, as the permitted log rate has been exceeded
        false
      } else if (messageCache.contains(event.getMessage)) {
        // De-duplicate the message within the time `expiryPeriod`
        false
      } else {
        val expiryTime = now + expiryPeriod.get()
        messageCache.update(event.getMessage, expiryTime)
        // Pass the message on through this filter
        true
      }
    }
  }

  this.setEvaluator(mine)
  this.setOnMismatch(FilterReply.DENY)
  this.setOnMatch(FilterReply.NEUTRAL) // Pass matching events onto the next filter

  override def start(): Unit = {
    if (!mine.isStarted) mine.start()
    super.start()
  }

}
