// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.protocol.Deliver
import com.digitalasset.canton.store.SequencedEventStore.{
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
  SequencedEventWithTraceContext,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicBoolean

/** Wrapper for a sequencer subscription event handler that will log warnings if the timestamps of
  * received messages appear significantly behind this consumer's clock.
  */
class DelayLogger(
    clock: Clock,
    logger: TracedLogger,
    threshold: NonNegativeFiniteDuration,
    gauge: Gauge[Long],
) {
  private val caughtUp = new AtomicBoolean(false)

  def checkForDelay(event: PossiblyIgnoredSequencedEvent[?]): Unit =
    event match {
      case event: OrdinarySequencedEvent[_] =>
        checkForDelay_(event.asSequencedSerializedEvent)
      case _ => ()
    }

  def checkForDelay_(event: SequencedEventWithTraceContext[?]): Unit = {
    implicit val traceContext: TraceContext = event.traceContext
    event.signedEvent.content match {
      case Deliver(_, ts, _, _, _, _, _) =>
        val now = clock.now
        val delta = java.time.Duration.between(ts.toInstant, now.toInstant)
        val deltaMs = delta.toMillis
        gauge.updateValue(deltaMs)
        val thresholdDuration = threshold.unwrap
        if (delta.compareTo(thresholdDuration) > 0) {
          if (caughtUp.compareAndSet(true, false)) {
            logger.warn(
              s"Detected late processing (or clock skew) of batch with timestamp = $ts; delta = $delta " +
                s"after sequencing (> threshold = $thresholdDuration)"
            )
          }
        } else if (caughtUp.compareAndSet(false, true)) {
          logger.info(
            s"Caught up with sequencer on batch with timestamp = $ts; delta = $delta " +
              s"(threshold = $thresholdDuration)"
          )
        }
      case _ => ()
    }
  }
}
