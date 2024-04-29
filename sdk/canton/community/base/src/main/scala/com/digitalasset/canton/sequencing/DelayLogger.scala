// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.protocol.Deliver
import com.digitalasset.canton.store.SequencedEventStore.{
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicBoolean

/** Wrapper for a sequencer subscription event handler that will log warnings if the timestamps of received messages
  * appear significantly behind this consumer's clock.
  */
class DelayLogger(
    clock: Clock,
    logger: TracedLogger,
    threshold: NonNegativeFiniteDuration,
    gauge: Gauge[Long],
) {
  private val caughtUp = new AtomicBoolean(false)

  def checkForDelay(event: PossiblyIgnoredSequencedEvent[_]): Unit = event match {
    case OrdinarySequencedEvent(signedEvent, _) =>
      implicit val traceContext: TraceContext = event.traceContext
      signedEvent.content match {
        case Deliver(counter, ts, _, _, _, _) =>
          val now = clock.now
          val delta = java.time.Duration.between(ts.toInstant, now.toInstant)
          val deltaMs = delta.toMillis
          gauge.updateValue(deltaMs)
          if (delta.compareTo(threshold.unwrap) > 0) {
            if (caughtUp.compareAndSet(true, false)) {
              logger.warn(
                s"Late processing (or clock skew) of batch with counter=$counter with timestamp $delta ms after sequencing."
              )
            }
          } else if (caughtUp.compareAndSet(false, true)) {
            logger.info(
              s"Caught up with batch with counter=${counter} with sequencer with $delta ms delay"
            )
          }
        case _ => ()
      }
    case _ => ()
  }
}
