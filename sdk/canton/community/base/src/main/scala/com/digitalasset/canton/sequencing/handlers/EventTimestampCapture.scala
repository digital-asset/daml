// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SequencedEventHandler
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Capture the sequencing timestamp of the last successfully processed event (only synchronous
  * processing).
  * @param initial
  *   Initial timestamp (or None) to return until an event is successfully processed.
  */
class EventTimestampCapture(
    private val initial: Option[CantonTimestamp],
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val currentValue = new AtomicReference[Option[CantonTimestamp]](initial)

  /** Wrap a handler and capture the timestamp of a successfully processed event. It only makes
    * sense to wrap a single handler however this is not enforced.
    */
  def apply[E](handler: SequencedEventHandler[E]): SequencedEventHandler[E] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)
    event => {
      implicit val traceContext: TraceContext = event.traceContext
      for {
        result <- handler(event)
      } yield {
        // only update if successful
        result foreach { _ =>
          currentValue.set(Some(event.timestamp))
          logger.trace(s"Captured event timestamp ${event.timestamp}")
        }
        result
      }
    }
  }

  /** Get the latest offset. */
  def latestEventTimestamp: Option[CantonTimestamp] = currentValue.get
}
