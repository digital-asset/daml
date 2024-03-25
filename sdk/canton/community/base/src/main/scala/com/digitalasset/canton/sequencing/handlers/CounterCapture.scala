// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext

/** Capture the sequencer counter of the last successfully processed event (only synchronous processing).
  * @param initial Initial counter to return until a event is successfully processed.
  */
class CounterCapture(
    private val initial: SequencerCounter,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val currentValue = new AtomicLong(initial.v)

  /**  Wrap a handler and capture the counter of a successfully processed event.
    *  It only makes sense to wrap a single handler however this is not enforced.
    */
  def apply[E](handler: SerializedEventHandler[E]): SerializedEventHandler[E] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)
    event => {
      implicit val traceContext: TraceContext = event.traceContext
      for {
        result <- handler(event)
      } yield {
        // only update if successful
        result foreach { _ =>
          val counter = event.counter
          currentValue.set(counter.v)
          logger.trace(s"Captured sequencer counter ${counter}")
        }
        result
      }
    }
  }

  /** Get the latest offset. */
  def counter: SequencerCounter = SequencerCounter(currentValue.get)
}
