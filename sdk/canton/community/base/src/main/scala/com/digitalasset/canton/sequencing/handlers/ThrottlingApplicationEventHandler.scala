// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.PossiblyIgnoredApplicationHandler
import com.digitalasset.canton.sequencing.handlers.ThrottlingApplicationEventHandler.Token
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, blocking}

class ThrottlingApplicationEventHandler(override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  def throttle(
      maximumInFlightEventBatches: PositiveInt,
      handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      metrics: SequencerClientMetrics,
  )(implicit
      ec: ExecutionContext
  ): PossiblyIgnoredApplicationHandler[ClosedEnvelope] = {
    metrics.handler.maxInFlightEventBatches.updateValue(maximumInFlightEventBatches.unwrap)
    /*
    It would be better if we didn't have to block a thread completely and instead
    could use Promise / Future, but given that there can be at most one thread blocked
    at a time due to the sequencer client calling this stuff sequentially,
    simplicity beats pre-mature optimization here.
     */
    val queue: BlockingQueue[Token.type] =
      new ArrayBlockingQueue[Token.type](
        maximumInFlightEventBatches.unwrap
      )

    handler.replace { tracedEvents =>
      if (!queue.offer(Token)) {
        implicit val traceContext: TraceContext = tracedEvents.traceContext

        val t1 = System.nanoTime()
        logger.info(
          s"Event queue already contains $maximumInFlightEventBatches batches. Waiting before adding envelopes."
        )
        blocking(queue.put(Token))

        val waitingDuration =
          LoggerUtil.roundDurationForHumans(Duration.fromNanos(System.nanoTime() - t1))
        logger.info(s"Adding envelopes to queue after waiting for $waitingDuration")
      }

      metrics.handler.actualInFlightEventBatches.inc()
      handler(tracedEvents)
        .map { asyncF =>
          // this will ensure that we unregister from the queue once the inner future asyncF is done, whether
          // it's a success, a shutdown or an exception that is thrown
          asyncF.thereafter { _ =>
            queue.remove().discard
            metrics.handler.actualInFlightEventBatches.dec()
          }
        }
        .thereafterSuccessOrFailure(
          success = _ => (),
          failure = {
            // don't forget to unblock other threads on shutdown or exception of the outer future such that we don't block other threads
            queue.remove().discard
            metrics.handler.actualInFlightEventBatches.dec()
          },
        )
    }
  }
}

object ThrottlingApplicationEventHandler {
  private case object Token
}
