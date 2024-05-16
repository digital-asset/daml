// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.ApplicationHandler
import com.digitalasset.canton.sequencing.protocol.Envelope
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.concurrent.{ExecutionContext, blocking}

object ThrottlingApplicationEventHandler {

  private case object Token

  def throttle[Box[+_ <: Envelope[?]], E <: Envelope[?]](
      maximumInFlightEventBatches: PositiveInt,
      handler: ApplicationHandler[Box, E],
      metrics: SequencerClientMetrics,
  )(implicit
      ec: ExecutionContext
  ): ApplicationHandler[Box, E] = {
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
      blocking { queue.put(Token) }
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
          _ => (), {
            // don't forget to unblock other threads on shutdown or exception of the outer future such that we don't block other threads
            queue.remove().discard
            metrics.handler.actualInFlightEventBatches.dec()
          },
        )
    }
  }
}
