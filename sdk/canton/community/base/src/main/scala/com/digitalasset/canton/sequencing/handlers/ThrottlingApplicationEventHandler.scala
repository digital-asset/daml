// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.config.RequireTypes.NonNegativeNumeric.SubtractionResult
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.PossiblyIgnoredApplicationHandler
import com.digitalasset.canton.sequencing.handlers.ThrottlingApplicationEventHandler.{
  AtLimit,
  BelowCapacity,
  ThrottlingState,
}
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

class ThrottlingApplicationEventHandler(override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  def throttle(
      maximumInFlightEventBatches: PositiveInt,
      handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      metrics: SequencerClientMetrics,
  )(implicit
      ec: ExecutionContext
  ): PossiblyIgnoredApplicationHandler[ClosedEnvelope] = {

    def acquirePermit(s: ThrottlingState)(implicit traceContext: TraceContext) =
      s match {
        case BelowCapacity(alreadyRunning) =>
          if (alreadyRunning < maximumInFlightEventBatches)
            BelowCapacity(alreadyRunning.increment.toNonNegative)
          else AtLimit(PromiseUnlessShutdown.unsupervised[Unit]())
        case AtLimit(_) =>
          // This method will run inside an atomic update operation. Normally one shouldn't log in this case,
          // but since reaching this state is
          // a) not expected and a programming error, and
          // b) throws an exception,
          // it's fine to do here.
          ErrorUtil.invalidState(
            "Synchronization issue: cannot handle an event while at capacity and another event already waiting"
          )
      }

    def releasePermit(s: ThrottlingState)(implicit traceContext: TraceContext) = s match {
      case BelowCapacity(alreadyRunning) =>
        alreadyRunning.subtract(NonNegativeInt.one) match {
          case SubtractionResult(result, NonNegativeInt.zero) => BelowCapacity(result)
          case SubtractionResult(_, _) =>
            ErrorUtil.invalidState("Tried to release a permit but there was none to release")
        }
      case AtLimit(_) => BelowCapacity(maximumInFlightEventBatches.toNonNegative)
    }

    val state = new AtomicReference[ThrottlingState](BelowCapacity(NonNegativeInt.zero))

    metrics.handler.maxInFlightEventBatches.updateValue(maximumInFlightEventBatches.unwrap)

    handler.replace { tracedEvents =>
      import tracedEvents.traceContext
      val newState = state.updateAndGet(acquirePermit)
      newState.continuation
        .flatMap { _ =>
          metrics.handler.actualInFlightEventBatches.inc()
          handler(tracedEvents)
            .map(asyncResult =>
              asyncResult.thereafter { _ =>
                val oldState = state.getAndUpdate(releasePermit)
                metrics.handler.actualInFlightEventBatches.dec()
                oldState.release()
              }
            )
            .thereafterSuccessOrFailure(
              success = _ => (),
              failure = {
                state.getAndUpdate(releasePermit).discard
                metrics.handler.actualInFlightEventBatches.dec()
                // this happens when the synchronous processing part failed.
                // therefore we don't have to release the promise, because the event dispatcher
                // in the sequencer client will shut down.
              },
            )
        }
    }
  }
}

object ThrottlingApplicationEventHandler {
  private sealed trait ThrottlingState extends Product with Serializable {
    def continuation: FutureUnlessShutdown[Unit]
    def release(): Unit
  }
  private final case class BelowCapacity(alreadyRunning: NonNegativeInt) extends ThrottlingState {
    override def continuation: FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit
    def release(): Unit = ()
  }
  private final case class AtLimit(promise: PromiseUnlessShutdown[Unit]) extends ThrottlingState {
    override def continuation: FutureUnlessShutdown[Unit] = promise.futureUS

    override def release(): Unit = promise.outcome_(())
  }
}
