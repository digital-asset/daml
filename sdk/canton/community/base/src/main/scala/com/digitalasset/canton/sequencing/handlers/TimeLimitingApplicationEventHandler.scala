// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.error.FatalError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.sequencing.PossiblyIgnoredApplicationHandler
import com.digitalasset.canton.sequencing.protocol.Envelope
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{SequencerCounter, config}

import java.time.temporal.ChronoUnit
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Monitors how long the processing of batches of sequenced events takes. If the processing takes
  * longer than the time limit on the given clock, the handler will once log an error or exit the
  * process, depending on `exitOnTimeout`.
  */
class TimeLimitingApplicationEventHandler(
    timeLimit: config.NonNegativeDuration,
    clock: Clock,
    exitOnTimeout: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import TimeLimitingApplicationEventHandler.*

  def timeLimit[E <: Envelope[_]](
      handler: PossiblyIgnoredApplicationHandler[E]
  )(implicit ec: ExecutionContext): PossiblyIgnoredApplicationHandler[E] =
    if (timeLimit.duration.isFinite) {
      handler.replace { boxedEnvelopes =>
        implicit val traceContext: TraceContext = boxedEnvelopes.traceContext
        NonEmpty.from(boxedEnvelopes.value) match {
          case Some(batches) =>
            val now = clock.now
            val deadline =
              CantonTimestamp
                .fromInstant(
                  now.toInstant.plus(timeLimit.asFiniteApproximation.toNanos, ChronoUnit.NANOS)
                )
                .getOrElse(CantonTimestamp.MaxValue)
            val data = ApplicationEventHandlerTimeoutData(
              batches.head1.counter,
              batches.last1.counter,
              boxedEnvelopes.value.map(_.traceContext),
              now,
            )
            val trigger = ApplicationEventHandlerTimeoutTrigger(logger, exitOnTimeout, data)
            clock.scheduleAt(trigger.trigger, deadline).discard[FutureUnlessShutdown[Unit]]
            handler(boxedEnvelopes).transform {
              case Success(Outcome(async)) =>
                Success(Outcome(async.thereafter(_ => trigger.markFinished())))
              case abort @ Success(AbortedDueToShutdown) =>
                trigger.markFinished()
                abort
              case failure @ Failure(_) =>
                trigger.markFinished()
                failure
            }

          case None => handler(boxedEnvelopes)
        }
      }
    } else handler
}

object TimeLimitingApplicationEventHandler extends HasLoggerName {
  private final case class ApplicationEventHandlerTimeoutData(
      sequencerCounterStart: SequencerCounter,
      sequencerCounterEnd: SequencerCounter,
      traceIds: Seq[TraceContext],
      start: CantonTimestamp,
  )(implicit val traceContext: TraceContext)

  private final class ApplicationEventHandlerTimeoutTrigger private (
      logger: TracedLogger,
      exitOnTimeout: Boolean,
  ) {
    // Use a plain volatile var to avoid the memory overhead of an atomic given that we don't need atomic operations
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    @volatile private[this] var dataF: Option[ApplicationEventHandlerTimeoutData] = None

    private def setData(data: ApplicationEventHandlerTimeoutData): Unit = dataF = Some(data)

    // Erase all references to the batch. This trigger will hang around for the whole timeout duration
    // and we want to avoid a memory hole.
    def markFinished(): Unit = {
      val dataO = dataF
      dataF = None
      dataO.foreach { data =>
        logger.trace(
          show"Processing of event batch with sequencer counters ${data.sequencerCounterStart} to ${data.sequencerCounterEnd} started at ${data.start} completed."
        )(data.traceContext)
      }
    }

    def trigger(at: CantonTimestamp): Unit =
      dataF.foreach {
        case data @ ApplicationEventHandlerTimeoutData(
              sequencerCounterStart,
              sequencerCounterEnd,
              traceIds,
              start,
            ) =>
          implicit val traceContext: TraceContext = data.traceContext
          val msg =
            show"Processing of event batch with sequencer counters $sequencerCounterStart to $sequencerCounterEnd started at $start did not complete by $at. Affected trace IDs: $traceIds"
          if (exitOnTimeout) FatalError.exitOnFatalError(msg, logger)
          else logger.error(msg)
      }
  }

  private object ApplicationEventHandlerTimeoutTrigger {
    def apply(
        logger: TracedLogger,
        exitOnTimeout: Boolean,
        data: ApplicationEventHandlerTimeoutData,
    ): ApplicationEventHandlerTimeoutTrigger = {
      val trigger = new ApplicationEventHandlerTimeoutTrigger(logger, exitOnTimeout)
      // Explicitly set the initial data through a setter so that the data cannot go
      // into the constructor where the Scala compiler might turn them into a field
      // where the data cannot be GCed once the processing has finished.
      trigger.setData(data)
      trigger
    }
  }
}
