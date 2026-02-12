// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{PromiseUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration

/** ProgressSupervisor is a component that is meant to monitor anomalies in the node's operation,
  * (i.e. node not making progressing), not covered by the conventional health checks, based on
  * timing and expectations of processing progress of a data flow. It is meant to trigger alerts and
  * possibly remedial actions:
  *   - Bumping log levels to DEBUG, flushing buffered logs, collecting thread dumps, heap dumps,
  *     etc.
  *   - Restarting the node.
  *   - Taking a poison pill.
  *
  * It is safe for asynchronous usage, i.e., multiple threads can arm and disarm monitors
  * concurrently. Disarming before arming is safe. As long as both operations are performed within
  * the `logAfter` duration, no warn action will be issued.
  *
  * Current usage of this class is to monitor sequencer backend event processing ("arm" for every
  * event addressed to sequencer itself), and sequencer self-subscription ("disarm" for every event
  * read by the sequencer's own sequencer client).
  */
class ProgressSupervisor(
    logLevel: Level,
    logAfter: Duration,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
    warnAction: => Unit = (),
) extends NamedLogging {

  logger.underlying.info(
    s"ProgressSupervisor created with log level $logLevel, log after $logAfter, future supervisor $futureSupervisor"
  )

  // below this bound: existing arm/disarm calls will be cancelled and any new ones ignored
  private val lowerBoundInclusive: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.MinValue)

  private val monitors: TrieMap[CantonTimestamp, PromiseUnlessShutdown[Unit]] = TrieMap.empty

  // Sometimes `disarm` overtakes `arm`, so we treat both ops the same, first - arming, second - disarming
  private def armOrDisarm(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit =
    if (timestamp <= lowerBoundInclusive.get()) {
      logger.debug(
        s"Ignoring progress monitor for $timestamp as it is before or at the lower bound ${lowerBoundInclusive.get()}"
      )
    } else {
      monitors
        .updateWith(timestamp) {
          case Some(promise) =>
            promise.success(UnlessShutdown.Outcome(()))
            None
          case None =>
            val promise = PromiseUnlessShutdown.supervised[Unit](
              s"self-subscription-observe-ts-$timestamp",
              futureSupervisor,
              logLevel = logLevel,
              logAfter = logAfter,
              warnAction = warnAction,
            )
            Some(promise)
        }
        .foreach { promise =>
          // start supervision only once the promise has made it into the map
          // due to promise.future being lazy, we need to explicitly poke it here
          promise.future.discard
        }
    }

  def arm(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Arming progress monitor for timestamp $timestamp")
    armOrDisarm(timestamp)
  }

  def disarm(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Disarming progress monitor for timestamp $timestamp")
    armOrDisarm(timestamp)
  }

  /** This method is useful to limit the bounds of progress monitoring during the node's startup:
    *   - Sequencer crash recovery can reset processing to an earlier block that has already been
    *     delivered and "disarmed", causing false positive alerts.
    *   - Sequencer client subscribes from an existing event to detect forks, which will call
    *     "disarm" on the event that will not be processed again, causing false positive alerts.
    */
  def ignoreTimestampsBefore(timestampInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = {
    val newLowerBound = lowerBoundInclusive.updateAndGet(_.max(timestampInclusive))
    logger.debug(s"Setting progress monitor lower bound to $newLowerBound")
    // we assume low number of monitors to clean up, and rare calls to this method,
    // otherwise we should switch `monitors` to a `SortedMap`
    monitors.keys.filter(_ <= newLowerBound).foreach { ts =>
      logger.debug(
        s"Dropping progress monitor for timestamp $ts below new lower bound $newLowerBound"
      )
      monitors
        .updateWith(ts) {
          case Some(promise) =>
            promise.success(UnlessShutdown.Outcome(()))
            None
          case None => None
        }
        .discard
    }
  }

}
