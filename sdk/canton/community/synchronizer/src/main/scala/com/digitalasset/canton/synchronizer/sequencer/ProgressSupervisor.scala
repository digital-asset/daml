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

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.Duration

/** ProgressSupervisor is a component that is meant to monitor anomalies in the node's operation,
  * (i.e. node not making progressing), not covered by the conventional health checks, based on
  * timing and expectations of processing progress of a data flow. It is meant to trigger alerts and
  * possibly remedial actions:
  *   - Bumping log levels to DEBUG, flushing buffered logs, collecting thread dumps, heap dumps,
  *     etc.
  *   - Restarting the node.
  *   - Taking a poison pill.
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

  private val monitors: ConcurrentHashMap[CantonTimestamp, PromiseUnlessShutdown[Unit]] =
    new ConcurrentHashMap()

  def arm(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Arming progress monitor for sequencing timestamp $timestamp")
    val promise = PromiseUnlessShutdown.supervised[Unit](
      s"self-subscription-observe-ts-$timestamp",
      futureSupervisor,
      logLevel = logLevel,
      logAfter = logAfter,
      warnAction = warnAction,
    )
    monitors.putIfAbsent(timestamp, promise)
    promise.future.discard // start supervision (due to promise.future being lazy)
  }

  def disarm(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Disarming progress monitor for sequencing timestamp $timestamp")
    Option(monitors.remove(timestamp)).foreach { promise =>
      promise.success(UnlessShutdown.Outcome(()))
    }
  }

}
