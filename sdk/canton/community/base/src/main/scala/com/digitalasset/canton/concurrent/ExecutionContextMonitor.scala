// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, LoggerUtil, StackTraceUtil}

import java.time.Instant
import java.util.concurrent.*
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** Debugging utility used to write at regular intervals the executor service queue size into the logfile
  *
  * Useful to debug starvation issues.
  */
class ExecutionContextMonitor(
    val loggerFactory: NamedLoggerFactory,
    interval: NonNegativeFiniteDuration,
    warnInterval: NonNegativeFiniteDuration,
    override val timeouts: ProcessingTimeout,
)(implicit scheduler: ScheduledExecutorService)
    extends NamedLogging
    with FlagCloseable {

  private def schedule(runnable: Runnable): Unit = {
    val ms = interval.toScala.toMillis
    val _ = scheduler.scheduleAtFixedRate(runnable, ms, ms, TimeUnit.MILLISECONDS)
  }

  private val warnIntervalMs = warnInterval.duration.toMillis

  // indicates when the last pending task has been scheduled
  private val scheduled = new AtomicReference[Option[Long]](None)
  // indicates how many times the current deadlock (if any) has been reported
  private val reported = new AtomicInteger(0)

  import TraceContext.Implicits.Empty.*

  def monitor(ec: ExecutionContextIdlenessExecutorService): Unit = {
    logger.debug(s"Monitoring ${ec.name}")
    val runnable = new Runnable {
      override def run(): Unit = {
        if (!isClosing) {
          // if we are still scheduled, complain!
          val started = scheduled.getAndUpdate {
            case None => Some(Instant.now().toEpochMilli)
            case x => x
          }
          started match {
            // if we are still scheduled, complain
            case Some(started) =>
              reportIssue(ec, started)
            // if we aren't scheduled yet, schedule a future!
            case None =>
              implicit val myEc: ExecutionContext = ec
              FutureUtil.doNotAwait(
                Future {
                  if (scheduled.getAndSet(None).isEmpty) {
                    logger.error(s"Are we monitoring the EC ${ec.name} twice?")
                  }
                  // reset the reporting
                  if (reported.getAndSet(0) > 0) {
                    emit(
                      s"Task runner ${ec.name} is just overloaded, but operating correctly. Task got executed in the meantime."
                    )
                  }
                },
                "Monitoring future failed despite being trivial ...",
              )
          }
        }
      }
    }
    schedule(runnable)
  }

  private def emit(message: => String): Unit = {
    logger.warn(message)
  }

  private def reportIssue(
      ec: ExecutionContextIdlenessExecutorService,
      startedEpochMs: Long,
  ): Unit = {
    val delta = Instant.now().toEpochMilli - startedEpochMs
    val current = reported.getAndIncrement()
    val warn = delta > (warnIntervalMs * current)
    if (warn) {
      val deltaTs = LoggerUtil.roundDurationForHumans(Duration(delta, TimeUnit.MILLISECONDS))
      if (current > 0) {
        emit(
          s"Task runner ${ec.name} is still stuck or overloaded for ${deltaTs}. (queue-size=${ec.queueSize}).\n$ec"
        )
      } else {
        emit(
          s"Task runner ${ec.name} is stuck or overloaded for ${deltaTs}. (queue-size=${ec.queueSize}).\n$ec"
        )
      }
      val traces = StackTraceUtil.formatStackTrace(_.getName.startsWith(ec.name))
      val msg = s"Here is the stack-trace of threads for ${ec.name}:\n$traces"
      if (current == 0)
        logger.info(msg)
      else
        logger.debug(msg)
    } else {
      reported.updateAndGet(x => Math.max(0, x - 1)).discard
    }
  }

}
