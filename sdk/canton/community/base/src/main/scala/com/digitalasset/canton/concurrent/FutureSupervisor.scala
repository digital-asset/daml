// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.ref.WeakReference
import scala.util.{Failure, Success}

/** Alert if a future does not complete within the prescribed duration
  *
  * We use future based synchronisation in some places, where we use a promise to only kick
  * off an action once a promise is completed. This can lead to deadlocks where something
  * does not start because we never complete the promise.
  * This leads to hard to debug situations. We can support debugging by tracking such futures.
  * As this is costly, we'll turn this off in production.
  *
  * @see HasFutureSupervision for a mixin
  */
trait FutureSupervisor {
  def supervised[T](
      description: => String,
      warnAfter: Duration = 10.seconds,
      logLevel: Level = Level.WARN,
  )(fut: Future[T])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[T]
  def supervisedUS[T](
      description: => String,
      warnAfter: Duration = 10.seconds,
      logLevel: Level = Level.WARN,
  )(
      fut: FutureUnlessShutdown[T]
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[T] = FutureUnlessShutdown(
    supervised(description, warnAfter, logLevel)(fut.unwrap)
  )
}

object FutureSupervisor {

  object Noop extends FutureSupervisor {
    override def supervised[T](
        description: => String,
        warnAfter: Duration,
        logLevel: Level = Level.WARN,
    )(fut: Future[T])(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[T] = fut
  }

  class Impl(
      defaultWarningInterval: NonNegativeDuration
  )(implicit
      scheduler: ScheduledExecutorService
  ) extends FutureSupervisor {
    import Impl.ScheduledFuture

    private val scheduled = new AtomicReference[Seq[ScheduledFuture]](Seq())
    private val defaultCheckMs = 1000L

    // schedule regular background checks
    scheduler.scheduleWithFixedDelay(
      () => checkSlow(),
      defaultCheckMs,
      defaultCheckMs,
      TimeUnit.MILLISECONDS,
    )

    @VisibleForTesting
    private[concurrent] def inspectScheduled: Seq[ScheduledFuture] = scheduled.get()

    private def log(
        message: String,
        level: Level,
        elc: ErrorLoggingContext,
        exception: Option[Throwable] = None,
    ): Unit =
      exception
        .map(LoggerUtil.logThrowableAtLevel(level, message, _)(elc))
        .getOrElse(LoggerUtil.logAtLevel(level, message)(elc))

    private def checkSlow(): Unit = {
      val now = System.nanoTime()
      val cur = scheduled.updateAndGet(_.filter(_.stillRelevantAndIncomplete))
      cur.filter(x => x.alertNow(now)).foreach { blocked =>
        val dur = Duration.fromNanos(now - blocked.startNanos)
        val message =
          s"${blocked.description()} has not completed after ${LoggerUtil.roundDurationForHumans(dur)}"
        log(message, blocked.logLevel, blocked.errorLoggingContext)
      }
    }

    def supervised[T](
        description: => String,
        warnAfter: Duration = defaultWarningInterval.duration,
        logLevel: Level = Level.WARN,
    )(fut: Future[T])(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[T] =
      // If the future has already completed, there's no point in supervising it in the first place
      if (fut.isCompleted) fut
      else {
        val itm = ScheduledFuture(
          WeakReference(fut),
          () => description,
          startNanos = System.nanoTime(),
          warnAfter.toNanos,
          errorLoggingContext,
          logLevel,
        )
        scheduled.updateAndGet(x => x.filter(_.stillRelevantAndIncomplete) :+ itm)

        // Use the direct execution context so that the supervision follow-up steps doesn't affect task-based scheduling
        // because the follow-up is run on the future itself.
        implicit val directExecutionContext: ExecutionContext =
          new DirectExecutionContext(errorLoggingContext.noTracingLogger)
        fut.thereafter {
          case Success(_) =>
            val time = elapsed(itm)
            if (time > warnAfter) {
              errorLoggingContext.info(
                s"$description succeed successfully but slow after $time"
              )
            }
          case Failure(exception) =>
            log(
              s"$description failed with exception after ${elapsed(itm)}",
              logLevel,
              errorLoggingContext,
              Some(exception),
            )
        }
      }

    private def elapsed(item: ScheduledFuture): Duration = {
      val dur = Duration.fromNanos(System.nanoTime() - item.startNanos)
      LoggerUtil.roundDurationForHumans(dur)
    }

  }
  object Impl {

    /** A scheduled future to monitor.
      *
      * The weak reference ensures that we stop monitoring this future if the future is garbage collected,
      * say because nothing else references it. This can happen if the future has been completed
      * (but the supervisor has not yet gotten around to checking it again) or the user code decided
      * that it does not need the future after all.
      *
      * This is safe because garbage collection will not remove Futures that are currently executing
      * or may execute at a later point in time, as all those futures are either referenced from
      * the executing thread or the execution context they're scheduled on.
      */
    @VisibleForTesting
    private[concurrent] final case class ScheduledFuture(
        fut: WeakReference[Future[_]],
        description: () => String,
        startNanos: Long,
        warnNanos: Long,
        errorLoggingContext: ErrorLoggingContext,
        logLevel: Level,
    ) {
      val warnCounter = new AtomicInteger(1)

      def stillRelevantAndIncomplete: Boolean = fut match {
        case WeakReference(f) => !f.isCompleted
        case _ => false
      }

      def alertNow(currentNanos: Long): Boolean = {
        val cur = warnCounter.get()
        if ((currentNanos - startNanos > (warnNanos * cur)) && stillRelevantAndIncomplete) {
          warnCounter.incrementAndGet().discard
          true
        } else false
      }
    }
  }
}
