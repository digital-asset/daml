// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.TryUtil.ForFailedOps
import com.digitalasset.canton.util.{LoggerUtil, TryUtil}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.ref.WeakReference
import scala.util.{Failure, Success}

/** Alert if a future does not complete within the prescribed duration
  *
  * We use future based synchronisation in some places, where we use a promise to only kick off an
  * action once a promise is completed. This can lead to deadlocks where something does not start
  * because we never complete the promise. This leads to hard to debug situations. We can support
  * debugging by tracking such futures. As this is costly, we'll turn this off in production.
  *
  * @see
  *   HasFutureSupervision for a mixin
  */
trait FutureSupervisor {
  def supervised[T](
      description: => String,
      warnAfter: Duration = 10.seconds,
      logLevel: Level = Level.WARN,
  )(fut: Future[T])(implicit errorLoggingContext: ErrorLoggingContext): Future[T]

  def supervisedUS[T](
      description: => String,
      warnAfter: Duration = 10.seconds,
      logLevel: Level = Level.WARN,
  )(fut: FutureUnlessShutdown[T])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(supervised(description, warnAfter, logLevel)(fut.unwrap))

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
      defaultWarningInterval: NonNegativeDuration,
      override protected val loggerFactory: NamedLoggerFactory,
  )(implicit
      scheduler: ScheduledExecutorService
  ) extends FutureSupervisor
      with NamedLogging {

    import Impl.*

    /** All supervised futures are kept in this mutable singly-linked list. Contains `null` iff the
      * scheduler has been stopped.
      */
    private val headScheduled: AtomicReference[ScheduledEntry] =
      new AtomicReference[ScheduledEntry](ScheduledSentinel())

    /** Approximates the number of currently supervised futures in the list. The returned number may
      * deviate from the actual number in both directions:
      *
      *   - It may not count the supervised futures whose `.supervised` calls have not yet finished.
      *   - It may count the supervised futures that are being removed by the currently running
      *     slowness check.
      *
      * [[ScheduledSentinel]] entries in the list are omitted from the count.
      */
    private val approximateSizeRef: AtomicInteger = new AtomicInteger(0)

    // schedule regular background checks
    private val scheduledHandle = scheduler.scheduleWithFixedDelay(
      () => checkSlow(),
      defaultCheckMs,
      defaultCheckMs,
      TimeUnit.MILLISECONDS,
    )

    @VisibleForTesting
    private[concurrent] def inspectApproximateSize: Int = approximateSizeRef.get

    /** Returns the current head, which may be a [[ScheduledSentinel]]. Returns [[scala.None$]] if
      * the scheduler has been stopped.
      */
    @VisibleForTesting
    private[concurrent] def inspectHead: Option[ScheduledEntry] = Option(headScheduled.get())

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

      // First a new Sentinel at the head of the linked list
      // as another indirection so that we can modify the `next` pointer afterwards
      // as we go through the list, removing completed futures, without having to access
      // the atomic again. This has the advantage that `insertAtHead` and `stop`
      // do not interfere with these updates as they don't look at `next` of what's in the list.
      // This sentinel will be removed with the next scheduled traversal of the list.

      val newSentinel = ScheduledSentinel()
      if (insertAtHead(newSentinel)) {
        val approximateSize = approximateSizeRef.get()

        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var removed = 0

        val summarizedAlerts: mutable.SortedMap[Level, mutable.Map[String, AlertStatistics]] =
          mutable.TreeMap.empty[Level, mutable.Map[String, AlertStatistics]](
            // Sorted in reverse so that we log higher levels first
            Ordering.by[Level, Int](_.toInt).reverse
          )

        TryUtil
          .tryCatchInterrupted {
            // Traverses the linked list with a look-ahead of 1, i.e.,
            // `current` lags behind by one element compared to what is being looked at.
            // This way, if we want to remove the entry we're looking at,
            // we simply modify the `next` field of `current`.
            @tailrec def go(current: ScheduledEntry): Unit = {
              val next = current.next
              if (next != null) {
                next match {
                  case blocked: ScheduledFuture =>
                    if (!blocked.stillRelevantAndIncomplete) {
                      current.next = blocked.next
                      removed += 1
                      go(current)
                    } else {
                      blocked.alertNow(now) match {
                        case DoNotAlert =>
                        case AlertWithLog =>
                          val dur = Duration.fromNanos(now - blocked.startNanos)
                          val message =
                            s"${blocked.description()} has not completed after ${LoggerUtil.roundDurationForHumans(dur)}"
                          log(message, blocked.logLevel, blocked.errorLoggingContext)
                        case AlertWithSummary =>
                          val level = blocked.logLevel
                          val traceId =
                            blocked.errorLoggingContext.traceId.getOrElse("<no trace id>")
                          val summariesForLevel = summarizedAlerts.getOrElseUpdate(
                            level,
                            new mutable.HashMap[String, AlertStatistics](),
                          )
                          val summary =
                            summariesForLevel.getOrElseUpdate(traceId, new AlertStatistics())
                          summary.add(now - blocked.startNanos)
                      }
                      go(blocked)
                    }
                  case sentinel: ScheduledSentinel =>
                    // Remove all sentinels we encounter.
                    current.next = sentinel.next
                    go(current)
                }
              }
            }

            go(newSentinel)

            // Finally print the summaries; one for each log level.
            summarizedAlerts.foreach { case (level, summariesForLevel) =>
              val sb = new StringBuilder()
              sb.append(
                s"Supervised futures for the following trace IDs have not completed with alert log level $level:\n"
              )
              summariesForLevel.foreach { case (traceId, statistics) =>
                val count = statistics.getCount
                val shortest = Duration.fromNanos(statistics.getShortest)
                sb.append("  ").append(traceId).discard
                if (count > 1) sb.append(" x").append(count).discard
                sb.append("(").append(LoggerUtil.roundDurationForHumans(shortest)).discard
                if (count > 1) {
                  val longest = Duration.fromNanos(statistics.getLongest)
                  sb.append("..").append(LoggerUtil.roundDurationForHumans(longest)).discard
                }
                sb.append(")").append("\n").discard[StringBuilder]
              }
              implicit val traceContext: TraceContext = TraceContext.empty
              LoggerUtil.logAtLevel(level, sb.toString())
            }
          }
          .valueOr { ex =>
            // Catch all non-fatal (and interrupted) exceptions and log them so that they don't get discarded.
            // Do not rethrow as this would terminate the scheduler immediately.
            // This is fairly crude because if the exception comes from the supervised future's description,
            // this will block the removal of all earlier added futures, but at least we see the exception.
            // Since new supervised futures are added to the head of the linked list,
            // they will be supervised as normally, so this is not a growing memory leak
            // unless there's a constant influx of throwing supervisions.
            noTracingLogger.error(
              s"Future supervision has failed with an exception, but will repeat in ${defaultCheckMs}ms",
              ex,
            )
          }

        // We update the approximation only once we've gone through the whole list
        // to reduce contention on this atomic.
        approximateSizeRef.addAndGet(-removed).discard

        val end = System.nanoTime()
        val duration = TimeUnit.NANOSECONDS.toMillis(end - now)
        if (duration >= defaultCheckMs)
          // Logged only at INFO level instead of WARN because GC runs can pause
          // supervision and lead to false positives.
          noTracingLogger.info(
            s"Supervising futures itself takes longer than the check frequency: $duration ms. Approximate supervision size: $approximateSize minus $removed"
          )
      } else {
        noTracingLogger.debug(
          "Skipping slowness check because this future supervisor has been stopped"
        )
      }
    }

    /** Replaces the [[headScheduled]] with `entry` and points `entry`'s [[ScheduledEntry.next]] to
      * the previous head. Does nothing and returns `false` if the supervisor has been stopped.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private def insertAtHead(entry: ScheduledEntry): Boolean =
      headScheduled.updateAndGet { oldHead =>
        if (oldHead == null) null
        else {
          entry.next = oldHead
          entry
        }
      } != null

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

        if (insertAtHead(itm)) {
          approximateSizeRef.incrementAndGet()

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
        } else {
          errorLoggingContext.debug(
            "Skipping future supervision because the supervisor has been stopped"
          )
          fut
        }
      }

    private def elapsed(item: ScheduledFuture): Duration = {
      val dur = Duration.fromNanos(System.nanoTime() - item.startNanos)
      LoggerUtil.roundDurationForHumans(dur)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def stop(): Unit = {
      scheduledHandle.cancel( /* mayInterruptIfRunning = */ false).discard[Boolean]
      headScheduled.set(null)
      approximateSizeRef.set(0)
    }
  }

  object Impl {

    /** How frequently the future supervisor checks for slowness, in milliseconds */
    val defaultCheckMs = 1000L

    /** An entry in the */
    private[Impl] sealed trait ScheduledEntry extends Product with Serializable {

      /** The next pointer in the linked list of scheduled future supervisions.
        *
        * We use `null` as the sentinel instead of `Option[ScheduledEntry]` to avoid allocating lots
        * of useless `Some`s.
        *
        * A plain variable without synchronization suffices here because access to this variable is
        * synchronized via the [[Impl.headScheduled]] atomic: Before this `ScheduledEntry` becomes
        * the head, this field is only accessed by the single thread that tries to insert the entry
        * into the list. Afterwards, this field is accessed only by the [[Impl.checkSlow]] method,
        * where the scheduler makes sure sequentializes all runs of [[Impl.checkSlow]]. The update
        * of the atomic creates the required happens-before relationships.
        */
      @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
      private[Impl] var next: ScheduledEntry = _
    }

    private final case class ScheduledSentinel() extends ScheduledEntry

    /** A scheduled future to monitor.
      *
      * The weak reference ensures that we stop monitoring this future if the future is garbage
      * collected, say because nothing else references it. This can happen if the future has been
      * completed (but the supervisor has not yet gotten around to checking it again) or the user
      * code decided that it does not need the future after all.
      *
      * This is safe because garbage collection will not remove Futures that are currently executing
      * or may execute at a later point in time, as all those futures are either referenced from the
      * executing thread or the execution context they're scheduled on.
      */
    @VisibleForTesting
    private[concurrent] final case class ScheduledFuture(
        fut: WeakReference[Future[?]],
        description: () => String,
        startNanos: Long,
        warnNanos: Long,
        errorLoggingContext: ErrorLoggingContext,
        logLevel: Level,
    ) extends ScheduledEntry {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      // Must only be accessed by the `checkSlow` method that runs sequentially
      private var hasWarned: Boolean = false

      def stillRelevantAndIncomplete: Boolean = fut match {
        case WeakReference(f) => !f.isCompleted
        case _ => false
      }

      private[Impl] def alertNow(currentNanos: Long): AlertNow =
        if (hasWarned) AlertWithSummary
        else {
          val elapsed = currentNanos - startNanos
          if (elapsed > warnNanos) {
            hasWarned = true
            AlertWithLog
          } else DoNotAlert
        }
    }

    private[Impl] sealed trait AlertNow extends Product with Serializable
    private[Impl] case object DoNotAlert extends AlertNow
    private[Impl] case object AlertWithLog extends AlertNow
    private[Impl] case object AlertWithSummary extends AlertNow

    private[Impl] class AlertStatistics {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      // Must only be accessed by the `checkSlow` method that runs sequentially
      private var count: Int = 0

      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      // Must only be accessed by the `checkSlow` method that runs sequentially
      private var shortest: Long = Long.MaxValue

      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      // Must only be accessed by the `checkSlow` method that runs sequentially
      private var longest: Long = Long.MinValue

      def getCount: Int = count
      def getShortest: Long = shortest
      def getLongest: Long = longest

      def add(elapsedNanos: Long): Unit = {
        count += 1
        if (elapsedNanos < shortest) shortest = elapsedNanos
        if (elapsedNanos > longest) longest = elapsedNanos
      }

      override def toString: String = s"AlertStatistics(#$count($shortest..$longest))"
    }

  }
}
