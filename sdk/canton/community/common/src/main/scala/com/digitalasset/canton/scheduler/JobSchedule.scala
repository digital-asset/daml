// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.scheduler.JobSchedule.NextRun
import com.digitalasset.canton.scheduler.JobScheduler.*
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.tracing.TraceContext

/** Job schedule allows flexibly composing multiple administration job schedules.
  */
sealed trait JobSchedule {

  /** Determines how long (at millisecond granularity) to wait before running the next task depending on the result
    * of the previous execution result (affects how soon to retry e.g. on errors or when there is more work to do).
    *
    * Returns:
    * 1. time to wait and
    * 2. the specific schedule that corresponds to the most immediate time to wait. This can be used by the scheduler
    *    job on which actions to perform depending on the triggering schedule.
    */
  protected[scheduler] def determineNextRun(result: ScheduledRunResult)(implicit
      traceContext: TraceContext
  ): Option[NextRun]
}

/** Trait for individual schedule (i.e. non-compound schedule)
  */
sealed trait IndividualSchedule extends JobSchedule

/** Schedule based on a utc-based cron expression and the clock
  */
sealed trait CronSchedule {
  protected def cron: Cron
  protected def maxDuration: PositiveSeconds

  protected[scheduler] def waitDurationUntilNextRun(
      result: ScheduledRunResult,
      now: CantonTimestamp,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Option[NonNegativeFiniteDuration] = {

    def durationUntilNextWindowStarts
        : Either[Cron.CronNextTimeAfterError, NonNegativeFiniteDuration] =
      cron
        .getNextValidTimeAfter(now)
        .map(at => NonNegativeFiniteDuration.tryOfMillis(math.max((at - now).toMillis, 0L)))

    def backoffDurationOrUntilNextWindowStarts(
        backoff: NonNegativeFiniteDuration
    ): Either[Cron.CronNextTimeAfterError, NonNegativeFiniteDuration] = for {
      // To see if the window is still open:
      // Subtract the maximum duration from now (plus backoff) prior to passing the resulting rewound time to
      // "cron.getNextValidTimeAfter". Then if the resulting next valid time is before now,
      // the schedule window is still open and we don't need to wait for the next window.
      // This adjustment does not extend the window by accident because the cron
      // expression is only "valid" at the beginning of the window.
      //
      // Side note: CronExpression also has a getNextInvalidTimeAfter that would have been
      // significantly more intuitive to use instead, but suffers from a bug that causes
      // getNextInvalidTimeAfter to hang when provided a "wild-card" cron-expression such
      // as "* * * * * ? *" that is always true.
      isCurrentWindowStillActive <- cron
        .getNextValidTimeAfter(now + backoff - maxDuration)
        .map(nextValidTime => nextValidTime <= now)
      wait <-
        if (isCurrentWindowStillActive) Right(backoff) else durationUntilNextWindowStarts
    } yield wait

    val waitE = (result match {
      case Done => durationUntilNextWindowStarts
      case Error(message, backoff, logAsInfo) =>
        // Unless the error can happen under normal circumstances (e.g. transient initialization or shutdown),
        // log a warning so that node operators become aware of potentially persistent errors. #14223
        val err = s"Backing off $backoff or until next window after error: $message"
        if (logAsInfo) logger.info(err)
        else logger.warn(err)

        backoffDurationOrUntilNextWindowStarts(backoff)
      case MoreWorkToPerform =>
        backoffDurationOrUntilNextWindowStarts(NonNegativeFiniteDuration.Zero)
    }).map(_.some)

    waitE.valueOr {
      case Cron.DateConversionError(err) =>
        // A bit of paranoia in case of an out-of-bounds overflow.
        logger.warn(
          "Error trying to determine next time to run according to cron expression " +
            s"${cron.unwrap.getCronExpression}: $err. Waking up in 10 seconds to avoid sleeping indefinitely."
        )
        JobSchedule.backoffAfterCronExpressionError.some
      case Cron.NoNextValidTimeAfter(ts) =>
        logger.debug( // logged as info by caller
          s"Cron expression ${cron.unwrap.getCronExpression} allows for no more running after $ts"
        )
        None
    }
  }
}

/** Pruning-specific cron schedules come with a pruning retention
  */
class PruningCronSchedule(
    override val cron: Cron,
    override val maxDuration: PositiveSeconds,
    val retention: PositiveSeconds,
    clock: Clock,
    logger: TracedLogger,
) extends IndividualSchedule
    with CronSchedule {
  override protected[scheduler] def determineNextRun(result: ScheduledRunResult)(implicit
      traceContext: TraceContext
  ): Option[NextRun] =
    waitDurationUntilNextRun(result, clock.now, logger).map(NextRun(_, this))
}

final class ParticipantPruningCronSchedule(
    cron: Cron,
    maxDuration: PositiveSeconds,
    retention: PositiveSeconds,
    val pruneInternallyOnly: Boolean,
    clock: Clock,
    logger: TracedLogger,
) extends PruningCronSchedule(cron, maxDuration, retention, clock, logger)

/** Interval schedule for things such as regular updating of metrics
  */
final class IntervalSchedule(interval: PositiveSeconds) extends IndividualSchedule {
  override protected[scheduler] def determineNextRun(
      result: ScheduledRunResult
  )(implicit traceContext: TraceContext): Option[NextRun] =
    NextRun(NonNegativeFiniteDuration(interval), this).some
}

/** Compound schedule allow combining multiple individual schedules into one schedule
  * that represents "the union" of all schedules.
  */
final class CompoundSchedule(schedules: NonEmpty[Set[IndividualSchedule]]) extends JobSchedule {

  // pick the earliest next time among the schedules
  override protected[scheduler] def determineNextRun(
      result: ScheduledRunResult
  )(implicit traceContext: TraceContext): Option[NextRun] =
    schedules.map(_.determineNextRun(result)).minBy1(_.map(_.waitDuration))
}

object JobSchedule {

  /** Describe when to run next and which individual schedule is triggering the next run
    */
  final case class NextRun(waitDuration: NonNegativeFiniteDuration, schedule: IndividualSchedule)

  /** Compose the right schedule - if any - depending on the pruning schedule and metric update
    * configurations.
    */
  def fromPruningSchedule(
      ops: Option[PruningSchedule],
      metricUpdateInterval: Option[PositiveSeconds],
      clock: Clock,
      logger: TracedLogger,
  ): Option[JobSchedule] =
    JobSchedule(
      List(
        metricUpdateInterval.map(interval => new IntervalSchedule(interval)),
        ops.map(ps => new PruningCronSchedule(ps.cron, ps.maxDuration, ps.retention, clock, logger)),
      ).flatten
    )

  def apply(schedules: List[IndividualSchedule]): Option[JobSchedule] =
    NonEmpty.from(schedules).map { schedulesNel =>
      if (schedulesNel.lengthCompare(1) == 0)
        schedulesNel.head1
      else
        new CompoundSchedule(schedulesNel.toSet)
    }

  // Magic constants configs. We might make these configurable if they turn out to need to be tweaked.
  private[scheduler] lazy val backoffAfterCronExpressionError =
    NonNegativeFiniteDuration.tryOfSeconds(10L)

}
