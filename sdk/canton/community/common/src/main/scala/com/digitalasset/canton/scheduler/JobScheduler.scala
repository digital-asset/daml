// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.scheduler.JobSchedule.NextRun
import com.digitalasset.canton.scheduler.JobScheduler.*
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Reusable scheduler base class for scheduling automated admin jobs.:
  *   - Based on a single-threaded scheduled executor.
  *   - Also manages schedule persistence reacting to schedule changes.
  *
  * Encapsulates generic scheduling logic reusable by specific schedulers.
  */
abstract class JobScheduler(
    name: String,
    processingTimeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends Scheduler
    with NamedLogging {

  /** Implements the code that is to be executed when the scheduled time has arrived for the
    * duration of the "maxDuration". Within a single such "window" schedulerJob will be called until
    * "Done" is returned as a result the first time.
    *
    * As guidance long running logic should be broken up into "chunks" expected to run at most for 1
    * minute although in the face of unpredictable database performance this is a "best effort".
    * Tasks may exceed the "maxDuration" window by however long they take to execute.
    *
    * @param schedule
    *   the specific schedule among the potentially multiple schedulers that triggered this
    *   particular job run. Useful for callees to extract additional information (such as retention
    *   for pruning) or to determine the type of work scheduled (e.g. pruning versus pruning metric
    *   update).
    */
  def schedulerJob(schedule: IndividualSchedule)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ScheduledRunResult]

  /** Hook to create and initialize the schedule when scheduler becomes active
    * @return
    *   if override returns Some[A] go ahead and schedule; if None don't
    */
  def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]]

  private val isActiveReplica = new AtomicBoolean(false)
  private val activeSchedulerState = new AtomicReference[Option[ActiveSchedulerState]](None)

  private def activate()(implicit traceContext: TraceContext): Future[Unit] =
    initializeSchedule().map(_.fold {
      logger.debug(s"Scheduler not configured. Not activating.")
    } { schedule =>
      logger.debug(s"Activating scheduler")
      activeSchedulerState
        .getAndSet(
          Some(
            ActiveSchedulerState(
              schedule,
              Threading.singleThreadScheduledExecutor(name, noTracingLogger),
            )
          )
        )
        .foreach { oldState =>
          logger.warn(s"Scheduler unexpectedly already started. Stopping old executor")
          oldState.close()
          logger.info(s"Stopped old unexpectedly active scheduler")
        }
      scheduleNextRun(
        MoreWorkToPerform // Upon becoming active assume there is more work to do, i.e. schedule asap
      )
    })

  protected def deactivate()(implicit traceContext: TraceContext): Unit =
    activeSchedulerState
      .getAndSet(None)
      .fold {
        logger.debug(s"Scheduler already stopped")
      } { oldState =>
        logger.info(s"Stopping scheduler")
        oldState.close()
        logger.info(s"Stopped scheduler")
      }

  protected def isScheduleActivated: Boolean =
    activeSchedulerState.get.nonEmpty

  override def start()(implicit traceContext: TraceContext): Future[Unit] = {
    isActiveReplica.set(true)
    activate()
  }

  override def stop()(implicit traceContext: TraceContext): Unit = {
    isActiveReplica.set(false)
    deactivate()
  }

  private def scheduleNextRun(
      result: ScheduledRunResult
  )(implicit traceContext: TraceContext): Unit =
    runIfActive(
      state => {
        val maybeNextRun = state.schedule.determineNextRun(result)
        maybeNextRun.fold(
          logger.info(s"Not scheduling anymore as schedule specifies no future valid next time")
        ) { case NextRun(wait, schedule) =>
          state.schedulerExecutor.schedule(
            new SchedulerRunnable(traceContext => schedulerJob(schedule)(traceContext)),
            wait.duration.toMillis,
            TimeUnit.MILLISECONDS,
          )
          logger.debug(s"With $result scheduled next $wait")
        }
      },
      (),
    )

  def reactivateSchedulerIfActive()(implicit traceContext: TraceContext): Future[Unit] =
    if (isActiveReplica.get) {
      // Stop and start scheduler executor to ensure the new schedule goes into effect asap.
      // This purposely emphasizes safety over efficiency.
      deactivate()
      activate()
    } else Future.unit

  def reactivateSchedulerIfActiveET()(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    EitherT(reactivateSchedulerIfActive().map(_ => Either.unit[String]))

  def updateScheduleAndReactivateIfActive(
      update: => Future[Unit]
  )(implicit traceContext: TraceContext): Future[Unit] = for {
    _ <- update
    _ <- reactivateSchedulerIfActive()
  } yield ()

  private def runIfActive[T](code: ActiveSchedulerState => T, noOp: T)(implicit
      traceContext: TraceContext
  ): T =
    activeSchedulerState.get() match {
      case None =>
        logger.info(s"Scheduler has become inactive.")
        noOp
      case Some(activeState) => code(activeState)
    }

  private class SchedulerRunnable(job: TraceContext => FutureUnlessShutdown[ScheduledRunResult])
      extends Runnable {
    override def run(): Unit = TraceContext.withNewTraceContext {
      implicit traceContext: TraceContext =>
        val future =
          runIfActive(
            _ => {
              logger.debug(s"Starting scheduler job")
              job(traceContext)
            },
            noOp = FutureUnlessShutdown.pure(Done),
          )
            .transform {
              case Success(UnlessShutdown.Outcome(result)) =>
                logger.debug(s"Completed scheduler job")
                Success(UnlessShutdown.Outcome(scheduleNextRun(result)))
              case Failure(NonFatal(t)) =>
                // log non-fatal errors as info rather than warning as we expect noisy when canceling arbitrary jobs
                logger.info(s"Scheduler job non fatal exception", t)
                Success(UnlessShutdown.Outcome(scheduleNextRun(Error(t.getMessage))))
              case Failure(fatal) => ErrorUtil.internalErrorTry(fatal)
              case Success(UnlessShutdown.AbortedDueToShutdown) =>
                Success(UnlessShutdown.Outcome(scheduleNextRun(Error("Aborted due to shutdown."))))
            }

        // Wait for schedule job indefinitely as we rely on scheduler jobs to break up long-running tasks.
        // We don't interrupt the job to prevent infinite retries.
        processingTimeouts.unbounded
          .await(
            s"Scheduler Job",
            Some(Level.INFO), // not WARN because RejectedExecutionException on stop/shutdown
          )(future.failOnShutdownToAbortException("JobScheduler.run"))
    }
  }

}

object JobScheduler {
  sealed trait ScheduledRunResult extends PrettyPrinting

  case object Done extends ScheduledRunResult {
    override protected def pretty: Pretty[Done.type] = prettyOfObject[Done.type]
  }
  case object MoreWorkToPerform extends ScheduledRunResult {
    override protected def pretty: Pretty[MoreWorkToPerform.type] =
      prettyOfObject[MoreWorkToPerform.type]
  }

  final case class Error(
      message: String,
      backoff: NonNegativeFiniteDuration = defaultBackoffAfterSchedulerJobError,
      logAsInfo: Boolean = false,
  ) extends ScheduledRunResult {
    override protected def pretty: Pretty[Error] = prettyOfClass(
      param("message", _.message.unquoted),
      param("backoff", _.backoff),
      paramIfTrue("log as info", _.logAsInfo),
    )
  }

  // When the scheduler is actively being scheduled, it has a ScheduledExecutorService the current schedule needed
  // to determine when to schedule the next task.
  private final case class ActiveSchedulerState(
      schedule: JobSchedule,
      schedulerExecutor: ScheduledExecutorService,
  ) extends AutoCloseable {
    override def close(): Unit =
      schedulerExecutor.shutdownNow().discard
  }

  // Magic constants configs. We might make these configurable if they turn out to need to be tweaked.
  private lazy val defaultBackoffAfterSchedulerJobError = NonNegativeFiniteDuration.tryOfSeconds(1L)
}
