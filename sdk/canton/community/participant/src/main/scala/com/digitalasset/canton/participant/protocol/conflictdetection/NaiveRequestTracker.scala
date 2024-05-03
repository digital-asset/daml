// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.data.{EitherT, NonEmptyChain}
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, TaskScheduler, TaskSchedulerMetrics}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
  PromiseUnlessShutdownFactory,
  RunOnShutdown,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ActiveContractStore.ContractState
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, SingleUseCell}
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.google.common.annotations.VisibleForTesting

import scala.annotation.nowarn
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.*
import scala.util.{Failure, Success, Try}

/** The naive request tracker performs all its tasks (activeness check/timeout/finalization) sequentially.
  * It accumulates all pending tasks in a priority queue and executes them as soon as the request tracker can
  * progress to their associated timestamp. The execution happens asynchronously in the execution context `ecForConflictDetection`.
  *
  * Requests are kept in memory from the call to [[NaiveRequestTracker!.addRequest]] until the finalization time or the timeout.
  *
  * @param initSc The first sequencer counter to be processed
  * @param initTimestamp Only timestamps after this timestamp are allowed
  */
private[participant] class NaiveRequestTracker(
    initSc: SequencerCounter,
    initTimestamp: CantonTimestamp,
    conflictDetector: ConflictDetector,
    taskSchedulerMetrics: TaskSchedulerMetrics,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit executionContext: ExecutionContext)
    extends RequestTracker
    with NamedLogging
    with FlagCloseableAsync
    with HasCloseContext { self =>
  import NaiveRequestTracker.*
  import RequestTracker.*

  override private[protocol] val taskScheduler =
    new TaskScheduler(
      initSc,
      initTimestamp,
      TimedTask.TimedTaskOrdering,
      taskSchedulerMetrics,
      timeouts,
      loggerFactory.appendUnnamedKey("task scheduler owner", "NaiveRequestTracker"),
      futureSupervisor,
    )

  // The task scheduler can decide to close itself if a task fails to execute
  // If that happens, close the tracker as well since we won't be able to make progress without a scheduler
  taskScheduler.runOnShutdown_(
    new RunOnShutdown {
      override def name: String = "close-request-tracker-due-to-scheduler-shutdown"
      override def done: Boolean = isClosing
      override def run(): Unit = self.close()
    }
  )(TraceContext.empty)

  /** Maps request counters to the data associated with a request.
    *
    * A request resides in the map from the call to [[RequestTracker!.addRequest]] until some time after
    * it times out or its commit set has been processed by the [[ConflictDetector]].
    *
    * @see NaiveRequestTracker.RequestData for the invariants
    */
  private[this] val requests: concurrent.Map[RequestCounter, RequestData] =
    new TrieMap[RequestCounter, RequestData]()

  override def tick(sc: SequencerCounter, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = {
    taskScheduler.addTick(sc, timestamp)
  }

  override def addRequest(
      rc: RequestCounter,
      sc: SequencerCounter,
      requestTimestamp: CantonTimestamp,
      activenessTimestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
      activenessSet: ActivenessSet,
  )(implicit
      traceContext: TraceContext
  ): Either[RequestAlreadyExists, FutureUnlessShutdown[RequestFutures]] = {
    ErrorUtil.requireArgument(rc.isNotMaxValue, "Request counter MaxValue used")
    ErrorUtil.requireArgument(sc.isNotMaxValue, "Sequencer counter Long.MaxValue used")
    ErrorUtil.requireArgument(
      requestTimestamp <= activenessTimestamp,
      withRC(
        rc,
        s"Activeness time $activenessTimestamp must not be earlier than the request timestamp $requestTimestamp.",
      ),
    )
    ErrorUtil.requireArgument(
      activenessTimestamp < decisionTime,
      withRC(
        rc,
        s"Activeness check at $activenessTimestamp must be before the decision time at $decisionTime.",
      ),
    )

    val data = RequestData.mk(
      sc,
      requestTimestamp,
      decisionTime,
      activenessSet,
      this,
      futureSupervisor,
    )

    requests.putIfAbsent(rc, data) match {
      case None =>
        logger.debug(
          withRC(
            rc,
            s"Added to the request tracker as a new request with timestamp $requestTimestamp",
          )
        )

        val checkActivenessAndLock =
          new CheckActivenessAndLock(rc, data.activenessResult, requestTimestamp, sc)
        val timeoutAction =
          new TriggerTimeout(rc, data.timeoutResult, requestTimestamp, decisionTime, sc)
        /* We don't need to guard these submissions with `shutdownSynchronizer` because the `taskScheduler` itself takes
         * care of not scheduling tasks during shutdown. */
        taskScheduler.scheduleTask(checkActivenessAndLock)
        taskScheduler.scheduleTask(timeoutAction)

        val f = conflictDetector
          .registerActivenessSet(rc, activenessSet)
          .map { _ =>
            // Tick the task scheduler only after all states have been prefetched into the conflict detector
            taskScheduler.addTick(sc, requestTimestamp)
            RequestFutures(data.activenessResult.futureUS, data.timeoutResult.futureUS)
          }
          .tapOnShutdown {
            data.activenessResult.shutdown()
            data.timeoutResult.shutdown()
          }
        Right(f)

      case Some(oldData) =>
        if (oldData == data) {
          logger.debug(withRC(rc, s"Added a second time to the request tracker"))
          Right(
            FutureUnlessShutdown.pure(
              RequestFutures(oldData.activenessResult.futureUS, oldData.timeoutResult.futureUS)
            )
          )
        } else {
          logger.info(withRC(rc, s"Signalled a second time with different parameters"))
          Left(RequestAlreadyExists(rc, oldData.sequencerCounter, oldData.requestTimestamp))
        }
    }
  }

  override def addResult(
      rc: RequestCounter,
      sc: SequencerCounter,
      resultTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Either[ResultError, Unit] = {
    ErrorUtil.requireArgument(
      resultTimestamp <= commitTime,
      withRC(rc, s"Commit time $commitTime before result timestamp $resultTimestamp"),
    )

    val preconditions =
      for {
        requestData <- requests.get(rc).toRight(RequestNotFound(rc))
        _ = ErrorUtil.requireArgument(
          resultTimestamp <= requestData.decisionTime,
          withRC(
            rc,
            s"Result timestamp $resultTimestamp after the decision time ${requestData.decisionTime}.",
          ),
        )
        _ <- Either.cond(resultTimestamp > requestData.requestTimestamp, (), RequestNotFound(rc))
      } yield requestData

    preconditions match {
      case Left(error) =>
        taskScheduler.addTick(sc, resultTimestamp)
        Left(error)
      case Right(requestData) =>
        val task =
          FinalizeRequest(
            rc,
            sc,
            requestData.requestTimestamp,
            requestData.commitSetPromise.futureUS,
            commitTime,
          )
        val data = FinalizationData(resultTimestamp, commitTime)(task.finalizationResult.futureUS)
        requestData.finalizationDataCell.putIfAbsent(data) match {
          case None =>
            logger.debug(
              withRC(rc, s"New result at $resultTimestamp signalled to the request tracker")
            )
            requestData.timeoutResult outcome NoTimeout
            taskScheduler.scheduleTask(task)
            taskScheduler.addTick(sc, resultTimestamp)
            Right(())

          case Some(oldData) =>
            if (oldData == data) {
              logger.debug(withRC(rc, s"Result signalled a second time to the request tracker."))
              Right(())
            } else {
              logger.warn(
                withRC(rc, s"Result with different parameters signalled to the request tracker.")
              )
              Left(ResultAlreadyExists(rc))
            }
        }
    }
  }

  override def addCommitSet(rc: RequestCounter, commitSet: Try[CommitSet])(implicit
      traceContext: TraceContext
  ): Either[CommitSetError, EitherT[FutureUnlessShutdown, NonEmptyChain[
    RequestTrackerStoreError
  ], Unit]] = {

    def tryAddCommitSet(
        commitSetPromise: PromiseUnlessShutdown[CommitSet],
        finalizationResult: FutureUnlessShutdown[
          Either[NonEmptyChain[RequestTrackerStoreError], Unit]
        ],
    ): Either[CommitSetError, EitherT[FutureUnlessShutdown, NonEmptyChain[
      RequestTrackerStoreError
    ], Unit]] = {
      // Complete the promise only if we're not shutting down.
      performUnlessClosing(functionFullName) {
        commitSetPromise.tryComplete(commitSet.map(UnlessShutdown.Outcome(_)))
      } match {
        case UnlessShutdown.AbortedDueToShutdown =>
          // Try to clean up as good as possible even though recovery of the ephemeral state will ultimately
          // take care of the cleaning up.
          logger.info(withRC(rc, s"Not adding commit set due to shutdown being in progress."))
          Either.right(EitherT.right(FutureUnlessShutdown.abortedDueToShutdown))
        case UnlessShutdown.Outcome(true) =>
          logger.debug(withRC(rc, "New commit set added."))
          Right(EitherT(finalizationResult))
        case UnlessShutdown.Outcome(false) =>
          val oldCommitSet = commitSetPromise.future.value
            .getOrElse(
              throw new RuntimeException(
                withRC(rc, s"Completed commit set promise does not contain a value")
              )
            )
          if (oldCommitSet == commitSet.map(UnlessShutdown.Outcome(_))) {
            logger.debug(withRC(rc, s"Commit set added a second time."))
            Right(EitherT(finalizationResult))
          } else if (oldCommitSet.toEither.contains(AbortedDueToShutdown)) {
            logger.debug(
              withRC(
                rc,
                s"Old commit set was aborted due to shutdown. New commit set will be ignored.",
              )
            )
            Left(CommitSetAlreadyExists(rc))
          } else {
            logger.warn(withRC(rc, s"Commit set with different parameters added a second time."))
            Left(CommitSetAlreadyExists(rc))
          }
      }
    }

    for {
      data <- requests.get(rc).toRight(RequestNotFound(rc))
      finData <- data.finalizationDataCell.get.toRight(ResultNotFound(rc))
      result <- tryAddCommitSet(data.commitSetPromise, finData.result)
    } yield result
  }

  override def getApproximateStates(coids: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, ContractState]] =
    conflictDetector.getApproximateStates(coids)

  /** Returns whether the request is in flight, i.e., in the requests map. */
  @VisibleForTesting
  def requestInFlight(rc: RequestCounter): Boolean = requests.contains(rc)

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
    SyncCloseable("taskScheduler", taskScheduler.close()),
    SyncCloseable("conflictDetector", conflictDetector.close()),
  )

  private[this] def evictRequest(rc: RequestCounter)(implicit traceContext: TraceContext): Unit = {
    logger.trace(withRC(rc, "Evicting it."))
    val _ = requests.remove(rc)
  }

  override def awaitTimestamp(timestamp: CantonTimestamp): Option[Future[Unit]] =
    taskScheduler.scheduleBarrier(timestamp)

  /** Releases all locks that are held by the given request */
  private[this] def releaseAllLocks(rc: RequestCounter, requestTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.trace(withRC(rc, "Releasing all locks."))
    if (!requests.contains(rc))
      throw new FatalRequestTrackerException(withRC(rc, s"No entry found in request table"))
    conflictDetector
      .finalizeRequest(CommitSet.empty, TimeOfChange(rc, requestTimestamp))
      .map { acsFuture =>
        FutureUtil.doNotAwait(
          acsFuture.onShutdown(logger.debug(s"Rollback of request $rc aborted due to shutdown")),
          s"Rollback of request $rc",
        )
      }
  }

  private[this] def withRC(rc: RequestCounter, msg: String): String = s"Request $rc: $msg"

  /** The action for checking activeness and locking the contracts
    *
    * @param rc The request counter
    * @param activenessResult The promise to be fulfilled with the result of the activeness check
    */
  private[this] class CheckActivenessAndLock(
      val rc: RequestCounter,
      activenessResult: PromiseUnlessShutdown[ActivenessResult],
      timestamp: CantonTimestamp,
      sequencerCounter: SequencerCounter,
  )(override implicit val traceContext: TraceContext)
      extends TimedTask(timestamp, sequencerCounter, Kind.Activeness) {

    /** Performs the activeness check consisting of the following:
      * <ul>
      *   <li>Check the activeness of the contracts in [[ActivenessSet.deactivations]] and [[ActivenessSet.usageOnly]].</li>
      *   <li>Check the non-existence of the contracts in [[ActivenessSet.creations]].</li>
      *   <li>Check the inactivity of the contracts in [[ActivenessSet.transferIns]].</li>
      *   <li>Lock all contracts to be deactivated.</li>
      *   <li>Lock all contracts to be activated (created or transferred-in).</li>
      *   <li>Fulfill the `activenessResult` promise with the result</li>
      * </ul>
      */
    override def perform(): FutureUnlessShutdown[Unit] =
      performUnlessClosingUSF("check-activeness-result") {
        logger.debug(withRC(rc, "Performing the activeness check"))

        val result = conflictDetector.checkActivenessAndLock(rc)
        activenessResult.completeWith(result)
        result.map { actRes =>
          logger.trace(withRC(rc, s"Activeness result $actRes"))
        }
      }.tapOnShutdown(activenessResult.shutdown())

    override def pretty: Pretty[this.type] = prettyOfClass(
      param("timestamp", _.timestamp),
      param("sequencerCounter", _.sequencerCounter),
      param("rc", _.rc),
    )

    override def close(): Unit = activenessResult.shutdown()
  }

  /** The action for triggering a timeout.
    *
    * @param rc The request counter
    * @param timeoutPromise The promise to be fulfilled with the timeout result.
    *                       This promise is also used to synchronize between results and timeouts:
    *                       An actual timeout occurs only if the promise is incomplete when the action is processed.
    */
  private[this] class TriggerTimeout(
      val rc: RequestCounter,
      timeoutPromise: PromiseUnlessShutdown[TimeoutResult],
      val requestTimestamp: CantonTimestamp,
      override val timestamp: CantonTimestamp,
      override val sequencerCounter: SequencerCounter,
  )(override implicit val traceContext: TraceContext)
      extends TimedTask(timestamp, sequencerCounter, Kind.Timeout) {

    override def perform(): FutureUnlessShutdown[Unit] =
      if (!timeoutPromise.isCompleted) {
        logger.debug(withRC(rc, "Timed out."))
        performUnlessClosingUSF("trigger-timeout")(releaseAllLocks(rc, requestTimestamp)).map { _ =>
          evictRequest(rc)
          /* Timeout promises are completed only here and in `addResult`.
           * These two completions never race.
           * Therefore, as the timeout promise was not completed in the `if` condition above,
           * we know that it is still not completed now.
           *
           * In detail, `addResult` checks that the result's timestamp is at most the decision time
           * and this check happens before it signals the timestamp to the task scheduler.
           * This task runs only after the task scheduler has observed the decision time.
           * So the two completions can only race if `addRequest` is called a second time,
           * as then the timestamp had already been signalled to the task scheduler.
           * In that case, `addResult` will see that the result has been added before
           * and therefore not complete the timeout future.
           *
           * We cannot use `Promise.trySuccess` in the above `if` condition to atomically test for completion and complete
           * the promise because this would complete the timeout promise too early, as the conflict detector has
           * not yet released the locks held by the request.
           */
          timeoutPromise outcome Timeout
          ()
        }
      } else { FutureUnlessShutdown.unit }

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("sequencerCounter", _.sequencerCounter),
        param("requestTimestamp", _.requestTimestamp),
        param("rc", _.rc),
      )

    override def close(): Unit = timeoutPromise.shutdown()
  }

  /** The action for finalizing a request by committing and rolling back contract changes.
    *
    * @param rc The request counter
    * @param sequencerCounter The sequencer counter on the result message
    * @param requestTimestamp The timestamp on the request
    * @param commitSetFuture The promise which contracts should be activated and deactivated.
    */
  private[this] case class FinalizeRequest(
      rc: RequestCounter,
      override val sequencerCounter: SequencerCounter,
      requestTimestamp: CantonTimestamp,
      commitSetFuture: FutureUnlessShutdown[CommitSet],
      commitTime: CantonTimestamp,
  )(override implicit val traceContext: TraceContext)
      extends TimedTask(commitTime, sequencerCounter, Kind.Finalization) {

    /** The promise to fulfill once the request has been finalized and all changes have been
      * persisted to the ACS.
      */
    val finalizationResult: PromiseUnlessShutdown[
      Either[NonEmptyChain[RequestTrackerStoreError], Unit]
    ] = mkPromise[Either[NonEmptyChain[RequestTrackerStoreError], Unit]](
      "finalization-result",
      futureSupervisor,
    )

    /** Tries to finalize the request with the data given in this class.
      *
      * @throws InvalidCommitSet if the commit set tries to archive or create a contract that was not locked
      *                          during the activeness check
      */
    override def perform(): FutureUnlessShutdown[Unit] =
      performUnlessClosingUSF("finalize-request") {
        commitSetFuture.transformWith {
          case Success(UnlessShutdown.Outcome(commitSet)) =>
            logger.debug(withRC(rc, s"Finalizing at $commitTime"))
            conflictDetector
              .finalizeRequest(commitSet, TimeOfChange(rc, requestTimestamp))
              .transform {
                case Success(UnlessShutdown.Outcome(storeFuture)) =>
                  // The finalization is complete when the conflict detection stores have been updated
                  finalizationResult.completeWith(storeFuture)
                  // Immediately evict the request
                  Success(UnlessShutdown.Outcome(evictRequest(rc)))
                case Success(UnlessShutdown.AbortedDueToShutdown) =>
                  finalizationResult.shutdown()
                  Success(UnlessShutdown.AbortedDueToShutdown)
                case Failure(e) =>
                  finalizationResult.tryFailure(e).discard[Boolean]
                  Failure(e)
              }

          case Success(UnlessShutdown.AbortedDueToShutdown) =>
            logger.debug(withRC(rc, s"Aborted finalizing at $commitTime due to shutdown"))
            finalizationResult.shutdown()
            FutureUnlessShutdown.abortedDueToShutdown

          case Failure(ex) =>
            logger.debug(withRC(rc, "Commit set computation failed"), ex)
            // Propagate the exception back to the protocol processor
            finalizationResult.failure(ex)
            // Pass the exception to the task scheduler and thereby interrupt conflict detection
            FutureUnlessShutdown.failed(ex)
        }
      }

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("sequencerCounter", _.sequencerCounter),
        param("requestTimestamp", _.requestTimestamp),
        param("rc", _.rc),
        param("commitTime", _.commitTime),
      )

    override def close(): Unit = finalizationResult.shutdown()
  }
}

private[conflictdetection] object NaiveRequestTracker {
  import RequestTracker.*

  /** Abstract class for tasks that the [[data.TaskScheduler]] accumulates in its `taskQueue`
    *
    * @param kind The kind of the task, which is used for ordering the tasks
    */
  sealed abstract class TimedTask(
      override val timestamp: CantonTimestamp,
      override val sequencerCounter: SequencerCounter,
      val kind: Kind,
  )(implicit val traceContext: TraceContext)
      extends TaskScheduler.TimedTask

  object TimedTask {
    def unapply(timedTask: TimedTask): Option[(CantonTimestamp, SequencerCounter, Kind)] =
      Some((timedTask.timestamp, timedTask.sequencerCounter, timedTask.kind))

    /** The order of tasks with the same timestamp is lexicographic by
      * <ol>
      *   <li>the `kind` of the task, and</li>
      *   <li>the sequencer counter `kind`</li>
      * <ol>
      * So if two tasks have the same timestamp, finalization comes first, then timeouts, and then activeness
      * checks for confirmation requests. The sequencer counter is used to resolve ties between requests
      * such that the earlier request gets priority (in case of logical reordering).
      */
    @nowarn("msg=match may not be exhaustive")
    val TimedTaskOrdering: Ordering[TimedTask] =
      Ordering
        .by[TimedTask, (Kind, SequencerCounter)] { case TimedTask(_, sc, kind) =>
          (kind, sc)
        }
  }

  /** Describes the kind of a task ordered by
    * [[Kind.Finalization]] < [[Kind.Timeout]] < [[Kind.Activeness]]
    *
    * @param id an internal [[Kind]] identifier used for comparisons.
    */
  sealed abstract class Kind(private val id: Int)
      extends Product
      with Serializable
      with Ordered[Kind] {
    override def compare(that: Kind): Int = id.compareTo(that.id)
  }

  object Kind {
    case object Finalization extends Kind(0)
    case object Timeout extends Kind(1)
    case object Activeness extends Kind(2)
  }

  /** Exception for fatal errors in the request tracker.
    * When this exception occurs, the state of the request tracker is inconsistent and should not be used any further.
    */
  class FatalRequestTrackerException(msg: String) extends RuntimeException(msg)

  /** Record for storing all the data that is needed for processing a request in the request tracker.
    * The mutable cells and promises are created when the request is added
    * so that the later tasks can fill in the data without having to update the [[NaiveRequestTracker!.requests]] map.
    * We use [[com.digitalasset.canton.util.SingleUseCell]]s rather than [[scala.concurrent.Promise]]s
    * when there is no need for synchronization.
    *
    * @param sequencerCounter     The sequencer counter of the request message
    * @param requestTimestamp     The timestamp on the request message
    * @param decisionTime         The decision time for the request, i.e., when it times out.
    * @param activenessSet        The activeness set used by the [[ConflictDetector]] in the activeness check
    *                             at the [[requestTimestamp]].
    * @param activenessResult     The promise for the [[ActivenessResult]] whose [[scala.concurrent.Future]]
    *                             [[NaiveRequestTracker.addRequest]] returned to the transaction processor.
    *                             The activeness check fulfills the promise.
    * @param timeoutResult        The promise for the timeout whose [[scala.concurrent.Future]]
    *                             [[NaiveRequestTracker.addRequest]] returned to the transaction processor.
    *                             When a result is added up to the [[decisionTime]], it is immediately fulfilled with [[NoTimeout]].
    *                             Otherwise, it will be fulfilled with [[Timeout]] at the [[decisionTime]].
    * @param finalizationDataCell Memory cell for storing the data needed to finalize the request.
    *                             This cell is filled by [[NaiveRequestTracker.addResult]].
    * @param commitSetPromise     Promise for storing the [[CommitSet]].
    *                             This promise is completed by [[NaiveRequestTracker.addCommitSet]].
    *                             As long as this cell is not filled, the request tracker will not progress beyond the request's
    *                             commit time.
    */
  private[NaiveRequestTracker] final case class RequestData private (
      sequencerCounter: SequencerCounter,
      requestTimestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
      activenessSet: ActivenessSet,
  )(
      val activenessResult: PromiseUnlessShutdown[ActivenessResult],
      val timeoutResult: PromiseUnlessShutdown[TimeoutResult],
      val finalizationDataCell: SingleUseCell[FinalizationData],
      val commitSetPromise: PromiseUnlessShutdown[CommitSet],
  )

  private[NaiveRequestTracker] object RequestData {
    def mk(
        sc: SequencerCounter,
        requestTimestamp: CantonTimestamp,
        decisionTime: CantonTimestamp,
        activenessSet: ActivenessSet,
        promiseUSFactory: PromiseUnlessShutdownFactory,
        futureSupervisor: FutureSupervisor,
    )(implicit elc: ErrorLoggingContext, executionContext: ExecutionContext): RequestData =
      new RequestData(
        sequencerCounter = sc,
        requestTimestamp = requestTimestamp,
        decisionTime = decisionTime,
        activenessSet = activenessSet,
      )(
        activenessResult = promiseUSFactory.mkPromise("activeness-result", futureSupervisor),
        timeoutResult = promiseUSFactory.mkPromise("timeout-result", futureSupervisor),
        finalizationDataCell = new SingleUseCell[FinalizationData],
        commitSetPromise = promiseUSFactory.mkPromise("commit-set", futureSupervisor),
      )
  }

  /** Data for finalization that is stored for the request when the result is added such that
    * [[NaiveRequestTracker!.addResult]] can be idempotent.
    *
    * @param resultTimestamp The timestamp on the result.
    * @param commitTime The commit time
    * @param result The promise to fulfill once the request has been finalized
    *               and all changes have been persisted to the ACS.
    */
  private final case class FinalizationData(
      resultTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
  )(val result: FutureUnlessShutdown[Either[NonEmptyChain[RequestTrackerStoreError], Unit]])
}
