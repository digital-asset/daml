// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.data.{EitherT, NonEmptyChain}
import com.digitalasset.canton.data.{CantonTimestamp, TaskScheduler}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetector.LockedStates
import com.digitalasset.canton.participant.protocol.conflictdetection.NaiveRequestTracker.TimedTask
import com.digitalasset.canton.participant.store.ActiveContractStore.ContractState
import com.digitalasset.canton.participant.store.{ActiveContractStore, TransferStore}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

import scala.concurrent.*
import scala.util.Try

/** The request tracker handles all the tasks around conflict detection that are difficult to parallelize.
  * In detail, it tracks in-flight requests, performs activeness checks, detects conflicts between requests,
  * keeps track of the sequencer time, checks for timeouts, and orchestrates the updates to the
  * [[com.digitalasset.canton.participant.store.ActiveContractStore]].
  * It does not write to the [[com.digitalasset.canton.participant.protocol.RequestJournal]], though.
  * The request tracker lives entirely in memory on a single compute node.
  *
  * The request tracker is a deterministic component that depends only on the ACS state and the sequencer messages.
  * The order and timing in which the request tracker receives this information does not matter.
  * (Even more broadly, the participant has no functional notion of “wall-clock” time;
  * all time and timestamps come from sequencer messages.)
  * These properties of determinism and sequencer time are both important parts of Canton
  * and critical to the crash recovery model.
  *
  * To keep track of the sequencer time, the request tracker must be notified
  * of every message that is received from the sequencer.
  * These notifications need not happen in order, but they must happen eventually. Every such message has
  * an associated monotonically increasing [[SequencerCounter]]. If one of the sequencer counters is skipped,
  * the request tracker may stall indefinitely. Every message has an associated timestamp. Time must
  * increase with every message in the order given by the sequencer counter.
  * This means that for sequencer counters `sc1` and `sc2` with `sc1` &lt; `sc2`, the associated timestamps `ts1` and `ts2`
  * must satisfy `ts1` &lt; `ts2`.
  *
  * Requests are identified by [[RequestCounter]]s, which themselves must be a monotonically increasing sequence without
  * gaps.
  *
  * The request tracker uses these time signals to determine on which requests a verdict can be issued,
  * which requests have timed out, and which requests and contract states can be safely evicted
  * from the tracker's internal state. We say that the request tracker has <strong>observed</strong> a timestamp
  * if it has been signalled this timestamp or a later one. The request tracker can <strong>progress
  * to</strong> a timestamp `ts` if it has observed all timestamps up to and including `ts`
  * and all commit sets with commit times up to `ts` have been added with [[RequestTracker.addCommitSet]].
  *
  * If the request tracker can progress to a timestamp `ts`, then it must eventually complete all futures
  * that [[RequestTracker.addRequest]] and [[RequestTracker.addCommitSet]] have returned
  * and that are associated with a timestamp equal or prior to `ts`.
  * The futures are associated to the following timestamps:
  * <ul>
  *   <li>Activeness result: the activeness time of the request, typically the timestamp on the request</li>
  *   <li>Timeout result: the decision time of the request</li>
  *   <li>Finalization result: the commit time of the request</li>
  * </ul>
  *
  * The three methods [[RequestTracker.addRequest]], [[RequestTracker.addResult]],
  * and [[RequestTracker.addCommitSet]] must normally be called in the given sequence;
  * the latter two need not be called if the request has timed out.
  * A request is <strong>in-flight</strong> from the corresponding call to [[RequestTracker.addRequest]]
  * until the request tracker has progressed to its decision time (if the request times out) or its commit time.
  *
  * These three methods are idempotent while the request is in-flight. That is, if one of the methods is called twice
  * with the same arguments, provided that the first one succeeded, then the second call has no effect,
  * but its return value is equivalent to what the first call returned.
  * If the method is called twice for the same request counter with different arguments, the subsequent behavior
  * of the request tracker becomes unspecified.
  * The same applies if the same sequencer counter is supplied several times with different timestamps.
  *
  * A request tracker is typically initialized with a [[SequencerCounter]] and a [[com.digitalasset.canton.data.CantonTimestamp]],
  * causing it to be ready to handle requests starting from the given sequencer counter (inclusive) and timestamp
  * (exclusive).
  *
  * =Conflict detection=
  * The request tracker checks whether a request uses only contracts that are active
  * at the activeness timestamp of the request.
  * To that end, it determines the [[ActivenessResult]] of the request.
  * A non-conflicting request should be approved in a response by the participant.
  * A conflicting request should be rejected.
  *
  * To describe conflict behavior, we introduce a few concepts:
  *
  * A <strong>conflict detection time</strong> is a triple ([[com.digitalasset.canton.data.CantonTimestamp]], `Kind`, [[SequencerCounter]]).
  * There are three kinds, and they are ordered by
  * {{{Finalization < Timeout < Activeness}}}.
  * Conflict detection times are ordered lexicographically.
  * In other words, {{{(ts1, kind1, sc1) < (ts2, kind2, sc2)}}} if and only if
  * {{{ts1 < ts2}}} or {{{ts1 == ts2 && kind1 < kind2}}} or {{{ts1 == ts2 && kind1 == kind2 && sc1 < sc2.}}}
  *
  * A request with [[SequencerCounter]] `sc` and [[com.digitalasset.canton.data.CantonTimestamp]] `ts` induces two conflict detection times:
  * The <strong>sequencing time</strong> at `(ts, Activeness, sc)` and the <strong>timeout time</strong> at
  * (decisionTime, Timeout, sc) where `decisionTime` is the decision time of the request.
  * Without logical reordering, the sequencing time is also the request's activeness time.
  * (Technically, the activeness time must lie between the sequencing time and the decision time;
  * see [[RequestTracker.addRequest]].)
  * A result with [[SequencerCounter]] `sc` and [[com.digitalasset.canton.data.CantonTimestamp]] `ts` also defines two conflict detection times:
  * The <strong>verdict time</strong> `(ts, Result, sc)` and the <strong>finalization time</strong>
  * `(commitTime, Finalize, sc)` where `commitTime` is the commit time of the request.
  *
  * A request is <strong>active</strong> at some conflict detection time `t` if all of the following holds:
  * <ul>
  *   <li>Its sequencing time is equal or prior to `t`.</li>
  *   <li>If no finalization time is known for the request, then `t` is equal or prior to the request's timeout time.</li>
  *   <li>If a finalization time is known for the request, then `t` is equal or prior to the finalization time.</li>
  * </ul>
  * The finalization time of a request is known if the result has been successfully signalled
  * to the request tracker using [[RequestTracker.addResult]].
  *
  * A request is <strong>finalized</strong> at some conflict detection time `t`
  * if its finalization time is known and equal or prior to `t`.
  *
  * A contract is <strong>active</strong> at time `t` if a request with activeness time at most `t` activates it, and
  * there is no finalized request at time `t` that has deactivated it.
  * Activation happens through creation and transfer-in.
  * Deactivation happens through archival and transfer-out.
  * An active contract `c` is locked at time `t` if one of the following cases holds:
  * <ul>
  *   <li>There is an active request at time `t` that activates `c`.
  *     In that case, we say that the contract is in activation.</li>
  *   <li>There is an active request at time `t` that deactivates `c` and the request does not activate `c`.</li>
  * </ul>
  * A contract may be locked because of activation and deactivation at the same time if there are two active requests,
  * one activating the contract and another deactivating it.
  * For example, if one request `r1`
  * creates a contract with sequencing time `t1` and finalization time `t1'` and another request `r2`
  * with sequencing time `t2` between `t1` and `t1'` archives it (despite that `r2`'s activeness check fails),
  * then the contract is created at time `t1` and archived at `t2`.
  *
  * Activeness of contracts is checked at the request's activeness time.
  * The [[ActivenessResult]] lists all contracts from the [[ActivenessSet]] of the request that are either locked or whose precondition fails.
  * The activeness check succeeds if the activeness result is empty.
  */
trait RequestTracker extends RequestTrackerLookup with AutoCloseable with NamedLogging {
  import RequestTracker.*

  private[protocol] val taskScheduler: TaskScheduler[TimedTask]

  /** Tells the request tracker that a message with the given sequencer counter and timestamp has been received.
    *
    * If [[RequestTracker!.addRequest]] or [[RequestTracker!.addResult]]
    * shall be called for the same message, [[RequestTracker!.tick]] may only be called after those methods
    * as they may schedule tasks at the timestamp or later and task scheduling for a timestamp must be done before
    * the time is observed.
    *
    * A given sequencer counter may be signalled several times provided that all calls signal the same timestamp.
    * Only the first such call is taken into account.
    * Since [[RequestTracker!.addRequest]] and [[RequestTracker!.addResult]] implicitly
    * signal the sequencer counter and timestamp to the request tracker (unless specified otherwise),
    * it is safe to call this method after calling these methods.
    *
    * The timestamps must increase with the sequencer counters.
    *
    * @param sequencerCounter The sequencer counter of the message. Must not be `Long.MaxValue`.
    * @param timestamp The timestamp on the message.
    * @throws java.lang.IllegalArgumentException If one of the following conditions hold:
    *         <ul>
    *           <li>If `sequencerCounter` is `Long.MaxValue`.</li>
    *           <li>If not all sequencer counters below `sequencerCounter` have been signalled
    *             and the `timestamp` does not increase with sequencer counters.</li>
    *           <li>If all sequencer counters below `sequencerCounter` have been signalled
    *             and the timestamp is at most the timestamp of an earlier sequencer counter.</li>
    *           <li>If `sequencerCounter` has been signalled before with a different `timestamp`
    *             and not all sequencer counters below `sequencerCounter` have been signalled.</li>
    *         </ul>
    */
  def tick(sequencerCounter: SequencerCounter, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit

  /** Adds the confirmation request to the request tracker.
    * It schedules the activeness check for the `activenessTime` and the timeout of the request for
    * the `decisionTime`, whereby a result exactly at the `decisionTime` is considered to be in time.
    * If a [[scala.Right$]] is returned, it also signals the arrival of the message with `sequencerCounter`
    * and timestamp `requestTimestamp`, like the [[RequestTracker!.tick]] operation.
    *
    * The activeness check request is expressed as an activeness set comprising all the contracts that must be active
    * and those that may be created or transferred-in.
    * This operation returns a pair of futures indicating the outcome of the activeness check
    * and the timeout state of the request, described below.
    *
    * If a request with `requestCounter` and `sequencerCounter` is added,
    * the request tracker expects that eventually every request prior to `requestCounter` will be added,
    * and that every sequencer counter prior to `sequencerCounter` will be signaled.
    *
    * If the request has been added before with the same parameters, but it is not yet finalized nor has it timed out,
    * the method ignores the second call and returns the same futures as in the first call.
    *
    * @param requestCounter The request counter. It must correspond to the sequencer counter and timestamp in the request journal.
    *           Must not be `Long.MaxValue`.
    * @param sequencerCounter The sequencer counter on the request. Must not be `Long.MaxValue`.
    * @param requestTimestamp The timestamp on the request.
    * @param activenessTime The timestamp when the activeness check should happen.
    *                       Must be at least `requestTimestamp` and less than the `decisionTime`.
    * @param decisionTime The timeout for the request. Must be after the `activenessTime`.
    * @param activenessSet The activeness set that determines the checks at the `activenessTime`.
    * @return [[scala.Right$]] if the request was added successfully or this is a duplicate call for the request.
    *         One future for the result of the activeness check and one for signalling the timeout state of a request.
    *         These futures complete only when their result is determined:
    *         The result of the activeness check when the request tracker has progressed to the `activenessTime`.
    *         The timeout state when the request tracker has progressed to the `decisionTime`
    *         or a transaction result was added for the request.
    *         [[scala.Left$]] if a request with the same `requestCounter` is in-flight with different parameters.
    * @throws java.lang.IllegalArgumentException
    *         <ul>
    *           <li>If the `requestTimestamp` or `sequencerCounter` is `Long.MaxValue`.</li>
    *           <li>If the `requestTimestamp` is earlier than to where the request tracking has already progressed</li>
    *           <li>If the `activenessTimestamp` is not between the `requestTimestamp` (inclusive)
    *             and the `decisionTime` (exclusive).</li>
    *         </ul>
    */
  def addRequest(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      requestTimestamp: CantonTimestamp,
      activenessTime: CantonTimestamp,
      decisionTime: CantonTimestamp,
      activenessSet: ActivenessSet,
  )(implicit
      traceContext: TraceContext
  ): Either[RequestAlreadyExists, FutureUnlessShutdown[RequestFutures]]

  /** Informs the request tracker that a result message has arrived for the given request.
    * This marks the request as not having timed out. The actual effect of the result must
    * be supplied via a subsequent call to [[RequestTracker!.addCommitSet]].
    * The request tracker may not progress beyond the `commitTime` until the commit set is provided.
    *
    * This method may be called only after a [[RequestTracker!.addRequest]] call for the same request.
    *
    * @param requestCounter The request counter of the request
    * @param sequencerCounter The sequencer counter on the result message.
    * @param resultTimestamp The timestamp of the result message.
    *                        Must be after the request's timestamp and at most the decision time.
    * @param commitTime The commit time, i.e., when the result takes effect
    *                   Must be no earlier than the result timestamp.
    * @return [[scala.Right$]] if the result was added successfully or this is a duplicate call for the request.
    *         The timeout future returned by the corresponding call to [[RequestTracker!.addRequest]] completes with [[RequestTracker$.NoTimeout]].
    *         [[scala.Left$]] if the result could not be added (see the [[RequestTracker$.ResultError]]s for the possible
    *         cases). The timestamp is nevertheless considered as observed.
    * @throws java.lang.IllegalArgumentException
    *         if the `commitTime` is before the `resultTimestamp`, or
    *         if the request is in-flight and the `resultTimestamp` is after its decision time.
    *         The timestamp is not observed in this case.
    */
  def addResult(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      resultTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Either[ResultError, Unit]

  /** Informs the request tracker of the effect of a request that is to be committed.
    * The effect is described by a [[CommitSet]], namely which contracts should be deactivated and which ones activated.
    *
    * This method may be called only after an [[addResult]] call for the same request.
    *
    * @param requestCounter The request counter of the request
    * @param commitSet The commit set to describe the committed effect of the request
    *                  A [[scala.util.Failure$]] indicates that result processing failed and no commit set can be provided.
    * @return A future to indicate whether the request was successfully finalized.
    *         When this future completes, the request's effect is persisted to the [[store.ActiveContractStore]]
    *         and the [[store.TransferStore]].
    *         The future fails with an exception if the commit set tries to activate or deactivate
    *         a contract that was not locked during the activeness check.
    *         Otherwise, activeness irregularities are reported as [[scala.Left$]].
    * @see ConflictDetector.finalizeRequest
    */
  def addCommitSet(requestCounter: RequestCounter, commitSet: Try[CommitSet])(implicit
      traceContext: TraceContext
  ): Either[CommitSetError, EitherT[FutureUnlessShutdown, NonEmptyChain[
    RequestTrackerStoreError
  ], Unit]]

  /** Returns a future that completes after the request has progressed to the given timestamp.
    * If the request tracker has already progressed to the timestamp, [[scala.None]] is returned.
    */
  def awaitTimestamp(timestamp: CantonTimestamp): Option[Future[Unit]]
}

trait RequestTrackerLookup extends AutoCloseable with NamedLogging {

  /** Returns a possibly outdated state of the contracts. */
  def getApproximateStates(coid: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, ContractState]]
}

object RequestTracker {

  /** Case class for the futures of [[RequestTracker.addRequest]].
    *
    * @param activenessResult The future for the [[ActivenessResult]]
    *                         that the activeness check completes with the activeness result
    * @param timeoutResult The future for the [[TimeoutResult]] that will be completed when the
    *                      request times out or a transaction result is added.
    */
  final case class RequestFutures(
      activenessResult: FutureUnlessShutdown[ActivenessResult],
      timeoutResult: FutureUnlessShutdown[TimeoutResult],
  )

  /** Indicates whether the request has timed out. */
  sealed trait TimeoutResult {
    def timedOut: Boolean
  }

  case object Timeout extends TimeoutResult {
    override def timedOut: Boolean = true
  }

  case object NoTimeout extends TimeoutResult {
    override def timedOut: Boolean = false
  }

  /** Trait for errors of the request tracker */
  sealed trait RequestTrackerError extends Product with Serializable with PrettyPrinting

  /** Returned by [[RequestTracker!.addRequest]] if the same request was added before with different
    * parameters (as given).
    */
  final case class RequestAlreadyExists(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  ) extends RequestTrackerError {
    override def pretty: Pretty[RequestAlreadyExists] = prettyOfClass(
      param("request counter", _.requestCounter),
      param("sequencer counter", _.sequencerCounter),
      param("timestamp", _.timestamp),
    )
  }

  /** Trait for errors that can occur for a result */
  sealed trait ResultError extends RequestTrackerError

  /** The request is not (any longer) tracked by the request tracker */
  final case class RequestNotFound(requestCounter: RequestCounter)
      extends ResultError
      with CommitSetError {
    override def pretty: Pretty[RequestNotFound] = prettyOfClass(unnamedParam(_.requestCounter))
  }

  /** Returned by [[RequestTracker!.addResult]] if the result has been signalled beforehand
    * with different parameters for the same request
    */
  final case class ResultAlreadyExists(requestCounter: RequestCounter) extends ResultError {
    override def pretty: Pretty[ResultAlreadyExists] = prettyOfClass(unnamedParam(_.requestCounter))
  }

  /** Trait for errors that can occur when adding a commit set */
  sealed trait CommitSetError extends RequestTrackerError

  /** Returned by [[RequestTracker!.addCommitSet]] if no result has been signalled beforehand for the
    * request
    */
  final case class ResultNotFound(requestCounter: RequestCounter) extends CommitSetError {
    override def pretty: Pretty[ResultNotFound] = prettyOfClass(unnamedParam(_.requestCounter))
  }

  /** Returned by [[RequestTracker!.addCommitSet]] if a different commit set has already been supplied
    * for the given request counter.
    */
  final case class CommitSetAlreadyExists(requestCounter: RequestCounter) extends CommitSetError {
    override def pretty: Pretty[CommitSetAlreadyExists] = prettyOfClass(
      unnamedParam(_.requestCounter)
    )
  }

  /** The commit set tries to activate or deactivate contracts
    * that were not locked during the activeness check.
    */
  final case class InvalidCommitSet(
      requestCounter: RequestCounter,
      commitSet: CommitSet,
      locked: LockedStates,
  ) extends RuntimeException(
        show"Commit set $commitSet for request $requestCounter is invalid. $locked"
      )

  /** Errors while writing to the stores */
  sealed trait RequestTrackerStoreError extends Product with Serializable

  final case class AcsError(error: ActiveContractStore.AcsBaseError)
      extends RequestTrackerStoreError

  final case class TransferStoreError(error: TransferStore.TransferStoreError)
      extends RequestTrackerStoreError
}
