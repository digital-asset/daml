// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsProvider
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.store.PrunableByTime
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.util.control.Breaks.*

/** Read and write interface for ACS commitments. Apart from pruning, should only be used by the ACS commitment processor */
trait AcsCommitmentStore extends AcsCommitmentLookup with PrunableByTime with AutoCloseable {

  override protected def kind: String = "acs commitments"

  /** Store a locally computed ACS commitment. To be called by the ACS commitment processor only.
    *
    * If the method is called twice with the same period and counter participant, then the supplied
    * commitments must be the same too. Otherwise, the future fails.
    *
    * The implementation is guaranteed to be idempotent: calling it twice with the same argument
    * doesn't change the system's behavior compared to calling it only once.
    */
  def storeComputed(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
      commitment: AcsCommitment.CommitmentType,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Mark that remote commitments are outstanding for a period */
  def markOutstanding(period: CommitmentPeriod, counterParticipants: Set[ParticipantId])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Marks a period as processed and thus its end as a safe point for crash-recovery.
    *
    * To be called by the ACS commitment processor only.
    *
    * The period must be after the time point returned by [[lastComputedAndSent]].
    */
  def markComputedAndSent(period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Store a received ACS commitment. To be called by the ACS commitment processor only.
    *
    * The implementation is guaranteed to be idempotent: calling it twice with the same argument
    * doesn't change the store's behavior compared to calling it only once.
    *
    * The callers are free to insert multiple different commitments for the same commitment period; all of them
    * will be stored (but will be deduplicated). This can be used in case the commitment sender is malicious or buggy,
    * and sends both a correct and an incorrect commitment for the same time period. The caller can still store both
    * commitments, for example, such that it can later prove to a third party that the sender sent an incorrect
    * commitment.
    */
  def storeReceived(commitment: SignedProtocolMessage[AcsCommitment])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Mark a period as safe for a counterparticipant. To be called by the ACS commitment processor only.
    *
    * "Safe" here means that the received commitment matches the locally computed commitment.
    * The `toInclusive` field of the period must not be higher than that of the last period passed to
    * [[markComputedAndSent]].
    *
    * May be called with the same parameters again, after a restart or a domain reconnect.
    *
    * Marking a period as safe may change the result of calling [[outstanding]].
    */
  def markSafe(
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
  )(implicit traceContext: TraceContext): Future[Unit]

  val runningCommitments: IncrementalCommitmentStore

  val queue: CommitmentQueue

}

/** Read interface for ACS commitments, with no usage restrictions. */
trait AcsCommitmentLookup {

  /** Finds for a counter participant all stored computed commitments whose period overlaps with the given period.
    *
    *      No guarantees on the order of the returned commitments.
    */
  def getComputed(period: CommitmentPeriod, counterParticipant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, AcsCommitment.CommitmentType)]]

  /** Last locally processed timestamp.
    *
    *      Upon crash-recovery, it is safe to resubscribe to the sequencer starting after the returned timestamp.
    */
  def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestampSecond]]

  /** The latest timestamp before or at the given timestamp for which no commitments are outstanding.
    * It is safe to prune the domain at the returned timestamp as long as it is not before the last timestamp needed
    * for crash recovery (see [[com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.safeToPrune]])
    *
    * Returns None if no such tick is known.
    */
  def noOutstandingCommitments(beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]

  /** Inspection: find periods for which commitments are still outstanding, and from whom.
    *
    *    The returned periods may overlap.
    */
  def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Future[Iterable[(CommitmentPeriod, ParticipantId)]]

  /** Inspection: search computed commitments applicable to the specified period (start is exclusive, end is inclusive) */
  def searchComputedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)]]

  /** Inspection: search received commitments applicable to the specified period (start is exclusive, end is inclusive) */
  def searchReceivedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit traceContext: TraceContext): Future[Iterable[SignedProtocolMessage[AcsCommitment]]]

}

/** A key-value store with sets of parties as keys, and with LtHash16 values. Keeps a watermark of the record time
  * (a timestamp accompanied by a tie-breaker, to account for multiple changes with the same timestamp) of the last update.
  *
  * While the store is agnostic to its use, we use is as follows. For each set S of parties such that:
  * <ol>
  *  <li>the parties are stakeholders on some contract C and</li>
  *  <li>the participant stores C in its ACS</li>
  * </ol>
  * the participant uses the store to store an LtHash16 commitment to all the contracts whose stakeholders are exactly S.
  *
  * To ensure that the commitments correspond to the ACSs, the caller(s) must jointly ensure that all ACS changes
  * are delivered to this store exactly once. In particular, upon crashes, the caller(s) must send ALL ACS changes
  * that are later - in the lexicographic order of (timestamp, request) - than the watermark returned by the store,
  * but must not replay any changes lower or equal to the watermark.
  */
trait IncrementalCommitmentStore {

  /** Retrieve the current store.
    *
    * Defaults to an empty map with a record time of
    * [[com.digitalasset.canton.participant.event.RecordTime.MinValue]], if no changes have been added yet.
    */
  def get()(implicit
      traceContext: TraceContext
  ): Future[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])]

  /** Return the record time of the latest update.
    *
    * Defaults to [[com.digitalasset.canton.participant.event.RecordTime.MinValue]]
    * if no changes have been added yet.
    */
  def watermark(implicit traceContext: TraceContext): Future[RecordTime]

  /** Update the commitments.
    *
    * @param rt         Record time of the update
    * @param updates    The key-value updates to be written. The key set must be disjoint from `deletes` (not checked)
    * @param deletes    Keys to be deleted to the store.
    */
  def update(
      rt: RecordTime,
      updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deletes: Set[SortedSet[LfPartyId]],
  )(implicit traceContext: TraceContext): Future[Unit]

}

/** Manages the buffer (priority queue) for incoming commitments.
  *
  * The priority is based on the timestamp of the end of the commitment's commitment period.
  * Lower timestamps have higher priority.
  */
trait CommitmentQueue {

  def enqueue(commitment: AcsCommitment)(implicit traceContext: TraceContext): Future[Unit]

  /** Returns an unordered list of commitments whose period ends at or before the given timestamp.
    *
    * Does not delete them from the queue.
    */
  def peekThrough(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[List[AcsCommitment]]

  /** Returns an unordered list of commitments whose period ends at or after the given timestamp.
    *
    * Does not delete them from the queue.
    */
  def peekThroughAtOrAfter(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[AcsCommitment]]

  /** Returns, if exists, a list containing all commitments originating from the given participant
    * that overlap the given period.
    * Does not delete them from the queue.
    *
    * When the period covers a single reconciliation interval, there should be only one such commitment
    * if the counter participant is correct; otherwise, the counter participant is malicious.
    * The method is unaware of reconciliation intervals, however, thus cannot distinguish malicious behavior,
    * and leaves this up to the caller.
    * The caller (who has access to the reconciliation interval duration) should signal malicious behavior by emitting
    * an AcsCommitmentAlarm if the list contains more than one element.
    * Even with possible malicious behavior, the list of commitments might still be useful to the caller, for example
    * if the list contains the expected commitment among all unexpected ones.
    *
    * However, the period passed to the method can cover several reconciliation intervals (e.g., when the caller
    * participant hasn't heard from the sequencer in a while, and thus did not observe intermediate ticks).
    * But the counter participant might have sent commitments for several reconciliation intervals in the period.
    * Thus, in this case, the method returns a list and there is no malicious behavior.
    */
  def peekOverlapsForCounterParticipant(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[AcsCommitment]]

  /** Deletes all commitments whose period ends at or before the given timestamp. */
  def deleteThrough(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Future[Unit]
}

object AcsCommitmentStore {

  /** Given a timestamp and a list of "unclean" periods, return the latest "clean" timestamp before or at the given one.
    *
    * A clean timestamp is one that is not covered by the unclean periods. The periods are given as pairs of
    * `(startExclusive, endInclusive)` timestamps.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def latestCleanPeriod(
      beforeOrAt: CantonTimestamp,
      uncleanPeriods: Iterable[(CantonTimestamp, CantonTimestamp)],
  ): CantonTimestamp = {
    val descendingPeriods = uncleanPeriods.toSeq.sortWith(_._2 > _._2)

    var startingClean = beforeOrAt
    breakable {
      for (p <- descendingPeriods) {
        if (p._2 < startingClean)
          break()
        if (p._1 < startingClean)
          startingClean = p._1
      }
    }
    startingClean
  }

}
