// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.util.TimeOfRequest
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

trait RequestJournalStore { this: NamedLogging =>

  private[store] implicit def ec: ExecutionContext

  /** Adds the initial request information to the store.
    *
    * @return
    *   A failed future, if a request is inserted more than once with differing `data`
    */
  def insert(data: RequestData)(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Find request information by request counter */
  def query(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, RequestData]

  /** Finds the request with the lowest request counter whose commit time is after the given
    * timestamp
    */
  def firstRequestWithCommitTimeAfter(commitTimeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RequestData]]

  /** Finds the highest request time before or equal to the given timestamp */
  def lastRequestTimeWithRequestTimestampBeforeOrAt(requestTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TimeOfRequest]]

  /** Replaces the state of the request. The operation will only succeed if the current state is
    * equal to the given `oldState` and the provided `requestTimestamp` matches the stored
    * timestamp, or if the current state is already the new state. If so, the state gets replaced
    * with `newState` and `commitTime`. If `commitTime` is [[scala.None$]], the commit time will not
    * be modified.
    *
    * The returned future may fail with a [[java.util.ConcurrentModificationException]] if the store
    * detects a concurrent modification.
    *
    * @param requestTimestamp
    *   The sequencing time of the request.
    */
  def replace(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RequestJournalStoreError, Unit]

  /** Deletes all request counters at or before the given timestamp. Calls to this method are
    * idempotent, independent of the order.
    *
    * @param beforeInclusive
    *   inclusive timestamp to prune up to
    */
  def prune(
      beforeInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    pruneInternal(beforeInclusive)

  /** Purges all data from the request journal.
    */
  def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Deletes all request counters at or before the given timestamp. Calls to this method are
    * idempotent, independent of the order.
    */
  @VisibleForTesting
  private[store] def pruneInternal(beforeInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Counts requests whose timestamps lie between the given timestamps (inclusive).
    *
    * @param start
    *   Count all requests after or at the given timestamp
    * @param end
    *   Count all requests before or at the given timestamp; use None to impose no upper limit
    */
  def size(start: CantonTimestamp = CantonTimestamp.Epoch, end: Option[CantonTimestamp] = None)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Int]

  /** Deletes all the requests with a request timestamp equal to or higher than the given request
    * timestamp.
    */
  def deleteSinceRequestTimestamp(fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Returns the number of dirty requests.
    */
  def totalDirtyRequests()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[NonNegativeInt]

  /** Returns an upper bound for the timestamps up to which pruning may remove data from the store
    * (inclusive) so that crash recovery will still work.
    */
  final def crashRecoveryPruningBoundInclusive(cleanSynchronizerIndexO: Option[SynchronizerIndex])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    // Crash recovery cleans up the store before replay starts,
    // however we may have used some of the deleted information to determine the starting points for the replay.
    // So if a crash occurs during crash recovery, we may start again and come up with an earlier processing starting point.
    // We want to make sure that crash recovery access only data whose timestamps comes after what pruning is allowed to delete.
    // This method returns a timestamp that is before the data that crash recovery accesses after any number of iterating
    // the computation of starting points and crash recovery clean-ups.
    //
    // The earliest possible starting point is the earlier of the following:
    // * The first request whose commit time is after the clean synchronizer index timestamp
    // * The clean sequencer counter prehead timestamp
    for {
      cleanTimeOfRequest <- cleanSynchronizerIndexO
        .fold[FutureUnlessShutdown[Option[TimeOfRequest]]](FutureUnlessShutdown.pure(None))(
          synchronizerIndex =>
            lastRequestTimeWithRequestTimestampBeforeOrAt(synchronizerIndex.recordTime)
        )
      requestReplayTs <- cleanTimeOfRequest match {
        case None =>
          // No request is known to be clean, nothing can be pruned
          FutureUnlessShutdown.pure(CantonTimestamp.MinValue)
        case Some(timeOfRequest) =>
          firstRequestWithCommitTimeAfter(timeOfRequest.timestamp).map { res =>
            val ts = res.fold(timeOfRequest.timestamp)(_.requestTimestamp)
            // If the only processed requests so far are repair requests, it can happen that `ts == CantonTimestamp.MinValue`.
            // Taking the predecessor throws an exception.
            if (ts == CantonTimestamp.MinValue) ts else ts.immediatePredecessor
          }
      }
      // TODO(i21246): Note for unifying crashRecoveryPruningBoundInclusive and startingPoints: This minimum building is not needed anymore, as the request timestamp is also smaller than the sequencer timestamp.
      cleanSequencerIndexTs = cleanSynchronizerIndexO
        .flatMap(_.sequencerIndex)
        .fold(CantonTimestamp.MinValue)(_.sequencerTimestamp.immediatePredecessor)
    } yield requestReplayTs.min(cleanSequencerIndexTs)

}

sealed trait RequestJournalStoreError extends Product with Serializable

final case class UnknownRequestCounter(requestCounter: RequestCounter)
    extends RequestJournalStoreError
final case class CommitTimeBeforeRequestTime(
    requestCounter: RequestCounter,
    requestTime: CantonTimestamp,
    commitTime: CantonTimestamp,
) extends RequestJournalStoreError
final case class InconsistentRequestTimestamp(
    requestCounter: RequestCounter,
    storedTimestamp: CantonTimestamp,
    expectedTimestamp: CantonTimestamp,
) extends RequestJournalStoreError
