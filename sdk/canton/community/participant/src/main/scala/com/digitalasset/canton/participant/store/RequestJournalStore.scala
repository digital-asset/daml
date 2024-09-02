// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.store.CursorPreheadStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{RequestCounter, RequestCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

trait RequestJournalStore { this: NamedLogging =>

  private[store] implicit def ec: ExecutionContext

  private[store] val cleanPreheadStore: CursorPreheadStore[RequestCounterDiscriminator]

  /** Adds the initial request information to the store.
    *
    * @return A failed future, if a request is inserted more than once with differing `data`
    */
  def insert(data: RequestData)(implicit traceContext: TraceContext): Future[Unit]

  /** Find request information by request counter */
  def query(rc: RequestCounter)(implicit traceContext: TraceContext): OptionT[Future, RequestData]

  /** Finds the request with the lowest request counter whose commit time is after the given timestamp */
  def firstRequestWithCommitTimeAfter(commitTimeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[RequestData]]

  /** Finds the highest request counter with request time before or equal to the given timestamp */
  def lastRequestCounterWithRequestTimestampBeforeOrAt(requestTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[RequestCounter]]

  /** Replaces the state of the request.
    * The operation will only succeed if the current state is equal to the given `oldState`
    * and the provided `requestTimestamp` matches the stored timestamp,
    * or if the current state is already the new state.
    * If so, the state gets replaced with `newState` and `commitTime`.
    * If `commitTime` is [[scala.None$]], the commit time will not be modified.
    *
    * The returned future may fail with a [[java.util.ConcurrentModificationException]]
    * if the store detects a concurrent modification.
    *
    * @param requestTimestamp The sequencing time of the request.
    */
  def replace(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): EitherT[Future, RequestJournalStoreError, Unit]

  /** Deletes all request counters at or before the given timestamp.
    * Calls to this method are idempotent, independent of the order.
    *
    * @param beforeInclusive inclusive timestamp to prune up to
    */
  def prune(
      beforeInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
    pruneInternal(beforeInclusive)

  /** Deletes all request counters at or before the given timestamp.
    * Calls to this method are idempotent, independent of the order.
    */
  @VisibleForTesting
  private[store] def pruneInternal(beforeInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Counts requests whose timestamps lie between the given timestamps (inclusive).
    *
    * @param start Count all requests after or at the given timestamp
    * @param end   Count all requests before or at the given timestamp; use None to impose no upper limit
    */
  def size(start: CantonTimestamp = CantonTimestamp.Epoch, end: Option[CantonTimestamp] = None)(
      implicit traceContext: TraceContext
  ): Future[Int]

  /** Deletes all the requests with a request counter equal to or higher than the given request counter. */
  def deleteSince(fromInclusive: RequestCounter)(implicit traceContext: TraceContext): Future[Unit]

  /** Returns all repair requests at or after `fromInclusive` in ascending order.
    * This method must not be called concurrently with other methods of the store.
    */
  def repairRequests(fromInclusive: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Seq[RequestData]]

  /** Returns the number of dirty requests.
    */
  def totalDirtyRequests()(implicit traceContext: TraceContext): Future[Int]
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
