// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.store.CursorPrehead.RequestCounterCursorPrehead
import com.digitalasset.canton.store.CursorPreheadStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
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
    * @param beforeAndIncluding inclusive timestamp to prune up to
    * @param bypassAllSanityChecks force if true, bypass all pre-condition sanity checks
    *
    * Pre-conditions for the call unless `bypassAllSanityChecks` is true:
    *   1. there must be a timestamp `ts` associated with the clean head
    *   2. beforeAndIncluding < `ts`
    * @throws java.lang.IllegalArgumentException if the preconditions are violated.
    */
  def prune(
      beforeAndIncluding: CantonTimestamp,
      bypassAllSanityChecks: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    def checkCleanHead(): Future[Unit] = for {
      cleanHead <- OptionT(preheadClean)
        .getOrElseF(
          ErrorUtil.internalErrorAsync(
            new IllegalArgumentException("Attempted to prune a journal with no clean timestamps")
          )
        )
      _ = ErrorUtil.requireArgument(
        cleanHead.timestamp > beforeAndIncluding,
        s"Attempted to prune at timestamp $beforeAndIncluding which is not earlier than ${cleanHead.timestamp} associated with the clean head",
      )
    } yield ()

    for {
      _ <- if (!bypassAllSanityChecks) checkCleanHead() else Future.unit
      result <- pruneInternal(beforeAndIncluding)
    } yield result
  }

  /** Deletes all request counters at or before the given timestamp.
    * Calls to this method are idempotent, independent of the order.
    */
  @VisibleForTesting
  private[store] def pruneInternal(beforeAndIncluding: CantonTimestamp)(implicit
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

  /** Gets the prehead for the clean cursor. */
  def preheadClean(implicit
      traceContext: TraceContext
  ): Future[Option[RequestCounterCursorPrehead]] =
    cleanPreheadStore.prehead

  /** Forces an update to the prehead for clean requests. Only use this for testing. */
  @VisibleForTesting
  private[participant] def overridePreheadCleanForTesting(
      rc: Option[RequestCounterCursorPrehead]
  )(implicit traceContext: TraceContext): Future[Unit] = cleanPreheadStore.overridePreheadUnsafe(rc)

  /** Sets the prehead counter for clean requests to `rc` with timestamp `timestamp`
    * unless it has previously been set to the same or a higher value.
    */
  def advancePreheadCleanTo(newPrehead: RequestCounterCursorPrehead)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] =
    cleanPreheadStore.advancePreheadTo(newPrehead)

  /** [[advancePreheadCleanTo]] as a [[com.digitalasset.canton.resource.TransactionalStoreUpdate]] */
  def advancePreheadCleanToTransactionalUpdate(
      newPrehead: RequestCounterCursorPrehead
  )(implicit
      traceContext: TraceContext
  ): TransactionalStoreUpdate =
    cleanPreheadStore.advancePreheadToTransactionalStoreUpdate(newPrehead)

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
