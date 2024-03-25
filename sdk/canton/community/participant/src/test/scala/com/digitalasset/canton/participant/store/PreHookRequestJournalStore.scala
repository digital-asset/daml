// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.store.CursorPrehead.RequestCounterCursorPrehead
import com.digitalasset.canton.store.CursorPreheadStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{RequestCounter, RequestCounterDiscriminator}
import org.testcontainers.shaded.com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class PreHookRequestJournalStore(
    val backing: RequestJournalStore,
    override protected val loggerFactory: NamedLoggerFactory,
) extends RequestJournalStore
    with NamedLogging {
  import PreHookRequestJournalStore.*

  override private[store] implicit val ec: ExecutionContext = backing.ec

  override private[store] val cleanPreheadStore: CursorPreheadStore[RequestCounterDiscriminator] =
    backing.cleanPreheadStore

  private val preInsertHook: AtomicReference[InsertHook] =
    new AtomicReference[InsertHook](emptyInsertHook)
  private val preReplaceHook: AtomicReference[ReplaceHook] =
    new AtomicReference[ReplaceHook](emptyReplaceHook)
  private val preCleanCounterHook: AtomicReference[CleanCounterHook] =
    new AtomicReference[CleanCounterHook](emptyCleanCounterHook)

  def setInsertHook(hook: InsertHook): Unit = preInsertHook.set(hook)

  def setReplaceHook(hook: ReplaceHook): Unit = preReplaceHook.set(hook)

  def setCleanCounterHook(hook: CleanCounterHook): Unit = preCleanCounterHook.set(hook)

  override def insert(data: RequestData)(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- preInsertHook.getAndSet(emptyInsertHook)(data)
      _ <- backing.insert(data)
    } yield ()

  override def query(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RequestData] =
    backing.query(rc)

  override def firstRequestWithCommitTimeAfter(commitTimeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[RequestData]] =
    backing.firstRequestWithCommitTimeAfter(commitTimeExclusive)

  override def replace(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): EitherT[Future, RequestJournalStoreError, Unit] =
    for {
      _ <- EitherT.liftF(
        preReplaceHook
          .getAndSet(emptyReplaceHook)(rc, requestTimestamp, newState, commitTime)
      )
      result <- backing.replace(rc, requestTimestamp, newState, commitTime)
    } yield result

  @VisibleForTesting
  private[store] override def pruneInternal(beforeAndIncluding: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    backing.pruneInternal(beforeAndIncluding)

  override def size(start: CantonTimestamp, end: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): Future[Int] = backing.size(start, end)

  override def advancePreheadCleanTo(
      newPrehead: RequestCounterCursorPrehead
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] =
    for {
      _ <- preCleanCounterHook.getAndSet(emptyCleanCounterHook)(newPrehead)
      _ <- backing.advancePreheadCleanTo(newPrehead)
    } yield ()

  override def deleteSince(fromInclusive: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    backing.deleteSince(fromInclusive)

  override def repairRequests(fromInclusive: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Seq[RequestData]] = backing.repairRequests(fromInclusive)
}

object PreHookRequestJournalStore {
  type InsertHook = RequestData => Future[Unit]

  type ReplaceHook =
    (
        RequestCounter,
        CantonTimestamp,
        RequestState,
        Option[CantonTimestamp],
    ) => Future[Unit]

  type CleanCounterHook = RequestCounterCursorPrehead => Future[Unit]

  val emptyInsertHook: InsertHook = _ => Future.unit

  val emptyReplaceHook: ReplaceHook = (_, _, _, _) => Future.unit

  val emptyCleanCounterHook: CleanCounterHook = _ => Future.unit
}
