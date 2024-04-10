// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsProvider
import com.digitalasset.canton.protocol.messages.AcsCommitment.CommitmentType
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{DiscardOps, LfPartyId}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}

class ThrowOnWriteCommitmentStore()(override implicit val ec: ExecutionContext)
    extends AcsCommitmentStore {
  // Counts the number of write method invocations
  val writeCounter = new AtomicInteger(0)

  override def storeComputed(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
      commitment: CommitmentType,
  )(implicit traceContext: TraceContext): Future[Unit] =
    incrementCounterAndErrF()

  override def markOutstanding(period: CommitmentPeriod, counterParticipants: Set[ParticipantId])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    incrementCounterAndErrF()

  override def markComputedAndSent(period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    incrementCounterAndErrF()

  override def storeReceived(commitment: SignedProtocolMessage[AcsCommitment])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    incrementCounterAndErrF()

  override def markSafe(
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
  )(implicit traceContext: TraceContext): Future[Unit] =
    incrementCounterAndErrF()

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): Future[Option[PruningStatus]] = Future.successful(None)

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    incrementCounterAndErrF()

  override protected[canton] def doPrune(
      limit: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Future[Int] =
    incrementCounterAndErrF()

  override def getComputed(period: CommitmentPeriod, counterParticipant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, CommitmentType)]] =
    Future.successful(Iterable.empty)

  override def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestampSecond]] =
    Future(None)

  override def noOutstandingCommitments(beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(None)

  override def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Future[Iterable[(CommitmentPeriod, ParticipantId)]] =
    Future.successful(Iterable.empty)

  override def searchComputedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, ParticipantId, CommitmentType)]] =
    Future.successful(Iterable.empty)

  override def searchReceivedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Future[Iterable[SignedProtocolMessage[AcsCommitment]]] =
    Future.successful(Iterable.empty)

  override val runningCommitments: IncrementalCommitmentStore =
    new ThrowOnWriteIncrementalCommitmentStore

  override val queue: CommitmentQueue = new ThrowOnWriteCommitmentQueue

  private def incrementCounterAndErrF[V](): Future[V] = {
    writeCounter.incrementAndGet().discard
    Future.failed(new RuntimeException("error"))
  }

  class ThrowOnWriteIncrementalCommitmentStore extends IncrementalCommitmentStore {
    override def get()(implicit
        traceContext: TraceContext
    ): Future[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])] =
      Future.successful((RecordTime.MinValue, Map.empty))

    override def watermark(implicit traceContext: TraceContext): Future[RecordTime] =
      Future.successful(RecordTime.MinValue)

    override def update(
        rt: RecordTime,
        updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
        deletes: Set[SortedSet[LfPartyId]],
    )(implicit traceContext: TraceContext): Future[Unit] =
      incrementCounterAndErrF()
  }

  class ThrowOnWriteCommitmentQueue extends CommitmentQueue {
    override def enqueue(commitment: AcsCommitment)(implicit
        traceContext: TraceContext
    ): Future[Unit] = incrementCounterAndErrF()

    override def peekThrough(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): Future[List[AcsCommitment]] = Future.successful(List.empty)

    override def peekThroughAtOrAfter(
        timestamp: CantonTimestamp
    )(implicit traceContext: TraceContext): Future[Seq[AcsCommitment]] =
      Future.successful(List.empty)

    def peekOverlapsForCounterParticipant(
        period: CommitmentPeriod,
        counterParticipant: ParticipantId,
    )(implicit
        traceContext: TraceContext
    ): Future[Seq[AcsCommitment]] = Future.successful(List.empty)

    override def deleteThrough(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): Future[Unit] = incrementCounterAndErrF()
  }

  override def close(): Unit = ()
}
