// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsProvider
import com.digitalasset.canton.protocol.messages.AcsCommitment.CommitmentType
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  CommitmentPeriodState,
  SignedProtocolMessage,
}
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}

class ThrowOnWriteCommitmentStore()(override implicit val ec: ExecutionContext)
    extends AcsCommitmentStore {
  // Counts the number of write method invocations
  val writeCounter = new AtomicInteger(0)

  override def storeComputed(items: NonEmpty[Seq[AcsCommitmentStore.CommitmentData]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    incrementCounterAndErrUS()

  override def markOutstanding(period: CommitmentPeriod, counterParticipants: Set[ParticipantId])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    incrementCounterAndErrUS()

  override def markComputedAndSent(period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    incrementCounterAndErrUS()

  override def storeReceived(commitment: SignedProtocolMessage[AcsCommitment])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    incrementCounterAndErrUS()

  override def markPeriod(
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      matchingState: CommitmentPeriodState,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    incrementCounterAndErrUS()

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PruningStatus]] = FutureUnlessShutdown.pure(None)

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    incrementCounterAndErrUS()

  override protected[canton] def doPrune(
      limit: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Future[Int] =
    incrementCounterAndErrF()

  override def getComputed(period: CommitmentPeriod, counterParticipant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[(CommitmentPeriod, CommitmentType)]] =
    FutureUnlessShutdown.pure(Iterable.empty)

  override def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] =
    FutureUnlessShutdown.pure(None)

  override def noOutstandingCommitments(
      beforeOrAt: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure(None)

  override def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipants: Seq[ParticipantId],
      includeMatchedPeriods: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)]] =
    FutureUnlessShutdown.pure(Iterable.empty)

  override def searchComputedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[(CommitmentPeriod, ParticipantId, CommitmentType)]] =
    FutureUnlessShutdown.pure(Iterable.empty)

  override def searchReceivedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[SignedProtocolMessage[AcsCommitment]]] =
    FutureUnlessShutdown.pure(Iterable.empty)

  override val runningCommitments: IncrementalCommitmentStore =
    new ThrowOnWriteIncrementalCommitmentStore

  override val queue: CommitmentQueue = new ThrowOnWriteCommitmentQueue

  private def incrementCounterAndErrF[V](): Future[V] = {
    writeCounter.incrementAndGet().discard
    Future.failed(new RuntimeException("error"))
  }

  private def incrementCounterAndErrUS[V](): FutureUnlessShutdown[V] = {
    writeCounter.incrementAndGet().discard
    FutureUnlessShutdown.failed(new RuntimeException("error"))
  }

  class ThrowOnWriteIncrementalCommitmentStore extends IncrementalCommitmentStore {
    override def get()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])] =
      FutureUnlessShutdown.pure((RecordTime.MinValue, Map.empty))

    override def watermark(implicit traceContext: TraceContext): FutureUnlessShutdown[RecordTime] =
      FutureUnlessShutdown.pure(RecordTime.MinValue)

    override def update(
        rt: RecordTime,
        updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
        deletes: Set[SortedSet[LfPartyId]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
      incrementCounterAndErrUS()
  }

  class ThrowOnWriteCommitmentQueue extends CommitmentQueue {
    override def enqueue(commitment: AcsCommitment)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.outcomeF(incrementCounterAndErrF())

    override def peekThrough(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[AcsCommitment]] = FutureUnlessShutdown.pure(List.empty)

    override def peekThroughAtOrAfter(
        timestamp: CantonTimestamp
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[AcsCommitment]] =
      FutureUnlessShutdown.pure(List.empty)

    def peekOverlapsForCounterParticipant(
        period: CommitmentPeriod,
        counterParticipant: ParticipantId,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[AcsCommitment]] = FutureUnlessShutdown.pure(List.empty)

    override def deleteThrough(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = incrementCounterAndErrUS()
  }

  override def close(): Unit = ()

  override def acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore = ???
}
