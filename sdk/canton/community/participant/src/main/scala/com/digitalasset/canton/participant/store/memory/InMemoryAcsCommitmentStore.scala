// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.store.AcsCommitmentStore.CommitmentData
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  AcsCounterParticipantConfigStore,
  CommitmentQueue,
  IncrementalCommitmentStore,
}
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  CommitmentPeriodState,
  SignedProtocolMessage,
}
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.IterableUtil.Ops

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

class InMemoryAcsCommitmentStore(
    domainId: DomainId,
    override val acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends AcsCommitmentStore
    with InMemoryPrunableByTime
    with NamedLogging {

  private val computed
      : TrieMap[ParticipantId, Map[CommitmentPeriod, AcsCommitment.CommitmentType]] = TrieMap.empty

  private val received: TrieMap[ParticipantId, Set[SignedProtocolMessage[AcsCommitment]]] =
    TrieMap.empty

  private val lastComputed: AtomicReference[Option[CantonTimestampSecond]] =
    new AtomicReference(None)

  private val _outstanding
      : AtomicReference[Set[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)]] =
    new AtomicReference(Set.empty)

  override val runningCommitments =
    new InMemoryIncrementalCommitments(RecordTime.MinValue, Map.empty)

  override val queue = new InMemoryCommitmentQueue

  override def storeComputed(
      items: NonEmpty[Seq[AcsCommitmentStore.CommitmentData]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    blocking {
      computed.synchronized {
        items.toList.foreach { item =>
          val CommitmentData(counterParticipant, period, commitment) = item
          val oldMap = computed.getOrElse(counterParticipant, Map.empty)
          val oldCommitment = oldMap.getOrElse(period, commitment)
          if (oldCommitment != commitment) {
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Trying to store $commitment for $period and counter-participant $counterParticipant, but $oldCommitment is already stored"
              )
            )
          } else {
            computed.update(counterParticipant, oldMap + (period -> commitment))
          }
        }
      }
    }
    FutureUnlessShutdown.unit
  }

  override def getComputed(period: CommitmentPeriod, counterParticipant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[(CommitmentPeriod, AcsCommitment.CommitmentType)]] =
    FutureUnlessShutdown.pure(
      for {
        m <- computed.get(counterParticipant).toList
        commitments <- m
          .filter(_._1.overlaps(period))
          .toList
      } yield commitments
    )

  override def storeReceived(
      commitment: SignedProtocolMessage[AcsCommitment]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    blocking {
      received.synchronized {
        val sender = commitment.message.sender
        val old = received.getOrElse(sender, Set.empty)
        received.update(sender, old + commitment)
      }
    }

    FutureUnlessShutdown.unit
  }

  override def markOutstanding(
      periods: NonEmpty[Set[CommitmentPeriod]],
      counterParticipants: NonEmpty[Set[ParticipantId]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    if (counterParticipants.nonEmpty) {
      _outstanding.updateAndGet(os =>
        os ++ periods.forgetNE.crossProductBy(counterParticipants).map {
          case (period, participant) =>
            (period, participant, CommitmentPeriodState.Outstanding)
        }
      )
    }
    FutureUnlessShutdown.unit
  }

  override def markComputedAndSent(
      period: CommitmentPeriod
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val timestamp = period.toInclusive
    lastComputed.set(Some(timestamp))
    FutureUnlessShutdown.unit
  }

  override def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] =
    FutureUnlessShutdown.pure(lastComputed.get())

  private def updateStateIfPossible(
      currentState: CommitmentPeriodState,
      newState: CommitmentPeriodState,
  ): CommitmentPeriodState =
    if (currentState == CommitmentPeriodState.Outstanding) newState
    else if (
      currentState == CommitmentPeriodState.Mismatched && newState == CommitmentPeriodState.Matched
    ) newState
    else currentState

  override def markPeriod(
      counterParticipant: ParticipantId,
      periods: NonEmpty[Set[CommitmentPeriod]],
      matchingState: CommitmentPeriodState,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    _outstanding.updateAndGet { currentOutstanding =>
      currentOutstanding.map { case (currentPeriod, currentCounterParticipant, currentState) =>
        if (periods.contains(currentPeriod) && currentCounterParticipant == counterParticipant)
          (
            currentPeriod,
            currentCounterParticipant,
            updateStateIfPossible(currentState, matchingState),
          )
        else
          (currentPeriod, currentCounterParticipant, currentState)
      }
    }
    FutureUnlessShutdown.unit
  }

  override def noOutstandingCommitments(
      beforeOrAt: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[CantonTimestamp]] =
    for {
      ignoredParticipants <- acsCounterParticipantConfigStore
        .getAllActiveNoWaitCounterParticipants(Seq(domainId), Seq.empty)
      result <- FutureUnlessShutdown.pure {
        for {
          lastTs <- lastComputed.get
          adjustedTs = lastTs.forgetRefinement.min(beforeOrAt)

          periods = _outstanding
            .get()
            .collect {
              case (period, participantId, state)
                  if state != CommitmentPeriodState.Matched &&
                    !ignoredParticipants
                      .exists(config => config.participantId == participantId) =>
                period.fromExclusive.forgetRefinement -> period.toInclusive.forgetRefinement
            }
          safe = AcsCommitmentStore.latestCleanPeriod(
            beforeOrAt = adjustedTs,
            uncleanPeriods = periods,
          )
        } yield safe
      }
    } yield result

  override def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Seq[ParticipantId],
      includeMatchedPeriods: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)]] =
    FutureUnlessShutdown.pure(_outstanding.get.filter { case (period, participant, state) =>
      (counterParticipant.isEmpty ||
        counterParticipant.contains(participant)) &&
      period.fromExclusive < end &&
      period.toInclusive >= start &&
      (includeMatchedPeriods || state != CommitmentPeriodState.Matched)
    })

  override def searchComputedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Seq[ParticipantId] = Seq.empty,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)]
  ] = {
    val filteredByCounterParty =
      if (counterParticipant.isEmpty) computed
      else computed.filter(c => counterParticipant.contains(c._1))

    FutureUnlessShutdown.pure(
      filteredByCounterParty.flatMap { case (p, m) =>
        LazyList
          .continually(p)
          .lazyZip(m.filter { case (period, _) =>
            start <= period.toInclusive && period.fromExclusive < end
          })
          .map { case (p, (period, cmt)) =>
            (period, p, cmt)
          }
      }
    )
  }

  override def searchReceivedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Seq[ParticipantId] = Seq.empty,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[SignedProtocolMessage[AcsCommitment]]] = {
    val filteredByCounterParty = (if (counterParticipant.isEmpty) received
                                  else
                                    received.filter(c => counterParticipant.contains(c._1))).values

    FutureUnlessShutdown.pure(
      filteredByCounterParty.flatMap(msgs =>
        msgs.filter(msg =>
          start <= msg.message.period.toInclusive && msg.message.period.fromExclusive < end
        )
      )
    )

  }

  override def doPrune(
      before: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] =
    Future.successful {
      val counter = new AtomicInteger(0)
      def count(res: Boolean): Boolean = { if (!res) counter.incrementAndGet(); res }
      computed.foreach { case (p, periods) =>
        computed.update(
          p,
          periods.filter { case (commitmentPeriod, _) =>
            count(commitmentPeriod.toInclusive >= before)
          },
        )
      }
      received.foreach { case (p, commitmentMsgs) =>
        received.update(
          p,
          commitmentMsgs.filter(x => count(x.message.period.toInclusive >= before)),
        )
      }
      _outstanding.updateAndGet { currentOutstanding =>
        val newOutstanding = currentOutstanding.filter {
          case (period, _counterParticipant, state) =>
            period.toInclusive >= before
        }
        counter.addAndGet(currentOutstanding.size - newOutstanding.size)
        newOutstanding
      }
      counter.get()
    }

  override def close(): Unit = ()
}

/* An in-memory, mutable running ACS snapshot */
class InMemoryIncrementalCommitments(
    initialRt: RecordTime,
    initialHashes: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
) extends IncrementalCommitmentStore {
  private val snap: TrieMap[SortedSet[LfPartyId], AcsCommitment.CommitmentType] = TrieMap.empty
  snap ++= initialHashes

  private val rt: AtomicReference[RecordTime] = new AtomicReference(initialRt)

  private object lock

  /** Update the snapshot */
  private def update_(
      rt: RecordTime,
      updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deletes: Set[SortedSet[LfPartyId]],
  ): Unit = blocking {
    lock.synchronized {
      this.rt.set(rt)
      snap --= deletes
      snap ++= updates
      ()
    }
  }

  private def watermark_(): RecordTime = rt.get()

  /** A read-only version of the snapshot.
    */
  def snapshot: TrieMap[SortedSet[LfPartyId], AcsCommitment.CommitmentType] = snap.snapshot()

  override def get()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])] = {
    val rt = watermark_()
    FutureUnlessShutdown.pure((rt, snapshot.toMap))
  }

  override def update(
      rt: RecordTime,
      updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deletes: Set[SortedSet[LfPartyId]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(update_(rt, updates, deletes))

  override def watermark(implicit traceContext: TraceContext): FutureUnlessShutdown[RecordTime] =
    FutureUnlessShutdown.pure(watermark_())
}

class InMemoryCommitmentQueue(implicit val ec: ExecutionContext) extends CommitmentQueue {

  import InMemoryCommitmentQueue.*

  /* Access must be synchronized, since PriorityQueue doesn't support concurrent
    modifications. */
  private val queue: mutable.PriorityQueue[AcsCommitment] =
    // Queues dequeue in max-first, so make the lowest timestamp the maximum
    mutable.PriorityQueue.empty(
      Ordering.by[AcsCommitment, CantonTimestampSecond](cmt => cmt.period.toInclusive).reverse
    )

  private object lock

  private def syncUS[T](v: => T): FutureUnlessShutdown[T] = {
    val evaluated = blocking(lock.synchronized(v))
    FutureUnlessShutdown.pure(evaluated)
  }

  override def enqueue(
      commitment: AcsCommitment
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = syncUS {
    queue.enqueue(commitment)
  }

  /** Returns all commitments whose period ends at or before the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThrough(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[List[AcsCommitment]] = syncUS {
    queue.takeWhile(_.period.toInclusive <= timestamp).toList
  }

  /** Returns all commitments whose period ends at or after the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThroughAtOrAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[AcsCommitment]] =
    syncUS {
      queue.filter(_.period.toInclusive >= timestamp).toSeq
    }

  def peekOverlapsForCounterParticipant(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[AcsCommitment]] =
    syncUS {
      queue
        .filter(_.period.overlaps(period))
        .filter(_.sender == counterParticipant)
        .toSeq
    }

  /** Deletes all commitments whose period ends at or before the given timestamp. */
  override def deleteThrough(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = syncUS {
    deleteWhile(queue)(_.period.toInclusive <= timestamp)
  }
}

object InMemoryCommitmentQueue {
  private def deleteWhile[A](q: mutable.PriorityQueue[A])(p: A => Boolean): Unit = {
    @tailrec
    def go(): Unit =
      q.headOption match {
        case None => ()
        case Some(hd) =>
          if (p(hd)) {
            q.dequeue().discard
            go()
          } else ()
      }
    go()
  }
}
