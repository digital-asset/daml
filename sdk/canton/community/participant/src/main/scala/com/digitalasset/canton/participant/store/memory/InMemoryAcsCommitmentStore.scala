// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.{
  SortedReconciliationIntervals,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.AcsCommitmentStore.CommitmentData
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
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
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import pprint.Tree

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

class InMemoryAcsCommitmentStore(protected val loggerFactory: NamedLoggerFactory)(implicit
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
  )(implicit traceContext: TraceContext): Future[Unit] = {
    blocking {
      computed.synchronized {
        items.toList.foreach { case item =>
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
    Future.unit
  }

  override def getComputed(period: CommitmentPeriod, counterParticipant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[List[(CommitmentPeriod, AcsCommitment.CommitmentType)]] =
    Future.successful(
      for {
        m <- computed.get(counterParticipant).toList
        commitments <- m
          .filter(_._1.overlaps(period))
          .toList
      } yield commitments
    )

  override def storeReceived(
      commitment: SignedProtocolMessage[AcsCommitment]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    blocking {
      received.synchronized {
        val sender = commitment.message.sender
        val old = received.getOrElse(sender, Set.empty)
        received.update(sender, old + commitment)
      }
    }

    Future.unit
  }

  override def markOutstanding(period: CommitmentPeriod, counterParticipants: Set[ParticipantId])(
      implicit traceContext: TraceContext
  ): Future[Unit] = {
    if (counterParticipants.nonEmpty) {
      _outstanding.updateAndGet(os =>
        os ++ counterParticipants.map(cp => (period, cp, CommitmentPeriodState.Outstanding))
      )
    }
    Future.unit
  }

  override def markComputedAndSent(
      period: CommitmentPeriod
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val timestamp = period.toInclusive
    lastComputed.set(Some(timestamp))
    Future.unit
  }

  override def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestampSecond]] =
    Future.successful(lastComputed.get())

  private def computeOutstanding(
      counterParticipant: ParticipantId,
      safePeriod: CommitmentPeriod,
      currentOutstanding: Set[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)],
      sortedReconciliationIntervals: SortedReconciliationIntervals,
      matchingState: CommitmentPeriodState,
  )(implicit
      traceContext: TraceContext
  ): Set[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)] = {
    def stateFilter(state: CommitmentPeriodState): Boolean =
      if (matchingState == CommitmentPeriodState.Matched)
        state == CommitmentPeriodState.Matched || state == CommitmentPeriodState.Mismatched ||
        state == CommitmentPeriodState.Outstanding
      else if (matchingState == CommitmentPeriodState.Mismatched)
        state == CommitmentPeriodState.Mismatched ||
        state == CommitmentPeriodState.Outstanding
      else
        state == CommitmentPeriodState.Outstanding

    val oldPeriods = currentOutstanding.filter { case (oldPeriod, participant, state) =>
      oldPeriod.overlaps(
        safePeriod
      ) && participant == counterParticipant && stateFilter(state)
    }

    def containsTick(commitmentPeriod: CommitmentPeriod): Boolean = sortedReconciliationIntervals
      .containsTick(
        commitmentPeriod.fromExclusive.forgetRefinement,
        commitmentPeriod.toInclusive.forgetRefinement,
      )
      .getOrElse {
        logger.warn(s"Unable to determine whether $commitmentPeriod contains a tick.")

        // We default to a safe value: the commitment period will not be marked as safe.
        true
      }

    val periodsToAdd: Set[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)] = oldPeriods
      .flatMap { case (oldPeriod, _, oldState) =>
        Set(
          (
            CommitmentPeriod.create(oldPeriod.fromExclusive, safePeriod.fromExclusive),
            oldState,
          ),
          (
            CommitmentPeriod.create(safePeriod.toInclusive, oldPeriod.toInclusive),
            oldState,
          ),
          (
            CommitmentPeriod.create(safePeriod.fromExclusive, safePeriod.toInclusive),
            matchingState,
          ),
        )
      }
      .collect {
        case (Right(commitmentPeriod), state) if containsTick(commitmentPeriod) =>
          (commitmentPeriod, counterParticipant, state)
      }

    val newPeriods = currentOutstanding.diff(oldPeriods).union(periodsToAdd)

    import com.digitalasset.canton.logging.pretty.Pretty.*
    def prettyNewPeriods = newPeriods.map { case (period, participants, state) =>
      Tree.Infix(participants.toTree, "-", Tree.Infix(period.toTree, "-", state.toTree))
    }
    logger.debug(
      show"Marked period $safePeriod safe for participant $counterParticipant; new outstanding commitment periods: $prettyNewPeriods"
    )
    newPeriods
  }

  override def markPeriod(
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      matchingState: CommitmentPeriodState,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val approxInterval = sortedReconciliationIntervalsProvider.approximateReconciliationIntervals

    val intervals = approxInterval
      .flatMap(
        // the domain parameters at the approximate topology timestamp is recent enough for the period
        interval =>
          if (interval.validUntil >= period.toInclusive.forgetRefinement) approxInterval
          else
            // it is safe to wait for the topology timestamp period.toInclusive.forgetRefinement because we validate
            // that it is before the sequencing timestamp when we process incoming commitments
            sortedReconciliationIntervalsProvider.reconciliationIntervals(
              period.toInclusive.forgetRefinement
            )
      )

    intervals
      .map { sortedReconciliationIntervals =>
        _outstanding.updateAndGet(currentOutstanding =>
          computeOutstanding(
            counterParticipant,
            period,
            currentOutstanding,
            sortedReconciliationIntervals,
            matchingState,
          )
        )
        ()
      }
      .onShutdown(
        logger.debug(
          s"Aborted marking period safe (${period.fromExclusive}, ${period.toInclusive}] due to shutdown"
        )
      )
  }

  override def noOutstandingCommitments(
      beforeOrAt: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] =
    Future.successful {
      for {
        lastTs <- lastComputed.get
        adjustedTs = lastTs.forgetRefinement.min(beforeOrAt)
        periods = _outstanding
          .get()
          .collect {
            case (period, _, state) if state != CommitmentPeriodState.Matched =>
              period.fromExclusive.forgetRefinement -> period.toInclusive.forgetRefinement
          }
        safe = AcsCommitmentStore.latestCleanPeriod(
          beforeOrAt = adjustedTs,
          uncleanPeriods = periods,
        )
      } yield safe
    }

  override def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Seq[ParticipantId],
      includeMatchedPeriods: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)]] =
    Future.successful(_outstanding.get.filter { case (period, participant, state) =>
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
  ): Future[Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)]] = {
    val filteredByCounterParty =
      if (counterParticipant.isEmpty) computed
      else computed.filter(c => counterParticipant.contains(c._1))

    Future.successful(
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
  )(implicit traceContext: TraceContext): Future[Iterable[SignedProtocolMessage[AcsCommitment]]] = {
    val filteredByCounterParty = (if (counterParticipant.isEmpty) received
                                  else
                                    received.filter(c => counterParticipant.contains(c._1))).values

    Future.successful(
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
            (state != CommitmentPeriodState.Matched && period.toInclusive < before) ||
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

  private object lock;

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

  def watermark_(): RecordTime = rt.get()

  /** A read-only version of the snapshot.
    */
  def snapshot: TrieMap[SortedSet[LfPartyId], AcsCommitment.CommitmentType] = snap.snapshot()

  override def get()(implicit
      traceContext: TraceContext
  ): Future[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])] = {
    val rt = watermark_()
    Future.successful((rt, snapshot.toMap))
  }

  override def update(
      rt: RecordTime,
      updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deletes: Set[SortedSet[LfPartyId]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful(update_(rt, updates, deletes))

  override def watermark(implicit traceContext: TraceContext): Future[RecordTime] =
    Future.successful(watermark_())
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

  private object lock;

  private def syncF[T](v: => T): Future[T] = {
    val evaluated = blocking(lock.synchronized(v))
    Future.successful(evaluated)
  }

  override def enqueue(
      commitment: AcsCommitment
  )(implicit traceContext: TraceContext): Future[Unit] = syncF {
    queue.enqueue(commitment)
  }

  /** Returns all commitments whose period ends at or before the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThrough(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[List[AcsCommitment]] = syncF {
    queue.takeWhile(_.period.toInclusive <= timestamp).toList
  }

  /** Returns all commitments whose period ends at or after the given timestamp.
    *
    * Does not delete them from the queue.
    */
  override def peekThroughAtOrAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Seq[AcsCommitment]] =
    syncF {
      queue.filter(_.period.toInclusive >= timestamp).toSeq
    }

  def peekOverlapsForCounterParticipant(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[AcsCommitment]] =
    syncF {
      queue
        .filter(_.period.overlaps(period))
        .filter(_.sender == counterParticipant)
        .toSeq
    }

  /** Deletes all commitments whose period ends at or before the given timestamp. */
  override def deleteThrough(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = syncF {
    deleteWhile(queue)(_.period.toInclusive <= timestamp)
  }
}

object InMemoryCommitmentQueue {
  def deleteWhile[A](q: mutable.PriorityQueue[A])(p: A => Boolean): Unit = {
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
