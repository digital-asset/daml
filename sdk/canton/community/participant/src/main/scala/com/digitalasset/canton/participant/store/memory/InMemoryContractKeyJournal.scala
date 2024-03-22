// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.participant.store.ContractKeyJournal.{
  ContractKeyJournalError,
  ContractKeyState,
  InconsistentKeyAllocationStatus,
  Status,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfGlobalKey
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.tracing.TraceContext

import scala.Ordered.orderingToOrdered
import scala.collection.concurrent.TrieMap
import scala.collection.{SortedMap, mutable}
import scala.concurrent.{ExecutionContext, Future, blocking}

class InMemoryContractKeyJournal(override protected val loggerFactory: NamedLoggerFactory)(
    override implicit val ec: ExecutionContext
) extends ContractKeyJournal
    with NamedLogging
    with InMemoryPrunableByTime {

  import InMemoryContractKeyJournal.*

  private val state: TrieMap[LfGlobalKey, KeyStatus] = new TrieMap[LfGlobalKey, KeyStatus]()

  /** Lock to synchronize all writes to [[state]].
    * We don't need to synchronize reads because TrieMap is thread-safe.
    */
  private val lock: Object = new Object()

  override def fetchStates(
      keys: Iterable[LfGlobalKey]
  )(implicit traceContext: TraceContext): Future[Map[LfGlobalKey, ContractKeyState]] =
    Future {
      val snapshot = state.readOnlySnapshot()
      keys.to(LazyList).mapFilter(key => snapshot.get(key).flatMap(_.latest.map(key -> _))).toMap
    }

  override def addKeyStateUpdates(updates: Map[LfGlobalKey, (Status, TimeOfChange)])(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournal.ContractKeyJournalError, Unit] =
    EitherT.fromEither[Future] {
      updates.to(LazyList).traverse_ { case (key, (count, toc)) =>
        withLock {
          KeyStatus.empty.addUpdate(key, count, toc).flatMap { fresh =>
            state.putIfAbsent(key, fresh) match {
              case None => Either.right(())
              case Some(previous) =>
                previous.addUpdate(key, count, toc).map { updated =>
                  val _ = state.put(key, updated)
                }
            }
          }
        }
      }
    }

  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Int] =
    Future.successful(mapWithCleanup(_.prune(beforeAndIncluding)))

  override def deleteSince(inclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    EitherT.pure[Future, ContractKeyJournalError](mapWithCleanup(_.deleteSince(inclusive)).discard)

  override def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int] =
    Future.successful {
      state.get(key).getOrElse(KeyStatus.empty).changes.size
    }

  private[this] def mapWithCleanup(f: KeyStatus => KeyStatus): Int = withLock {
    val obsolete = mutable.Seq.empty[LfGlobalKey]
    state.mapValuesInPlace { case (key, status) =>
      val updated = f(status)
      if (updated eq KeyStatus.empty) {
        val _ = obsolete :+ key
      }
      updated
    }
    obsolete.foreach { key =>
      state -= key
    }
    obsolete.length
  }

  private[this] def withLock[A](x: => A): A = blocking { lock.synchronized(x) }

}

object InMemoryContractKeyJournal {

  // Entries are sorted in reverse order so that we can iterate in descending order
  private type ChangeJournal = SortedMap[TimeOfChange, Status]

  // Invariant: All values in the change journal are non-negative.
  final case class KeyStatus private (changes: ChangeJournal) {

    def latest: Option[ContractKeyState] = changes.headOption.map { case (toc, status) =>
      ContractKeyState(status, toc)
    }

    def addUpdate(
        key: LfGlobalKey,
        status: Status,
        toc: TimeOfChange,
    ): Either[ContractKeyJournalError, KeyStatus] = {
      for {
        newChanges <- changes.get(toc) match {
          case None =>
            Either.right(changes.concat(List(toc -> status)))
          case Some(oldStatus) =>
            Either.cond(
              oldStatus == status,
              changes,
              InconsistentKeyAllocationStatus(key, toc, oldStatus, status),
            )
        }
      } yield KeyStatus(newChanges)
    }

    def prune(beforeAndIncluding: CantonTimestamp): KeyStatus = {
      val later = changes.takeWhile { case (toc, _status) => toc.timestamp > beforeAndIncluding }

      // We want to keep the last change before or at `beforeAndIncluding` unless its count is 0
      val earliestO = later.lastOption.fold(changes.headOption) { case (toc, _count) =>
        val iter = changes.iteratorFrom(toc)
        val _ = iter.next() // skip this one as it's already in later
        if (iter.hasNext) Some(iter.next()) else None
      }
      val pruned = earliestO match {
        case None => later
        case Some((toc, state)) => if (state.prunable) later else later.concat(List((toc -> state)))
      }

      if (pruned.isEmpty) KeyStatus.empty
      else if (pruned.lastOption == changes.lastOption) this
      else new KeyStatus(pruned)
    }

    def deleteSince(including: TimeOfChange): KeyStatus = {
      val deleted = changes.dropWhile { case (toc, _) => toc >= including }
      if (deleted.isEmpty) KeyStatus.empty
      else if (deleted.headOption == changes.headOption) this
      else new KeyStatus(deleted)
    }
  }

  object KeyStatus {
    val empty = new KeyStatus(SortedMap.empty[TimeOfChange, Status](Ordering[TimeOfChange].reverse))
  }

}
