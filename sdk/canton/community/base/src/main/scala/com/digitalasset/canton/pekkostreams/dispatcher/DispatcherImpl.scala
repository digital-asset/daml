// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pekkostreams.dispatcher

import com.digitalasset.canton.pekkostreams.dispatcher.DispatcherImpl.Incrementable
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.math.Ordered.orderingToOrdered

final class DispatcherImpl[Index <: Ordered[Index] & Incrementable[Index]](
    name: String,
    firstIndex: Index,
    headAtInitialization: Option[Index],
) extends Dispatcher[Index] {
  import DispatcherImpl.DispatcherIsClosedException

  private type State = DispatcherImpl.State[Index]
  private type Closed = DispatcherImpl.Closed[Index]
  private val Running = DispatcherImpl.Running
  private val Closed = DispatcherImpl.Closed

  private val logger = LoggerFactory.getLogger(getClass)

  require(
    !indexIsBeforeFirst(headAtInitialization),
    s"head supplied at Dispatcher initialization $headAtInitialization is before first index $firstIndex. " +
      s"This would imply that the ledger end is before the ledger begin, which makes this invalid configuration.",
  )

  // So why not broadcast the actual new index, instead of using a signaller?
  // The reason is if we do that, the new indices race with readHead
  // in a way that makes it hard to start up new subscriptions. In particular,
  // we can tolerate NewIndexSignals being out of order or dropped, maintaining the weaker invariant that,
  // if head is updated, at least one NewIndexSignal eventually arrives.

  private val state = new AtomicReference[State](Running(headAtInitialization, SignalDispatcher()))

  /** returns the head index where this Dispatcher is at */
  override def getHead(): Option[Index] = state.get.getLastIndex

  /** Signal to this Dispatcher that there's a new head `Index`. The Dispatcher will emit values on
    * all streams until the new head is reached.
    */
  override def signalNewHead(head: Index): Unit =
    state
      .getAndUpdate {
        case original @ Running(prev, disp) =>
          if (prev < Some(head)) Running(Some(head), disp) else original
        case c: Closed => c
      } match {
      case Running(prev, disp) =>
        if (prev < Some(head)) disp.signal()
      case _: Closed =>
        logger.debug(s"$name: Failed to update Dispatcher HEAD: instance already closed.")
    }

  // noinspection MatchToPartialFunction, ScalaUnusedSymbol
  override def startingAt[T](
      startExclusive: Option[Index],
      subsource: SubSource[Index, T],
      endInclusive: Option[Index] = None,
  ): Source[(Index, T), NotUsed] = {
    val startInclusive = startExclusive.fold(firstIndex)(_.increment)
    if (indexIsBeforeFirst(Some(startInclusive)))
      Source.failed(
        new IllegalArgumentException(
          s"$name: Invalid start index: '$startInclusive' before first index '$firstIndex'"
        )
      )
    else if (endInclusive.exists(end => startInclusive > end))
      Source.failed(
        new IllegalArgumentException(
          s"$name: Invalid index section: start '$startInclusive' is after end '$endInclusive'"
        )
      )
    else {
      val subscription: Source[Index, NotUsed] =
        state.get.getSignalDispatcher
          .fold(Source.failed[Option[Index]](closedError))(
            _.subscribe(signalOnSubscribe = true)
              // This needs to call getHead directly, otherwise this subscription might miss a Signal being emitted
              .map(_ => getHead())
          )
          // discard uninitialized offset
          .collect { case Some(off) => off }

      val withOptionalEnd: Source[Index, NotUsed] =
        endInclusive.fold(subscription)(maxLedgerEnd =>
          // If we detect that the __signal__ (i.e. new ledger end updates) goes beyond the provided max ledger end,
          // we can complete the stream from the upstream direction and are not dependent on doing the filtering
          // on actual ledger entries (which might not even exist, e.g. due to duplicate submissions or
          // the ledger end having moved after a package upload or party allocation)
          subscription
            // accept ledger end signals until the signal exceeds the requested max ledger end.
            // the last offset that we should emit here is maxLedgerEnd
            .takeWhile(_ < maxLedgerEnd, inclusive = true)
            .map(Ordering[Index].min(_, maxLedgerEnd))
        )

      withOptionalEnd
        .statefulMap(create = () => startInclusive)(
          f = continuousRangeEmitter,
          onComplete = _ => None,
        )
        .mapConcat(identity)
        .flatMapConcat { case (previousHead, head) =>
          subsource(previousHead, head)
        }
    }
  }

//   if newHead >= inclusiveBegin returns a list with a single pair representing a ([inclusiveBegin, param]) range, and
//   newHead + 1 as the the next inclusiveBegin state,
//   Nil and the same state, otherwise.
  private def continuousRangeEmitter: (Index, Index) => (Index, List[(Index, Index)]) =
    (inclusiveBegin, newHead) =>
      if (newHead >= inclusiveBegin) {
        val nextInclusiveBegin = newHead.increment
        (nextInclusiveBegin, List(inclusiveBegin -> newHead))
      } else (inclusiveBegin, Nil)

  private def indexIsBeforeFirst(checkedIndexO: Option[Index]): Boolean =
    checkedIndexO match {
      case Some(idx) => idx < firstIndex
      case None => false
    }

  override def shutdown(): Future[Unit] =
    shutdownInternal { dispatcher =>
      dispatcher.signal()
      dispatcher.shutdown()
    }

  override def cancel(throwableBuilder: () => Throwable): Future[Unit] =
    shutdownInternal(_.fail(throwableBuilder))

  private def shutdownInternal(shutdown: SignalDispatcher => Future[Unit]): Future[Unit] =
    state.getAndUpdate {
      case Running(idx, _) => Closed(idx)
      case c: Closed => c
    } match {
      case Running(_, disp) => shutdown(disp)
      case _: Closed => Future.unit
    }

  private def closedError: DispatcherIsClosedException =
    new DispatcherIsClosedException(s"$name: Dispatcher is closed")

}

object DispatcherImpl {
  class DispatcherIsClosedException(msg: String) extends IllegalStateException(msg)

  private sealed abstract class State[Index] extends Product with Serializable {
    def getSignalDispatcher: Option[SignalDispatcher]

    def getLastIndex: Option[Index]
  }

  private final case class Running[Index](
      lastIndex: Option[Index],
      signalDispatcher: SignalDispatcher,
  ) extends State[Index] {
    override def getLastIndex: Option[Index] = lastIndex

    override def getSignalDispatcher: Option[SignalDispatcher] = Some(signalDispatcher)
  }

  private final case class Closed[Index](lastIndex: Option[Index]) extends State[Index] {
    override def getLastIndex: Option[Index] = lastIndex

    override def getSignalDispatcher: Option[SignalDispatcher] = None
  }

  trait Incrementable[T] extends Any {
    def increment: T
  }

}
