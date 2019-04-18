// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.platform.akkastreams.Dispatcher._
import com.digitalasset.platform.akkastreams.SteppingMode.{OneAfterAnother, RangeQuery}
import com.digitalasset.platform.common.util.DirectExecutionContext
import org.slf4j.LoggerFactory
import com.github.ghik.silencer.silent

import scala.collection.immutable
import scala.concurrent.Future

sealed abstract class SteppingMode[Index: Ordering, T] extends Product with Serializable {}

object SteppingMode {
  final case class OneAfterAnother[Index: Ordering, T](
      readSuccessor: (Index, T) => Index,
      readElement: Index => Future[T])
      extends SteppingMode[Index, T]
  final case class RangeQuery[Index: Ordering, T]() extends SteppingMode[Index, T] //TODO
}

/**
  * A fanout signaller, representing a stream of external updates,
  * that can be subscribed to dynamically at a given point in the stream.
  * Stream positions are given by the Index type, and stream values are given by T. Subscribing to a point
  * yields all values starting at that point.
  * It is assumed that the head index is the "end of the stream" and has no value.
  * This stage supports asynchronous reads both of index successors and values.
  * This class is thread-safe, and all callbacks provided to it must be thread-safe.
  *
  * @tparam Index The Index type.
  * @tparam T     The stored type.
  *
  */
//TODO update the docs
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Dispatcher[Index: Ordering, T] private (
    steppingMode: SteppingMode[Index, T],
    zeroIndex: Index,
    headAtInitialization: Index)
    extends HeadAwareDispatcher[Index, T]
    with AutoCloseable {

  require(
    !indexIsBeforeZero(headAtInitialization),
    s"head supplied at Dispatcher initialization $headAtInitialization is before zero index $zeroIndex. " +
      s"This would imply that the ledger end is before the ledger begin, which makes this invalid configuration."
  )

  private sealed abstract class State extends Product with Serializable {
    def getSignalDispatcher: Option[SignalDispatcher]

    def getLastIndex: Index
  }

  // the following silent are due to
  // <https://github.com/scala/bug/issues/4440>
  @silent
  private final case class Running(lastIndex: Index, signalDispatcher: SignalDispatcher)
      extends State {
    override def getLastIndex: Index = lastIndex
    override def getSignalDispatcher: Option[SignalDispatcher] = Some(signalDispatcher)
  }
  @silent
  private final case class Closed(lastIndex: Index) extends State {
    override def getLastIndex: Index = lastIndex
    override def getSignalDispatcher: Option[SignalDispatcher] = None
  }

  // So why not broadcast the actual new index, instead of using a signaller?
  // The reason is if we do that, the new indices race with readHead
  // in a way that makes it hard to start up new subscriptions. In particular,
  // we can tolerate NewIndexSignals being out of order or dropped, maintaining the weaker invariant that,
  // if head is updated, at least one NewIndexSignal eventually arrives.

  private val state = new AtomicReference[State](Running(headAtInitialization, SignalDispatcher()))

  /** returns the head index where this Dispatcher is at */
  override def getHead(): Index = state.get.getLastIndex

  /**
    * Signal to this Dispatcher that there's a new head `Index`.
    * The Dispatcher will emit values on all streams until the new head is reached.
    */
  override def signalNewHead(head: Index): Unit =
    state
      .getAndUpdate {
        case original @ Running(prev, disp) =>
          if (Ordering[Index].gt(head, prev)) Running(head, disp) else original
        case c: Closed => c
      } match {
      case Running(prev, disp) =>
        if (Ordering[Index].gt(head, prev)) disp.signal()
      case c: Closed =>
        logger.debug("Failed to update Dispatcher HEAD: instance already closed.")
    }

  override def startingAt(start: Index, requestedEnd: Option[Index]): Source[(Index, T), NotUsed] =
    requestedEnd.fold(startingAt(start))(
      end =>
        if (Ordering[Index].gt(start, end))
          Source.failed(new IllegalArgumentException(
            s"Invalid index section: start '$start' is after end '$end'"))
        else startingAt(start).takeWhile(_._1 != end, inclusive = true))

  /**
    * Gets all values from start, inclusive, to end, exclusive.
    */
  private def subsource(start: Index, end: Index): Source[(Index, T), NotUsed] =
    //TODO: we can do range here..
    Source
      .unfoldAsync[Index, (Index, T)](start) { i =>
        if (i == end) {
          Future.successful(None)
        } else {
          steppingMode match {
            case OneAfterAnother(readSuccessor, readElement) =>
              readElement(i).map { t =>
                val nextIndex = readSuccessor(i, t)
                Some((nextIndex, (nextIndex, t)))
              }(DirectExecutionContext)
            case RangeQuery() => ??? //TODO
          }

        }
      }

  /**
    * Return a source of all values starting at the given index, in the form (successor index, value).
    */
  // noinspection MatchToPartialFunction, ScalaUnusedSymbol
  def startingAt(start: Index): Source[(Index, T), NotUsed] =
    if (indexIsBeforeZero(start))
      Source.failed(
        new IllegalArgumentException(
          s"Invalid start index: '$start' before zero index '$zeroIndex'"))
    else
      state.get.getSignalDispatcher.fold(Source.failed[(Index, T)](closedError))(
        _.subscribe(signalOnSubscribe = true)
          .map(_ => getHead())
          .statefulMapConcat(() => new ContinuousRangeEmitter(start))
          .flatMapConcat {
            case (previousHead, head) => subsource(previousHead, head)
          })

  private class ContinuousRangeEmitter(private var max: Index) // var doesn't need to be synchronized, it is accessed in a GraphStage.
      extends (Index => immutable.Iterable[(Index, Index)]) {

    /**
      * @return if param  > [[max]] : a list with a single pair representing a ([max, param[) range, and also stores param in [[max]].
      *         Nil otherwise.
      */
    override def apply(newHead: Index): immutable.Iterable[(Index, Index)] =
      if (Ordering[Index].gt(newHead, max)) {
        val intervalBegin = max
        max = newHead
        List(intervalBegin -> newHead)
      } else Nil
  }

  private def indexIsBeforeZero(checkedIndex: Index): Boolean =
    Ordering[Index].gt(zeroIndex, checkedIndex)

  def close(): Unit =
    state.getAndUpdate {
      case Running(idx, _) => Closed(idx)
      case c: Closed => c
    } match {
      case Running(idx, disp) =>
        disp.signal()
        disp.close()
      case c: Closed => ()
    }

}

object Dispatcher {

  private val logger = LoggerFactory.getLogger(Dispatcher.getClass)

  private def closedError: IllegalStateException = {
    new IllegalStateException("Dispatcher is closed")
  }

  //TODO docs!
  /**
    * Construct a new Dispatcher. This will consume Akka resources until closed.
    *
    * @param readSuccessor The successor function for the Index. Must succeed for all Indices except the head index.
    *                      Dispatcher will never call this for the head index.
    * @param readElement   Reads an element for a corresponding Index.
    *                      Must succeed for any valid Index except the head index.
    * @tparam Index The index type.
    * @tparam T The element type.
    * @return A new Dispatcher.
    */
  def apply[Index: Ordering, T](
      steppingMode: SteppingMode[Index, T],
      firstIndex: Index,
      headAtInitialization: Index): Dispatcher[Index, T] =
    new Dispatcher(steppingMode, firstIndex, headAtInitialization)
}
