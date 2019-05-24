// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams.dispatcher

import akka.NotUsed
import akka.stream.scaladsl.Source

/**
  * A fanout signaller, representing a stream of external updates,
  * that can be subscribed to dynamically at a given point in the stream.
  * Stream positions are given by the Index type, and stream values are given by T. Subscribing to a point
  * yields all values starting at that point.
  * It is assumed that the head index is the "end of the stream" and has no value.
  *
  * Implementations must be thread-safe, so must the callbacks provided to it.
  */
trait Dispatcher[Index, T] extends AutoCloseable {

  /** Returns the head index where this Dispatcher is at */
  def getHead(): Index

  /** Signals and stores a new head in memory. */
  def signalNewHead(head: Index): Unit

  /** Returns a stream of elements with the next index from start (inclusive) to end (exclusive) */
  def startingAt(startInclusive: Index, endExclusive: Option[Index]): Source[(Index, T), NotUsed]

  /** Returns a source of all values starting at the given index, in the form (successor index, value) */
  def startingAt(start: Index): Source[(Index, T), NotUsed]
}

object Dispatcher {

  /**
    * Construct a new Dispatcher. This will consume Akka resources until closed.
    *
    * @param steppingMode         the chosen SteppingMode
    * @param zeroIndex            the initial starting Index instance
    * @param headAtInitialization the head index at the time of creation
    * @tparam Index The index type
    * @tparam T     The element type
    * @return A new Dispatcher.
    */
  def apply[Index: Ordering, T](
      steppingMode: SubSource[Index, T],
      zeroIndex: Index,
      headAtInitialization: Index): Dispatcher[Index, T] =
    new DispatcherImpl[Index, T](steppingMode, zeroIndex, headAtInitialization)
}
