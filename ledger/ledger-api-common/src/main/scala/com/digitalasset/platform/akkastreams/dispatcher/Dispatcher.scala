// Copyright (c) 2019 The DAML Authors. All rights reserved.
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
trait Dispatcher[Index] extends AutoCloseable {

  /** Returns the head index where this Dispatcher is at */
  def getHead(): Index

  /** Signals and stores a new head in memory. */
  def signalNewHead(head: Index): Unit

  /** Returns a stream of elements with the next index from start (inclusive) to end (exclusive) */
  def startingAt[T](
      startInclusive: Index,
      subSource: SubSource[Index, T],
      endExclusive: Option[Index]): Source[(Index, T), NotUsed]

  /** Returns a source of all values starting at the given index, in the form (successor index, value) */
  def startingAt[T](start: Index, subSource: SubSource[Index, T]): Source[(Index, T), NotUsed]
}

object Dispatcher {

  /**
    * Construct a new Dispatcher. This will consume Akka resources until closed.
    *
    * @param zeroIndex            the initial starting Index instance
    * @param headAtInitialization the head index at the time of creation
    * @tparam Index The index type
    * @return A new Dispatcher.
    */
  def apply[Index: Ordering](zeroIndex: Index, headAtInitialization: Index): Dispatcher[Index] =
    new DispatcherImpl[Index](zeroIndex, headAtInitialization)
}
