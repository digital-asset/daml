// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.akkastreams.dispatcher

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.resources.ResourceOwner

/** A fanout signaller, representing a stream of external updates,
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

  /** Returns a stream of elements with the next index from start (exclusive) to end (inclusive) */
  def startingAt[T](
      startExclusive: Index,
      subSource: SubSource[Index, T],
      endInclusive: Option[Index] = None,
  ): Source[(Index, T), NotUsed]
}

object Dispatcher {

  /** Construct a new Dispatcher. This will consume Akka resources until closed.
    *
    * @param zeroIndex            the initial starting Index instance
    * @param headAtInitialization the head index at the time of creation
    * @tparam Index The index type
    * @return A new Dispatcher.
    */
  def apply[Index: Ordering](
      name: String,
      zeroIndex: Index,
      headAtInitialization: Index,
  ): Dispatcher[Index] =
    new DispatcherImpl[Index](name: String, zeroIndex, headAtInitialization)

  def owner[Index: Ordering](
      name: String,
      zeroIndex: Index,
      headAtInitialization: Index,
  ): ResourceOwner[Dispatcher[Index]] =
    ResourceOwner.forCloseable(() => apply(name, zeroIndex, headAtInitialization))

}
