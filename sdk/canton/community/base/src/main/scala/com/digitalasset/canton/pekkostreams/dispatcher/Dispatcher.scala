// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pekkostreams.dispatcher

import com.digitalasset.canton.pekkostreams.dispatcher.DispatcherImpl.Incrementable
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** A fanout signaller, representing a stream of external updates,
  * that can be subscribed to dynamically at a given point in the stream.
  * Stream positions are given by the Index type, and stream values are given by T. Subscribing to a point
  * yields all values starting at that point.
  * It is assumed that the head index is the "end of the stream" and has no value.
  *
  * Implementations must be thread-safe, so must the callbacks provided to it.
  */
trait Dispatcher[Index] {

  /** Returns the head index where this Dispatcher is at */
  def getHead(): Option[Index]

  /** Signals and stores a new head in memory. */
  def signalNewHead(head: Index): Unit

  /** Returns a stream of elements with the next index from start (exclusive) to end (inclusive)
    * Throws `DispatcherIsClosedException` if dispatcher is in the shutting down state
    */
  def startingAt[T](
      startExclusive: Option[Index],
      subSource: SubSource[Index, T],
      endInclusive: Option[Index] = None,
  ): Source[(Index, T), NotUsed]

  /** Triggers shutdown of the Dispatcher by completing all outstanding stream subscriptions.
    * This method ensures that all outstanding subscriptions have been notified with the latest signalled head
    * and waits for their graceful completion.
    */
  def shutdown(): Future[Unit]

  /** Triggers shutdown of the Dispatcher by eagerly failing all outstanding stream subscriptions with a throwable.
    *
    * @param throwableBuilder Create a new throwable.
    *                     It is important to create a new throwable for each failed subscription
    *                     since the throwable closing the streams or parts of it can be mutable.
    *                     (e.g. the [[io.grpc.Metadata]] provided as part of [[io.grpc.StatusRuntimeException]]
    *                     is mutated in the gRPC layer)
    */
  def cancel(throwableBuilder: () => Throwable): Future[Unit]
}

object Dispatcher {

  /** Construct a new Dispatcher. This will consume Pekko resources until closed.
    *
    * @param firstIndex            the initial starting Index instance
    * @param headAtInitialization the head index at the time of creation
    * @tparam Index The index type
    * @return A new Dispatcher.
    */
  def apply[Index <: Ordered[Index] & Incrementable[Index]](
      name: String,
      firstIndex: Index,
      headAtInitialization: Option[Index],
  ): Dispatcher[Index] =
    new DispatcherImpl[Index](name, firstIndex, headAtInitialization)

}
