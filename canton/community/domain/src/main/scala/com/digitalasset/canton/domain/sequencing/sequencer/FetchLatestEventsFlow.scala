// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Source}

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Flow that upon read signals will attempt to fetch all events until head is reached.
  * Events are paged in within a sub-source to prevent all events being held in memory at once.
  */
object FetchLatestEventsFlow {

  def apply[Out, State](
      initialState: State,
      lookup: State => Future[(State, Seq[Out])],
      hasReachedHead: (State, Seq[Out]) => Boolean,
  )(implicit executionContext: ExecutionContext): Flow[ReadSignal, Out, NotUsed] = {
    // i've struggled to work out a akka-stream only way of maintaining this state.
    // we really want a `statefulFlatMapConcat` however that doesn't exist and trying to attempt
    // something similar in a custom graph is problematic. the current usage of a separate atomic reference
    // with the flatmapConcat operator appears correct and a ConcurrentModificationException will be thrown
    // if this assumptions are violated.
    val stateRef = new AtomicReference[State](initialState)

    def fetchAllEventsUntilEmpty(): Source[Seq[Out], NotUsed] = {
      Source.unfoldAsync((stateRef.get(), false)) {
        case (_, true) => Future.successful(None)
        case (state, false) =>
          lookup(state) map { case (newState, events) =>
            if (!stateRef.compareAndSet(state, newState)) {
              throw new ConcurrentModificationException("event states was unexpectedly modified")
            }

            val reachedHead = hasReachedHead(newState, events)

            Some(((newState, reachedHead), events))
          }
      }
    }

    Flow[ReadSignal]
      .buffer(1, OverflowStrategy.dropHead)
      .prepend(Source.single(ReadSignal))
      .flatMapConcat(_ => fetchAllEventsUntilEmpty())
      .mapConcat(identity)
  }

}
