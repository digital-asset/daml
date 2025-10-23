// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Flow that upon read signals will attempt to fetch all events until head is reached. Events are
  * paged in within a sub-source to prevent all events being held in memory at once.
  */
object FetchLatestEventsFlow {

  def apply[Out, State](
      initialState: State,
      lookup: State => FutureUnlessShutdown[(State, Seq[Out])],
      hasReachedHead: (State, Seq[Out]) => Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Flow[Traced[ReadSignal], Out, NotUsed] = {
    // i've struggled to work out a pekko-stream only way of maintaining this state.
    // we really want a `statefulFlatMapConcat` however that doesn't exist and trying to attempt
    // something similar in a custom graph is problematic. the current usage of a separate atomic reference
    // with the flatmapConcat operator appears correct and a ConcurrentModificationException will be thrown
    // if this assumptions are violated.
    val stateRef = new AtomicReference[State](initialState)

    val logger = loggerFactory.getTracedLogger(this.getClass)

    def fetchAllEventsUntilEmpty(implicit traceContext: TraceContext): Source[Seq[Out], NotUsed] = {
      logger.debug("Calling fetchAllEventsUntilEmpty...") // TODO(i28037): remove extra logging
      Source.unfoldAsync((stateRef.get(), false)) {
        case (_, true) =>
          // TODO(#26818): clean up excessive debug logging
          logger.debug("Reached head, stopping fetch")
          Future.successful(None)
        case (state, false) =>
          // TODO(#26818): clean up excessive debug logging
          logger.debug(s"Not yet at the head, performing fetch with state=$state")
          lookup(state)
            .map { case (newState, events) =>
              if (!stateRef.compareAndSet(state, newState)) {
                throw new ConcurrentModificationException("event states was unexpectedly modified")
              }

              val reachedHead = hasReachedHead(newState, events)

              Some(((newState, reachedHead), events))
            }
            .onShutdown {
              // TODO(#26818): clean up excessive debug logging
              logger.debug(s"Not going to fetch due to shutdown")
              None
            }
      }
    }

    Flow[Traced[ReadSignal]]
      .buffer(1, OverflowStrategy.dropHead)
      .prepend(Source.single(Traced(ReadSignal)(TraceContext.empty))) // initial fetch
      .map { tracedReadSignal =>
        tracedReadSignal.withTraceContext { implicit traceContext => readSignal =>
          // TODO(#26818): clean up excessive debug logging
          logger.debug(s"Received read signal: $readSignal")
        }
        tracedReadSignal
      }
      .flatMapConcat(tracedReadSignal => fetchAllEventsUntilEmpty(tracedReadSignal.traceContext))
      .mapConcat(identity)
  }

}
