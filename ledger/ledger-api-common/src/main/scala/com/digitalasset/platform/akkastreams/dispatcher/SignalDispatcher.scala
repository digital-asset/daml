// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams.dispatcher

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import com.digitalasset.platform.akkastreams.dispatcher.SignalDispatcher.Signal
import com.digitalasset.dec.DirectExecutionContext
import org.slf4j.LoggerFactory

/**
  * A fanout signaller that can be subscribed to dynamically.
  * Signals may be coalesced, but if a signal is sent, we guarantee that all consumers subscribed before
  * the signal is sent will eventually receive a signal.
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SignalDispatcher private () extends AutoCloseable {

  val logger = LoggerFactory.getLogger(getClass)

  private val runningState: AtomicReference[Option[Set[SourceQueueWithComplete[Signal]]]] =
    new AtomicReference(Some(Set.empty))

  private[akkastreams] def getRunningState: Set[SourceQueueWithComplete[Signal]] =
    runningState.get.getOrElse(throwClosed())

  /**
    * Signal to this Dispatcher that there's a new head `Index`.
    * The Dispatcher will emit values on all streams until the new head is reached.
    */
  def signal(): Unit = getRunningState.foreach(_.offer(Signal))

  /**
    * Returns a Source that, when materialized, subscribes to this SignalDispatcher.
    *
    * @param signalOnSubscribe True if you want to send a signal to the new subscription.
    */
  def subscribe(signalOnSubscribe: Boolean = false): Source[Signal, NotUsed] =
    Source
      .queue[Signal](1, OverflowStrategy.dropTail)
      .mapMaterializedValue { q =>
        // this use of mapMaterializedValue, believe it or not, seems to be kosher
        runningState.updateAndGet { s =>
          s.map(set => set + q)
        } match {
          // We do this here, since updateAndGet is not guaranteed once-only.
          case Some(_) =>
            if (signalOnSubscribe) q.offer(Signal)
          case None =>
            q.complete() // avoid a leak
            throwClosed()
        }
        q.watchCompletion()
          .onComplete { _ =>
            runningState.updateAndGet(_.map(s => s - q))
          }(DirectExecutionContext)
        NotUsed
      }

  private def throwClosed(): Nothing = throw new IllegalStateException("SignalDispatcher is closed")

  /**
    * Closes this SignalDispatcher.
    * For any downstream with pending signals, at least one such signal will be sent first.
    */
  def close(): Unit =
    runningState
      .getAndSet(None)
      // note, Materializer's lifecycle is managed outside of this class
      // fire and forget complete signals -- we can't control how long downstream takes
      .fold(throw new IllegalStateException("SignalDispatcher is already closed"))(
        _.foreach(_.complete()))

}

object SignalDispatcher {

  sealed abstract class Signal

  /** The signal sent by SignalDispatcher. */
  final case object Signal extends Signal

  /** Construct a new SignalDispatcher. Created Sources will consume Akka resources until closed. */
  def apply[T](): SignalDispatcher = new SignalDispatcher()
}
