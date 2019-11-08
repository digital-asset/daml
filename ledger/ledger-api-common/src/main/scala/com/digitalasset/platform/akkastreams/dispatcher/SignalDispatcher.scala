// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams.dispatcher

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import com.digitalasset.platform.common.util.DirectExecutionContext
import org.slf4j.LoggerFactory

/**
  * A fanout signaller that can be subscribed to dynamically.
  * Signals may be coalesced, but if a signal is sent, we guarantee that all consumers subscribed before
  * the signal is sent will eventually receive a signal.
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SignalDispatcher[T] private () extends AutoCloseable {

  val logger = LoggerFactory.getLogger(getClass)

  private val runningState: AtomicReference[Option[Set[SourceQueueWithComplete[T]]]] =
    new AtomicReference(Some(Set.empty))

  private[akkastreams] def getRunningState: Set[SourceQueueWithComplete[T]] =
    runningState.get.getOrElse(throwClosed())

  /**
    * Signal to this Dispatcher that there's a new head `Index`.
    * The Dispatcher will emit values on all streams until the new head is reached.
    */
  def signal(t: T): Unit = getRunningState.foreach(_.offer(t))

  /**
    * Returns a Source that, when materialized, subscribes to this SignalDispatcher.
    *
    * @param signalOnSubscribe True if you want to send a signal to the new subscription.
    */
  def subscribe(signalOnSubscribe: Option[T]): Source[T, NotUsed] =
    Source
      .queue[T](1, OverflowStrategy.dropTail)
      .mapMaterializedValue { q =>
        // this use of mapMaterializedValue, believe it or not, seems to be kosher
        runningState.updateAndGet { s =>
          s.map(set => set + q)
        } match {
          // We do this here, since updateAndGet is not guaranteed once-only.
          case Some(_) =>
            signalOnSubscribe.foreach(q.offer)
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

  /** Construct a new SignalDispatcher. Created Sources will consume Akka resources until closed. */
  def apply[T](): SignalDispatcher[T] = new SignalDispatcher[T]()
}
