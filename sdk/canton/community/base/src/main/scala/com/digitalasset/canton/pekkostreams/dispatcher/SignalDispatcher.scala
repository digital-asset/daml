// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pekkostreams.dispatcher

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.pekkostreams.dispatcher.SignalDispatcher.Signal
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Source, SourceQueueWithComplete}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** A fanout signaller that can be subscribed to dynamically.
  * Signals may be coalesced, but if a signal is sent, we guarantee that all consumers subscribed before
  * the signal is sent will eventually receive a signal.
  */
@SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
class SignalDispatcher private () {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  private val runningState: AtomicReference[Option[Set[SourceQueueWithComplete[Signal]]]] =
    new AtomicReference(Some(Set.empty))

  private[pekkostreams] def getRunningState: Set[SourceQueueWithComplete[Signal]] =
    runningState.get.getOrElse(throwClosed())

  /** Signal to this Dispatcher that there's a new head `Index`.
    * The Dispatcher will emit values on all streams until the new head is reached.
    */
  def signal(): Unit = getRunningState.foreach(state => state.offer(Signal).discard)

  /** Returns a Source that, when materialized, subscribes to this SignalDispatcher.
    *
    * @param signalOnSubscribe True if you want to send a signal to the new subscription.
    */
  def subscribe(signalOnSubscribe: Boolean = false): Source[Signal, NotUsed] =
    Source
      .queue[Signal](1, OverflowStrategy.dropTail)
      .mapMaterializedValue { q =>
        // this use of mapMaterializedValue, believe it or not, seems to be kosher
        val _ = runningState.updateAndGet { s =>
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
          }(ExecutionContext.parasitic)
        NotUsed
      }

  private def throwClosed(): Nothing = throw new IllegalStateException("SignalDispatcher is closed")

  /** Closes this SignalDispatcher by gracefully completing the existing Source subscriptions.
    * For any downstream with pending signals, at least one such signal will be sent first.
    */
  def shutdown(): Future[Unit] =
    shutdownInternal { source =>
      source.complete()
      source.watchCompletion()
    }

  /** Closes this SignalDispatcher by failing the existing Source subscriptions with the provided throwable. */
  def fail(throwableBuilder: () => Throwable): Future[Unit] =
    shutdownInternal { source =>
      implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.parasitic
      val throwable = throwableBuilder()
      source.fail(throwable)
      source
        .watchCompletion()
        .recover {
          case `throwable` =>
            // This throwable is expected so map to Success
            ()
          case unexpectedThrowable =>
            // On unexpected throwable, warn and continue
            logger.warn(s"Unexpected failure on Source shutdown", unexpectedThrowable)
        }
    }

  private def shutdownInternal(
      shutdownSourceQueue: SourceQueueWithComplete[_] => Future[_]
  ): Future[Unit] = {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.parasitic
    runningState
      .getAndSet(None)
      .fold(Future.failed[Unit](new IllegalStateException("SignalDispatcher is already closed"))) {
        sources =>
          Future.delegate {
            Future
              .traverse(sources) { source =>
                // Return a successful Future wrapping a Try
                // to ensure that Future.traverse waits for the completion of all sources
                shutdownSourceQueue(source)
                  .map(Success(_))
                  .recover { case failure => Failure(failure) }
              }
              .map { results =>
                // Fail if any of the sources failed
                results.map(_.fold(throw _, identity))
              }
              .map(_ => ())
          }
      }
  }
}

object SignalDispatcher {

  sealed abstract class Signal

  /** The signal sent by SignalDispatcher. */
  final case object Signal extends Signal

  /** Construct a new SignalDispatcher. Created Sources will consume Pekko resources until closed. */
  def apply[T](): SignalDispatcher = new SignalDispatcher()
}
