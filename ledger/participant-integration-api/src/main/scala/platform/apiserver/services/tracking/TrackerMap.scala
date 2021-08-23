// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.util.concurrent.atomic.AtomicReference

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionSuccess,
  TrackedCompletionFailure,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.collection.immutable.HashMap
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** A map for [[Tracker]]s with thread-safe tracking methods and automatic cleanup. A tracker tracker, if you will.
  * @param retentionPeriod The minimum finite duration for which to retain idle trackers.
  */
private[services] final class TrackerMap[Key](
    retentionPeriod: FiniteDuration,
    getKey: Commands => Key,
    newTracker: Commands => Future[Tracker],
)(implicit loggingContext: LoggingContext)
    extends Tracker
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val lock = new Object()

  @volatile private var trackerBySubmitter =
    HashMap.empty[Key, TrackerMap.AsyncResource[Tracker.WithLastSubmission]]

  require(
    retentionPeriod < Long.MaxValue.nanoseconds,
    s"Retention period$retentionPeriod is too long. Must be below ${Long.MaxValue} nanoseconds.",
  )

  private val retentionNanos = retentionPeriod.toNanos

  val cleanup: Runnable = () =>
    lock.synchronized {
      val nanoTime = System.nanoTime()
      trackerBySubmitter foreach { case (submitter, trackerResource) =>
        trackerResource.ifPresent { tracker =>
          if (nanoTime - tracker.getLastSubmission > retentionNanos) {
            logger.info(
              s"Shutting down tracker for $submitter after inactivity of $retentionPeriod"
            )(trackerResource.loggingContext)
            trackerBySubmitter -= submitter
            tracker.close()
          }
        }
      }
    }

  def track(
      request: SubmitAndWaitRequest
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
    val commands = request.getCommands
    val key = getKey(commands)
    // double-checked locking
    trackerBySubmitter
      .getOrElse(
        key,
        lock.synchronized {
          trackerBySubmitter.getOrElse(
            key, {
              val r = new TrackerMap.AsyncResource(newTracker(commands).map { t =>
                logger.info(s"Registered tracker for submitter $key")
                Tracker.WithLastSubmission(t)
              })
              trackerBySubmitter += key -> r
              r
            },
          )
        },
      )
      .flatMap(_.track(request))
  }

  def close(): Unit = lock.synchronized {
    logger.info(s"Shutting down ${trackerBySubmitter.size} trackers")
    trackerBySubmitter.values.foreach(_.close())
    trackerBySubmitter = HashMap.empty
  }
}

private[services] object TrackerMap {
  sealed trait AsyncResourceState[+T <: AutoCloseable]
  final case object Waiting extends AsyncResourceState[Nothing]
  final case object Closed extends AsyncResourceState[Nothing]
  final case class Ready[T <: AutoCloseable](t: T) extends AsyncResourceState[T]

  /** A holder for an AutoCloseable that can be opened and closed async.
    * If closed before the underlying Future completes, will close the resource on completion.
    */
  final class AsyncResource[T <: AutoCloseable](
      future: Future[T]
  )(implicit val loggingContext: LoggingContext) {
    private val logger = ContextualizedLogger.get(this.getClass)

    // Must progress Waiting => Ready => Closed or Waiting => Closed.
    val state: AtomicReference[AsyncResourceState[T]] = new AtomicReference(Waiting)

    future.andThen {
      case Success(t) =>
        if (!state.compareAndSet(Waiting, Ready(t))) {
          // This is the punch line of AsyncResource.
          // If we've been closed in the meantime, we must close the underlying resource also.
          // This "on-failure-to-complete" behavior is not present in scala or java Futures.
          t.close()
        }
      // Someone should be listening to this failure downstream
      // TODO(mthvedt): Refactor so at least one downstream listener is always present,
      // and exceptions are never dropped.
      case Failure(ex) =>
        logger.error("failure to get async resource", ex)
        state.set(Closed)
    }(DirectExecutionContext)

    def flatMap[U](f: T => Future[U])(implicit ex: ExecutionContext): Future[U] =
      state.get() match {
        case Waiting => future.flatMap(f)
        case Closed => throw new IllegalStateException()
        case Ready(t) => f(t)
      }

    def ifPresent[U](f: T => U): Option[U] = state.get() match {
      case Ready(t) => Some(f(t))
      case _ => None
    }

    def close(): Unit = state.getAndSet(Closed) match {
      case Ready(t) => t.close()
      case _ =>
    }
  }
}
