// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.util.concurrent.atomic.AtomicReference

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** A map for [[Tracker]]s with thread-safe tracking methods and automatic cleanup. A tracker tracker, if you will.
  * @param retentionPeriod The minimum finite duration for which to retain idle trackers.
  */
private[services] final class TrackerMap(retentionPeriod: FiniteDuration)(implicit
    loggingContext: LoggingContext
) extends AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val lock = new Object()

  @volatile private var trackerBySubmitter =
    HashMap.empty[TrackerMap.Key, TrackerMap.AsyncResource[Tracker.WithLastSubmission]]

  val cleanup: Runnable = {
    require(
      retentionPeriod < Long.MaxValue.nanoseconds,
      s"Retention period$retentionPeriod is too long. Must be below ${Long.MaxValue} nanoseconds.",
    )

    val retentionNanos = retentionPeriod.toNanos

    { () =>
      lock.synchronized {
        val nanoTime = System.nanoTime()
        trackerBySubmitter foreach { case (submitter, trackerResource) =>
          trackerResource.ifPresent(tracker =>
            if (nanoTime - tracker.getLastSubmission > retentionNanos) {
              logger.info(
                s"Shutting down tracker for $submitter after inactivity of $retentionPeriod"
              )
              remove(submitter)
              tracker.close()
            }
          )
        }
      }
    }
  }

  def track(submitter: TrackerMap.Key, request: SubmitAndWaitRequest)(
      newTracker: => Future[Tracker]
  )(implicit ec: ExecutionContext): Future[Either[CompletionFailure, CompletionSuccess]] =
    // double-checked locking
    trackerBySubmitter
      .getOrElse(
        submitter,
        lock.synchronized {
          trackerBySubmitter.getOrElse(
            submitter, {
              val r = new TrackerMap.AsyncResource(newTracker.map { t =>
                logger.info(s"Registered tracker for submitter $submitter")
                Tracker.WithLastSubmission(t)
              })

              trackerBySubmitter += submitter -> r

              r
            },
          )
        },
      )
      .flatMap(_.track(request))

  private def remove(submitter: TrackerMap.Key): Unit = lock.synchronized {
    trackerBySubmitter -= submitter
  }

  def close(): Unit = {
    lock.synchronized {
      logger.info(s"Shutting down ${trackerBySubmitter.size} trackers")
      trackerBySubmitter.values.foreach(_.close())
      trackerBySubmitter = HashMap.empty
    }
  }
}

private[services] object TrackerMap {

  final case class Key(application: String, parties: Set[String])

  sealed trait AsyncResourceState[+T <: AutoCloseable]
  final case object Waiting extends AsyncResourceState[Nothing]
  final case object Closed extends AsyncResourceState[Nothing]
  final case class Ready[T <: AutoCloseable](t: T) extends AsyncResourceState[T]

  /** A holder for an AutoCloseable that can be opened and closed async.
    * If closed before the underlying Future completes, will close the resource on completion.
    */
  final class AsyncResource[T <: AutoCloseable](future: Future[T]) {
    private val logger = LoggerFactory.getLogger(this.getClass)

    // Must progress Waiting => Ready => Closed or Waiting => Closed.
    val state: AtomicReference[AsyncResourceState[T]] = new AtomicReference(Waiting)

    future.andThen({
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
    })(DirectExecutionContext)

    def flatMap[U](f: T => Future[U])(implicit ex: ExecutionContext): Future[U] = {
      state.get() match {
        case Waiting => future.flatMap(f)
        case Closed => throw new IllegalStateException()
        case Ready(t) => f(t)
      }
    }

    def map[U](f: T => U)(implicit ex: ExecutionContext): Future[U] =
      flatMap(t => Future.successful(f(t)))

    def ifPresent[U](f: T => U): Option[U] = state.get() match {
      case Ready(t) => Some(f(t))
      case _ => None
    }

    def close(): Unit = state.getAndSet(Closed) match {
      case Ready(t) => t.close()
      case _ =>
    }
  }

  def apply(retentionPeriod: FiniteDuration)(implicit loggingContext: LoggingContext): TrackerMap =
    new TrackerMap(retentionPeriod)
}
