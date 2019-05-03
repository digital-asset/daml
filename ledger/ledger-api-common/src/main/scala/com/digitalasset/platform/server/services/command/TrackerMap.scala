// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.command

import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.services.command.TrackerMap.{AsyncResource, Key}
import com.github.ghik.silencer.silent
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.HashMap
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * A map for [[Tracker]]s with thread-safe tracking methods and automatic cleanup. A tracker tracker, if you will.
  * @param retentionPeriod The minimum finite duration for which to retain idle trackers.
  */
class TrackerMap(retentionPeriod: FiniteDuration) extends AutoCloseable with LazyLogging {

  private val lock = new Object()

  @volatile private var trackerBySubmitter =
    HashMap.empty[Key, AsyncResource[Tracker.WithLastSubmission]]

  val cleanup: Runnable = {
    require(
      retentionPeriod < Long.MaxValue.nanoseconds,
      s"Retention period$retentionPeriod is too long. Must be below ${Long.MaxValue} nanoseconds."
    )

    val retentionNanos = retentionPeriod.toNanos

    { () =>
      lock.synchronized {
        val nanoTime = System.nanoTime()
        trackerBySubmitter foreach {
          case (submitter, trackerResource) =>
            trackerResource.ifPresent(tracker =>
              if (nanoTime - tracker.getLastSubmission > retentionNanos) {
                logger.info(
                  s"Shutting down tracker for $submitter after inactivity of $retentionPeriod")
                remove(submitter)
                tracker.close()
            })
        }
      }
    }
  }

  def track(submitter: Key, request: SubmitAndWaitRequest)(newTracker: => Future[Tracker])(
      implicit ec: ExecutionContext): Future[Completion] =
    // double-checked locking
    trackerBySubmitter
      .getOrElse(
        submitter,
        lock.synchronized {
          trackerBySubmitter.getOrElse(
            submitter, {
              val r = new AsyncResource(newTracker.map { t =>
                logger.info("Registered tracker for submitter {}", submitter)
                Tracker.WithLastSubmission(t)
              })

              trackerBySubmitter += submitter -> r

              r
            }
          )
        }
      )
      .flatMap(_.track(request))

  private def remove(submitter: Key): Unit = lock.synchronized {
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

object TrackerMap {
  case class Key(application: String, party: String)

  /**
    * A holder for an AutoCloseable that can be opened and closed async.
    * If closed before the underlying Future completes, will close the resource on completion.
    */
  class AsyncResource[T <: AutoCloseable](future: Future[T]) extends LazyLogging {
    sealed trait AsnycResourceState
    final case object Waiting extends AsnycResourceState
    // the following silent is due to
    // <https://github.com/scala/bug/issues/4440>
    @silent
    final case class Ready(t: T) extends AsnycResourceState
    final case object Closed extends AsnycResourceState

    // Must progress Waiting => Ready => Closed or Waiting => Closed.
    val state: AtomicReference[AsnycResourceState] = new AtomicReference(Waiting)

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
        case Ready(t) => f(t)
        case Closed => throw new IllegalStateException()
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

  def apply(retentionPeriod: FiniteDuration): TrackerMap = new TrackerMap(retentionPeriod)
}
