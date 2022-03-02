// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

import akka.stream.Materializer
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionSuccess,
  TrackedCompletionFailure,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.services.tracking.TrackerMap._

import scala.collection.immutable.HashMap
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/** A map for [[Tracker]]s with thread-safe tracking methods and automatic cleanup.
  * A tracker tracker, if you will.
  *
  * @param retentionPeriod The minimum duration for which to retain ready-but-idling trackers.
  * @param getKey          A function to compute the tracker key from the commands.
  * @param newTracker      A function to construct a new tracker.
  *                        Called when there is no tracker for the given key.
  */
private[services] final class TrackerMap[Key](
    retentionPeriod: Duration,
    getKey: Commands => Key,
    newTracker: Key => Future[Tracker],
)(implicit loggingContext: LoggingContext)
    extends Tracker
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val lock = new Object()

  @volatile private var trackerBySubmitter =
    HashMap.empty[Key, TrackerMap.AsyncResource[TrackerWithLastSubmission]]

  private val retentionNanos =
    try {
      retentionPeriod.toNanos
    } catch {
      case _: ArithmeticException =>
        throw new IllegalArgumentException(
          s"Retention period $retentionPeriod is invalid. Must be between 1 and ${Long.MaxValue} nanoseconds."
        )
    }

  override def track(
      submission: CommandSubmission
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
    val key = getKey(submission.commands)
    // double-checked locking
    trackerBySubmitter
      .getOrElse(
        key,
        lock.synchronized {
          trackerBySubmitter.getOrElse(key, registerNewTracker(key))
        },
      )
      .withResource(_.track(submission))
  }

  private def registerNewTracker(
      key: Key
  )(implicit executionContext: ExecutionContext): AsyncResource[TrackerWithLastSubmission] = {
    val resource = new AsyncResource(
      newTracker(key)
        .andThen {
          case Success(_) =>
            logger.info(s"Registered a tracker for submitter $key.")
          case Failure(exception) =>
            logger.error("Failed to register a tracker.", exception)
        }
        .map(new TrackerWithLastSubmission(_))
    )
    trackerBySubmitter += key -> resource
    resource
  }

  def cleanup(): Unit = lock.synchronized {
    val currentTime = System.nanoTime()
    trackerBySubmitter.foreach { case (submitter, trackerResource) =>
      trackerResource.currentState match {
        case Waiting => // there is nothing to clean up
        case Ready(tracker) =>
          // close and forget expired trackers
          if (currentTime - tracker.getLastSubmission > retentionNanos) {
            logger.info(
              s"Shutting down tracker for $submitter after inactivity of $retentionPeriod"
            )(trackerResource.loggingContext)
            tracker.close()
            trackerBySubmitter -= submitter
          }
        case Failed(_) | Closed =>
          // simply forget already-failed or closed trackers
          trackerBySubmitter -= submitter
      }
    }
  }

  def close(): Unit = lock.synchronized {
    logger.info(s"Shutting down ${trackerBySubmitter.size} trackers")
    trackerBySubmitter.values.foreach(_.close())
    trackerBySubmitter = HashMap.empty
  }
}

private[services] object TrackerMap {
  final class SelfCleaning[Key](
      retentionPeriod: Duration,
      getKey: Commands => Key,
      newTracker: Key => Future[Tracker],
      cleanupInterval: FiniteDuration,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ) extends Tracker {
    private val delegate = new TrackerMap(retentionPeriod, getKey, newTracker)
    private val trackerCleanupJob = materializer.system.scheduler
      .scheduleAtFixedRate(cleanupInterval, cleanupInterval)(() => delegate.cleanup())

    override def track(
        submission: CommandSubmission
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] =
      delegate.track(submission)

    override def close(): Unit = {
      trackerCleanupJob.cancel()
      delegate.close()
    }
  }

  private sealed trait AsyncResourceState[+T <: AutoCloseable]

  private final case object Waiting extends AsyncResourceState[Nothing]

  private final case class Ready[T <: AutoCloseable](resource: T) extends AsyncResourceState[T]

  private final case object Closed extends AsyncResourceState[Nothing]

  private final case class Failed(exception: Throwable) extends AsyncResourceState[Nothing]

  /** A holder for an AutoCloseable that can be opened and closed async.
    * If closed before the underlying Future completes, will close the resource on completion.
    */
  final class AsyncResource[T <: AutoCloseable](
      start: Future[T]
  )(implicit val loggingContext: LoggingContext) {
    // Must progress as follows:
    //   - Waiting -> Closed,
    //   - Waiting -> Ready -> Closed, or
    //   - Waiting -> Failed.
    private val state: AtomicReference[AsyncResourceState[T]] = new AtomicReference(Waiting)
    private val future = start.andThen {
      case Success(resource) =>
        state.set(Ready(resource))
      case Failure(exception) =>
        state.set(Failed(exception))
    }(ExecutionContext.parasitic)

    private[TrackerMap] def currentState: AsyncResourceState[T] = state.get()

    // This will recurse in the `Waiting` state, but only after `future` completes,
    // which means that the state will have changed to either `Ready` or `Failed`.
    def withResource[U](f: T => Future[U])(implicit ex: ExecutionContext): Future[U] =
      currentState match {
        case Waiting => future.flatMap(_ => withResource(f)) // try again
        case Ready(resource) => f(resource)
        case Failed(exception) => Future.failed(exception)
        case Closed => Future.failed(new IllegalStateException("The resource is closed."))
      }

    def close(): Unit = state.getAndSet(Closed) match {
      case Waiting =>
        try {
          Await.result(
            future.transform(Success(_))(ExecutionContext.parasitic),
            10.seconds,
          ) match {
            case Success(resource) => resource.close()
            case Failure(_) =>
          }
        } catch {
          case _: InterruptedException | _: TimeoutException => // don't worry about it
        }
      case Ready(resource) => resource.close()
      case Failed(_) | Closed =>
    }
  }

  private final class TrackerWithLastSubmission(delegate: Tracker) extends Tracker {
    @volatile private var lastSubmission = System.nanoTime()

    def getLastSubmission: Long = lastSubmission

    override def track(
        submission: CommandSubmission
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
      lastSubmission = System.nanoTime()
      delegate.track(submission)
    }

    override def close(): Unit = delegate.close()
  }
}
