// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

import akka.stream.Materializer
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.Commands
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
  * @param retentionPeriod The minimum finite duration for which to retain idle trackers.
  */
private[services] final class TrackerMap[Key](
    retentionPeriod: FiniteDuration,
    getKey: Commands => Key,
    newTracker: Key => Future[Tracker],
)(implicit loggingContext: LoggingContext)
    extends Tracker
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val lock = new Object()

  @volatile private var trackerBySubmitter =
    HashMap.empty[Key, TrackerMap.AsyncResource[TrackerWithLastSubmission]]

  require(
    retentionPeriod < Long.MaxValue.nanoseconds,
    s"Retention period $retentionPeriod is too long. Must be below ${Long.MaxValue} nanoseconds.",
  )

  private val retentionNanos = retentionPeriod.toNanos

  override def track(
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
          trackerBySubmitter.getOrElse(key, registerNewTracker(key))
        },
      )
      .withResource(_.track(request))
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
        case Ready(tracker) =>
          if (currentTime - tracker.getLastSubmission > retentionNanos) {
            logger.info(
              s"Shutting down tracker for $submitter after inactivity of $retentionPeriod"
            )(trackerResource.loggingContext)
            trackerBySubmitter -= submitter
            tracker.close()
          }
        case Waiting => // do nothing
        case Failed(_) | Closed =>
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
      retentionPeriod: FiniteDuration,
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
        request: SubmitAndWaitRequest
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] =
      delegate.track(request)

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
      future: Future[T]
  )(implicit val loggingContext: LoggingContext) {
    // Must progress Waiting => Ready => Closed, Waiting => Closed, or Waiting => Failed.
    private val state: AtomicReference[AsyncResourceState[T]] = new AtomicReference(Waiting)

    private val managedFuture = future.andThen {
      case Success(resource) =>
        state.set(Ready(resource))
      case Failure(exception) =>
        state.set(Failed(exception))
    }(DirectExecutionContext)

    private[TrackerMap] def currentState: AsyncResourceState[T] = state.get()

    def withResource[U](f: T => Future[U])(implicit ex: ExecutionContext): Future[U] =
      currentState match {
        case Waiting => managedFuture.flatMap(_ => withResource(f)) // try again
        case Ready(resource) => f(resource)
        case Closed => Future.failed(new IllegalStateException("The resource is closed."))
        case Failed(exception) => Future.failed(exception)
      }

    def close(): Unit = state.getAndSet(Closed) match {
      case Waiting =>
        try {
          Await.result(
            managedFuture.transform(Success(_))(DirectExecutionContext),
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
        request: SubmitAndWaitRequest
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
      lastSubmission = System.nanoTime()
      delegate.track(request)
    }

    override def close(): Unit = delegate.close()
  }
}
