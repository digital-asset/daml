// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait TimeServiceBackend extends TimeProvider {
  def setCurrentTime(currentTime: Instant, newTime: Instant): Future[Boolean]
}

trait ObservedTimeServiceBackend extends TimeServiceBackend {
  def changes: ResourceOwner[Source[Instant, NotUsed]]
}

object TimeServiceBackend {
  def simple(startTime: Instant): TimeServiceBackend =
    new SimpleTimeServiceBackend(startTime)

  def observing(backend: TimeServiceBackend)(
      implicit materializer: Materializer
  ): ObservedTimeServiceBackend =
    new AkkaQueueBasedObservedTimeServiceBackend(backend)

  def withObserver(
      timeProvider: TimeServiceBackend,
      onTimeChange: Instant => Future[Unit]): TimeServiceBackend =
    new ObservingTimeServiceBackend(timeProvider, onTimeChange)

  private final class SimpleTimeServiceBackend(startTime: Instant) extends TimeServiceBackend {
    private val timeRef = new AtomicReference[Instant](startTime)

    override def getCurrentTime: Instant = timeRef.get

    override def setCurrentTime(expectedTime: Instant, newTime: Instant): Future[Boolean] = {
      val currentTime = timeRef.get
      val res = currentTime == expectedTime && timeRef.compareAndSet(currentTime, newTime)
      Future.successful(res)
    }
  }

  private final class AkkaQueueBasedObservedTimeServiceBackend(delegate: TimeServiceBackend)(
      implicit materializer: Materializer
  ) extends ObservedTimeServiceBackend {
    private val queues = mutable.Set[SourceQueueWithComplete[Instant]]()

    override def getCurrentTime: Instant =
      delegate.getCurrentTime

    override def setCurrentTime(currentTime: Instant, newTime: Instant): Future[Boolean] =
      delegate
        .setCurrentTime(currentTime, newTime)
        .andThen {
          case Success(true) =>
            queues.foreach(_.offer(newTime))
        }(materializer.executionContext)

    override def changes: ResourceOwner[Source[Instant, NotUsed]] =
      new ResourceOwner[Source[Instant, NotUsed]] {
        override def acquire()(
            implicit executionContext: ExecutionContext
        ): Resource[Source[Instant, NotUsed]] = {
          Resource(Future {
            val (sourceQueue, sinkQueue) = Source
              .queue(bufferSize = 1, overflowStrategy = OverflowStrategy.dropHead)
              .toMat(Sink.queue[Instant]())(
                Keep.both[SourceQueueWithComplete[Instant], SinkQueueWithCancel[Instant]])
              .run()
            sourceQueue.offer(getCurrentTime)
            queues += sourceQueue
            (sourceQueue, sinkQueue)
          })({
            case (sourceQueue, _) =>
              queues -= sourceQueue
              sourceQueue.complete()
              sourceQueue.watchCompletion().map(_ => ())
          }).map {
            case (_, sinkQueue) =>
              Source.unfoldAsync(sinkQueue)(queue => queue.pull().map(_.map(queue -> _)))
          }
        }
      }
  }

  private final class ObservingTimeServiceBackend(
      timeProvider: TimeServiceBackend,
      onTimeChange: Instant => Future[Unit]
  ) extends TimeServiceBackend {
    override def getCurrentTime: Instant = timeProvider.getCurrentTime

    override def setCurrentTime(expectedTime: Instant, newTime: Instant): Future[Boolean] =
      timeProvider
        .setCurrentTime(expectedTime, newTime)
        .flatMap { success =>
          if (success)
            onTimeChange(expectedTime).map(_ => true)(DirectExecutionContext)
          else Future.successful(false)
        }(DirectExecutionContext)
  }
}
