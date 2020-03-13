// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.collection.concurrent
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
    private type Changes = Source[Instant, NotUsed]
    private type SourceQueue = SourceQueueWithComplete[Instant]
    private type SinkQueue = SinkQueueWithCancel[Instant]

    // There is no `TrieSet` type, so we emulate it with a `TrieMap` with meaningless values.
    // Scala doesn't react well to `()` in tuples, so we're using Boolean instead of Unit here.
    private val queues = concurrent.TrieMap[SourceQueue, Boolean]()

    override def getCurrentTime: Instant =
      delegate.getCurrentTime

    override def setCurrentTime(currentTime: Instant, newTime: Instant): Future[Boolean] =
      delegate
        .setCurrentTime(currentTime, newTime)
        .andThen {
          case Success(true) =>
            queues.keys.foreach(_.offer(newTime))
        }(materializer.executionContext)

    override def changes: ResourceOwner[Changes] = new ResourceOwner[Changes] {
      override def acquire()(implicit executionContext: ExecutionContext): Resource[Changes] = {
        val (sourceQueue, sinkQueue) = createQueues()
        sourceQueue.offer(getCurrentTime)
        val changesSource: Source[Instant, NotUsed] =
          Source.unfoldAsync(sinkQueue)(queue => queue.pull().map(_.map(queue -> _)))
        Resource(Future.successful(changesSource))(_ => release(sourceQueue))
      }

      private def createQueues(): (SourceQueue, SinkQueue) = {
        val (sourceQueue, sinkQueue) = Source
          .queue(bufferSize = 1, overflowStrategy = OverflowStrategy.dropHead)
          .toMat(Sink.queue[Instant]())(
            Keep.both[SourceQueueWithComplete[Instant], SinkQueueWithCancel[Instant]])
          .run()
        queues += sourceQueue -> true
        (sourceQueue, sinkQueue)
      }

      private def release(sourceQueue: SourceQueue)(
          implicit executionContext: ExecutionContext
      ): Future[Unit] = {
        queues -= sourceQueue
        sourceQueue.complete()
        sourceQueue.watchCompletion().map(_ => ())
      }
    }
  }
}
