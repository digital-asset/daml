// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import java.time.{Clock, Instant}

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

class RegularHeartbeat(clock: Clock, interval: FiniteDuration)(implicit materializer: Materializer)
    extends ResourceOwner[Source[Instant, NotUsed]] {
  override def acquire()(
      implicit executionContext: ExecutionContext
  ): Resource[Source[Instant, NotUsed]] =
    Resource(Future {
      Source
        .tick(Duration.Zero, interval, ())
        .map(_ => clock.instant())
        .toMat(Sink.queue[Instant])(Keep.both[Cancellable, SinkQueueWithCancel[Instant]])
        .run()
    })({
      case (cancellable, sinkQueue) =>
        cancellable.cancel()
        sinkQueue.cancel()
        Future.unit
    }).map {
      case (_, sinkQueue) =>
        Source.unfoldAsync(sinkQueue)(queue => queue.pull().map(_.map(queue -> _)))
    }
}
