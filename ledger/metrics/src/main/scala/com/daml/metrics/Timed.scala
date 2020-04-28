// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.scaladsl.{Keep, Source}
import com.codahale.metrics.{Counter, Timer}
import com.daml.dec.DirectExecutionContext

import scala.concurrent.Future

object Timed {

  def value[T](timer: Timer, value: => T): T =
    timer.time(() => value)

  def completionStage[T](timer: Timer, future: => CompletionStage[T]): CompletionStage[T] = {
    val ctx = timer.time()
    future.whenComplete { (_, _) =>
      ctx.stop()
      ()
    }
  }

  def future[T](timer: Timer, future: => Future[T]): Future[T] = {
    val ctx = timer.time()
    val result = future
    result.onComplete(_ => ctx.stop())(DirectExecutionContext)
    result
  }

  def trackedFuture[T](counter: Counter, future: => Future[T]): Future[T] = {
    counter.inc()
    future.andThen { case _ => counter.dec() }(DirectExecutionContext)
  }

  def timedAndTrackedFuture[T](timer: Timer, counter: Counter, future: => Future[T]): Future[T] = {
    Timed.future(timer, trackedFuture(counter, future))
  }

  def source[Out, Mat](timer: Timer, source: => Source[Out, Mat]): Source[Out, Mat] = {
    val ctx = timer.time()
    source
      .watchTermination()(Keep.both[Mat, Future[Done]])
      .mapMaterializedValue {
        case (mat, done) =>
          done.onComplete(_ => ctx.stop())(DirectExecutionContext)
          mat
      }
  }

}
