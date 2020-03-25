// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.scaladsl.{Keep, Source}
import com.codahale.metrics.Timer
import com.digitalasset.dec.DirectExecutionContext

import scala.concurrent.Future

private[state] object Metrics {

  def timedCompletionStage[T](timer: Timer, future: => CompletionStage[T]): CompletionStage[T] = {
    val ctx = timer.time()
    future.whenComplete { (_, _) =>
      ctx.stop()
      ()
    }
  }

  def timedFuture[T](timer: Timer, future: => Future[T]): Future[T] = {
    val ctx = timer.time()
    val result = future
    result.onComplete(_ => ctx.stop())(DirectExecutionContext)
    result
  }

  def timedSource[Out, Mat](timer: Timer, source: => Source[Out, Mat]): Source[Out, Mat] = {
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
