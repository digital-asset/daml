// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import com.daml.timer.Delayed
import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

final class TimeBoundObserver[T](duration: FiniteDuration, maximumResults: Option[Int] = None)(
    implicit executionContext: ExecutionContext)
    extends StreamObserver[T] {

  private val promise = Promise[Vector[T]]
  private val buffer = Vector.newBuilder[T]
  private var size = 0

  Delayed.by(duration)(onCompleted())

  def result: Future[Vector[T]] = promise.future

  override def onNext(value: T): Unit = {
    buffer += value
    size += 1
    if (maximumResults.exists(size >= _)) {
      onCompleted()
    }
  }

  override def onError(t: Throwable): Unit = {
    val _ = promise.tryFailure(t)
  }

  override def onCompleted(): Unit = {
    promise.trySuccess(buffer.result())
    Context.current().withCancellation().cancel(null)
    ()
  }
}
