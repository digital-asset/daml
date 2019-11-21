// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import io.grpc.stub.StreamObserver

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

object TimeBoundObserver {
  def apply[T](duration: FiniteDuration)(attach: StreamObserver[T] => Unit)(
      implicit executionContext: ExecutionContext): Future[Seq[T]] = {
    val observer = new TimeBoundObserver[T](duration)
    attach(observer)
    observer.future
  }
}

class TimeBoundObserver[T](duration: FiniteDuration)(implicit executionContext: ExecutionContext)
    extends StreamObserver[T] {
  private val promise: Promise[immutable.Seq[T]] = Promise()
  private val buffer: mutable.Buffer[T] = mutable.ListBuffer()

  executionContext.execute(() => {
    Thread.sleep(duration.toMillis)
    onCompleted()
  })

  def future: Future[Seq[T]] = promise.future

  override def onNext(value: T): Unit = {
    buffer += value
  }

  override def onError(t: Throwable): Unit = {
    val _ = promise.tryFailure(t)
  }

  override def onCompleted(): Unit = {
    val _ = promise.trySuccess(buffer.toList)
  }
}
