// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

object SizeBoundObserver {

  def apply[A](n: Int)(attach: StreamObserver[A] => Unit): Future[Vector[A]] = {
    if (n < 1) {
      Future.failed(
        new IllegalArgumentException(
          s"Invalid argument $n, `take` requires a strictly positive integer as an argument"))
    } else {
      val observer = new SizeBoundObserver[A](n)
      attach(observer)
      observer.result
    }
  }

}

final class SizeBoundObserver[A](cap: Int) extends StreamObserver[A] {

  private[this] val promise = Promise[Vector[A]]()
  private[this] val items = Vector.newBuilder[A]

  val result: Future[Vector[A]] = promise.future
  var counter = 0

  // Since builders don't guarantee to return a reliable size when asked
  // we rely on a simple mutable variable and synchronize on the promise
  override def onNext(value: A): Unit = promise.synchronized {
    items += value
    counter += 1
    if (counter == cap) {
      promise.trySuccess(items.result())
      val _ = Context.current().withCancellation().cancel(null)
    }
  }

  override def onError(t: Throwable): Unit = promise.synchronized {
    val _ = promise.tryFailure(t)
  }

  override def onCompleted(): Unit = promise.synchronized {
    val _ = promise.trySuccess(items.result())
  }
}
