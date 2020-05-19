// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

private[testing] final class SizeBoundObserver[A](cap: Int, p: A => Boolean)
    extends StreamObserver[A] {

  private val promise = Promise[Vector[A]]()
  private val items = Vector.newBuilder[A]
  private var counter = 0

  val result: Future[Vector[A]] = promise.future

  // Since builders don't guarantee to return a reliable size when asked
  // we rely on a simple mutable variable and synchronize on the promise
  override def onNext(value: A): Unit = promise.synchronized {
    if (p(value)) {
      items += value
      counter += 1
    }
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
