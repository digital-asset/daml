// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

object SingleItemObserver {

  def find[A](p: A => Boolean)(attach: StreamObserver[A] => Unit): Future[Option[A]] = {
    val observer = new SingleItemObserver[A](p)
    attach(observer)
    observer.result
  }

  def first[A](attach: StreamObserver[A] => Unit): Future[Option[A]] = {
    val observer = new SingleItemObserver[A](_ => true)
    attach(observer)
    observer.result
  }
}

final class SingleItemObserver[A](predicate: A => Boolean) extends StreamObserver[A] {

  private[this] val promise = Promise[Option[A]]()

  val result: Future[Option[A]] = promise.future

  override def onNext(value: A): Unit = promise.synchronized {
    if (predicate(value)) {
      promise.trySuccess(Some(value))
      val _ = Context.current().withCancellation().cancel(null)
    }
  }

  override def onError(t: Throwable): Unit = promise.synchronized {
    val _ = promise.tryFailure(t)
  }

  override def onCompleted(): Unit = promise.synchronized {
    val _ = promise.trySuccess(None)
  }
}
