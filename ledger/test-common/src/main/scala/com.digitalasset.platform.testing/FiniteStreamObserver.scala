// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

object FiniteStreamObserver {

  def apply[A](attach: StreamObserver[A] => Unit): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attach(observer)
    observer.result
  }

}

/**
  * Implementation of [[StreamObserver]] designed to expose a finite amount of items
  *
  * THIS WILL NEVER COMPLETE IF FED AN UNBOUND STREAM!!!
  *
  * @tparam A
  */
final class FiniteStreamObserver[A] extends StreamObserver[A] {

  private[this] val promise = Promise[Vector[A]]()
  private[this] val items = Vector.newBuilder[A]

  val result: Future[Vector[A]] = promise.future

  override def onNext(value: A): Unit = {
    val _ = items.synchronized(items += value)
  }

  override def onError(t: Throwable): Unit = promise.failure(t)

  override def onCompleted(): Unit = {
    val _ = items.synchronized(promise.success(items.result()))
  }

}
