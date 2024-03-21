// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

/** Implementation of [[StreamObserver]] designed to expose a finite amount of items
  *
  * THIS WILL NEVER COMPLETE IF FED AN UNBOUND STREAM!!!
  */
private[test] final class FiniteStreamObserver[A] extends StreamObserver[A] {

  private[this] val promise = Promise[Vector[A]]()
  private[this] val items = Vector.newBuilder[A]

  val result: Future[Vector[A]] = promise.future

  override def onNext(value: A): Unit = items.synchronized {
    val _ = items += value
  }

  override def onError(t: Throwable): Unit = {
    val _ = promise.tryFailure(t)
  }

  override def onCompleted(): Unit = items.synchronized {
    val _ = promise.trySuccess(items.result())
  }

}
