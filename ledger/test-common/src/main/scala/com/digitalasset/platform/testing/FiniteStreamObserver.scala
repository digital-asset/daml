// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

/**
  * Implementation of [[StreamObserver]] designed to expose a finite amount of items
  *
  * THIS WILL NEVER COMPLETE IF FED AN UNBOUND STREAM!!!
  */
private[testing] final class FiniteStreamObserver[A] extends StreamObserver[A] {

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
