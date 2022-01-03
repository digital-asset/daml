// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.utils

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import io.grpc.stub.StreamObserver

import scala.concurrent.Promise

class BufferingObserver[T](limit: Option[Int] = None) extends StreamObserver[T] {
  private val promise = Promise[Vector[T]]()
  val buffer = new ConcurrentLinkedQueue[T]()
  val size = new AtomicInteger(0)
  def resultsF = promise.future

  override def onError(t: Throwable): Unit = promise.failure(t)

  override def onCompleted(): Unit = {
    val vec = Vector.newBuilder[T]
    buffer.forEach((e) => vec += e)
    promise.trySuccess(vec.result())
    ()
  }

  override def onNext(value: T): Unit = {
    size.updateAndGet(curr => {
      if (limit.fold(false)(_ <= curr)) {
        onCompleted()
        curr
      } else {
        buffer.add(value)
        curr + 1
      }
    })
    ()
  }
}
