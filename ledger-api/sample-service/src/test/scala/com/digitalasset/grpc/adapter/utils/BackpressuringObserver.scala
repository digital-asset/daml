// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.utils

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import io.grpc.stub.StreamObserver

class BackpressuringObserver[T](limit: Int) extends StreamObserver[T] {
  private val observedElements = new AtomicInteger()
  val signalDemand = new AtomicReference[Runnable]()

  override def onError(t: Throwable): Unit = throw t

  override def onCompleted(): Unit = ()

  override def onNext(value: T): Unit = {
    if (observedElements.incrementAndGet() < limit) signalDemand.get().run()
  }
}
