// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import io.grpc.Context
import io.grpc.stub.StreamObserver

private[testing] final class SizeBoundObserver[A](sizeCap: Int)(delegate: StreamObserver[A])
    extends StreamObserver[A] {

  if (sizeCap < 0) {
    throw new IllegalArgumentException(
      s"sizeCap = $sizeCap. The size of the stream cannot be less than 0."
    )
  } else if (sizeCap == 0) {
    delegate.onCompleted()
  }

  private var counter = 0

  override def onNext(value: A): Unit = synchronized {
    delegate.onNext(value)
    counter += 1
    if (counter == sizeCap) {
      delegate.onCompleted()
      val _ = Context.current().withCancellation().cancel(null)
    }
  }

  override def onError(t: Throwable): Unit = synchronized {
    delegate.onError(t)
  }

  override def onCompleted(): Unit = synchronized {
    delegate.onCompleted()
  }
}
