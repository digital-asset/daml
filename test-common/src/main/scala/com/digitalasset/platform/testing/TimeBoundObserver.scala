// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import com.daml.timer.Delayed
import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

final class TimeBoundObserver[A](
    duration: FiniteDuration
)(delegate: StreamObserver[A])(implicit executionContext: ExecutionContext)
    extends StreamObserver[A] {

  private var done = false

  Delayed.by(duration)(synchronized {
    if (!done) {
      onCompleted()
      val _ = Context.current().withCancellation().cancel(null)
    }
  })

  override def onNext(value: A): Unit = synchronized {
    if (!done) {
      delegate.onNext(value)
    }
  }

  override def onError(t: Throwable): Unit = synchronized {
    if (!done) {
      delegate.onError(t)
      done = true
    }
  }

  override def onCompleted(): Unit = synchronized {
    if (!done) {
      delegate.onCompleted()
      done = true
    }
  }
}
