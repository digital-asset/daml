// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  Delayed.by(duration)(synchronized {
    onCompleted()
    Context.current().withCancellation().cancel(null)
  })

  override def onNext(value: A): Unit = synchronized {
    delegate.onNext(value)
  }

  override def onError(t: Throwable): Unit = synchronized {
    delegate.onError(t)
  }

  override def onCompleted(): Unit = synchronized {
    delegate.onCompleted()
  }
}
