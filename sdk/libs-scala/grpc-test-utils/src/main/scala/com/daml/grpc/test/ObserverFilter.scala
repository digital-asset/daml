// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import io.grpc.stub.StreamObserver

private[test] final class ObserverFilter[A](predicate: A => Boolean)(delegate: StreamObserver[A])
    extends StreamObserver[A] {

  override def onNext(value: A): Unit = synchronized {
    if (predicate(value)) {
      delegate.onNext(value)
    }
  }

  override def onError(t: Throwable): Unit = synchronized {
    delegate.onError(t)
  }

  override def onCompleted(): Unit = synchronized {
    delegate.onCompleted()
  }
}
