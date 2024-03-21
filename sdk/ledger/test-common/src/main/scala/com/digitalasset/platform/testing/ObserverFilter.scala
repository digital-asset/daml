// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import io.grpc.stub.StreamObserver

private[testing] final class ObserverFilter[A](predicate: A => Boolean)(delegate: StreamObserver[A])
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
