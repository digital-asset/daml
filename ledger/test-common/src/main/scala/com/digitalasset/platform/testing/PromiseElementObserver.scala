// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import io.grpc.stub.StreamObserver

import scala.concurrent.Promise

/** Implementation of [[StreamObserver]] designed to expose a promise of a single element
  */
private[testing] final class PromiseElementObserver[A] extends StreamObserver[A] {

  val promise: Promise[A] = Promise[A]()

  override def onNext(value: A): Unit = synchronized {
    val _ = promise.trySuccess(value)
  }

  override def onError(t: Throwable): Unit = synchronized {
    val _ = promise.tryFailure(t)
  }

  override def onCompleted(): Unit = {
    val _ = promise.tryFailure(new Exception("The stream has been completed already"))
  }

}
