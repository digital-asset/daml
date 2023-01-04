// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs

import io.grpc.stub.ClientCallStreamObserver

import scala.concurrent.Promise

class MockClientCallStreamObserver[Request](onRequest: Int => Unit)
    extends ClientCallStreamObserver[Request] {

  private val cancellationPromise = Promise[(String, Throwable)]()

  override def cancel(s: String, throwable: Throwable): Unit = {
    cancellationPromise.trySuccess(s -> throwable)
    ()
  }

  override def isReady: Boolean = ???

  override def setOnReadyHandler(runnable: Runnable): Unit = ???

  override def disableAutoInboundFlowControl(): Unit = ()

  override def request(i: Int): Unit = onRequest(i)

  override def setMessageCompression(b: Boolean): Unit = ???

  override def onNext(v: Request): Unit = ???

  override def onError(throwable: Throwable): Unit = ???

  override def onCompleted(): Unit = ???
}
