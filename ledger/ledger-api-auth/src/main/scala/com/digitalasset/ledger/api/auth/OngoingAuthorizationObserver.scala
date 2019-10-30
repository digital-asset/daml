// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth

import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

final class OngoingAuthorizationObserver[A](
    observer: StreamObserver[A],
    claims: Claims,
    authorized: Claims => Boolean,
    throwOnFailure: => Throwable)
    extends ServerCallStreamObserver[A] {

  private[this] val wrapped = observer match {
    case scso: ServerCallStreamObserver[_] => scso
    case _ => throw new IllegalArgumentException
  }

  override def isCancelled: Boolean = wrapped.isCancelled

  override def setOnCancelHandler(runnable: Runnable): Unit = wrapped.setOnCancelHandler(runnable)

  override def setCompression(s: String): Unit = wrapped.setCompression(s)

  override def isReady: Boolean = wrapped.isReady

  override def setOnReadyHandler(runnable: Runnable): Unit = wrapped.setOnReadyHandler(runnable)

  override def disableAutoInboundFlowControl(): Unit = wrapped.disableAutoInboundFlowControl()

  override def request(i: Int): Unit = wrapped.request(i)

  override def setMessageCompression(b: Boolean): Unit = wrapped.setMessageCompression(b)

  override def onNext(v: A): Unit =
    if (authorized(claims)) observer.onError(throwOnFailure)
    else observer.onNext(v)

  override def onError(throwable: Throwable): Unit = observer.onError(throwable)

  override def onCompleted(): Unit = observer.onCompleted()
}
