// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import io.grpc.stub.ServerCallStreamObserver

private[auth] final class OngoingAuthorizationObserver[A](
    observer: ServerCallStreamObserver[A],
    claims: Claims,
    authorized: Claims => Either[AuthorizationError, Unit],
    throwOnFailure: AuthorizationError => Throwable)
    extends ServerCallStreamObserver[A] {

  override def isCancelled: Boolean = observer.isCancelled

  override def setOnCancelHandler(runnable: Runnable): Unit = observer.setOnCancelHandler(runnable)

  override def setCompression(s: String): Unit = observer.setCompression(s)

  override def isReady: Boolean = observer.isReady

  override def setOnReadyHandler(runnable: Runnable): Unit = observer.setOnReadyHandler(runnable)

  override def disableAutoInboundFlowControl(): Unit = observer.disableAutoInboundFlowControl()

  override def request(i: Int): Unit = observer.request(i)

  override def setMessageCompression(b: Boolean): Unit = observer.setMessageCompression(b)

  override def onNext(v: A): Unit =
    authorized(claims) match {
      case Right(_) => observer.onNext(v)
      case Left(authorizationError) => observer.onError(throwOnFailure(authorizationError))
    }

  override def onError(throwable: Throwable): Unit = observer.onError(throwable)

  override def onCompleted(): Unit = observer.onCompleted()
}
