// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.logging

import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.slf4j.Logger

class ErrorLoggingStreamObserver[T](
    responseObserver: ServerCallStreamObserver[T],
    override protected val logger: Logger)
    extends ServerCallStreamObserver[T]
    with InternalErrorLogging {

  override def isCancelled: Boolean =
    responseObserver.isCancelled
  override def setOnCancelHandler(onCancelHandler: Runnable): Unit =
    responseObserver.setOnCancelHandler(onCancelHandler)
  override def setCompression(compression: String): Unit =
    responseObserver.setCompression(compression)
  override def isReady: Boolean =
    responseObserver.isReady
  override def setOnReadyHandler(onReadyHandler: Runnable): Unit =
    responseObserver.setOnReadyHandler(onReadyHandler)
  override def disableAutoInboundFlowControl(): Unit =
    responseObserver.disableAutoInboundFlowControl()
  override def request(count: Int): Unit =
    responseObserver.request(count)
  override def setMessageCompression(enable: Boolean): Unit =
    responseObserver.setMessageCompression(enable)
  override def onNext(value: T): Unit =
    responseObserver.onNext(value)
  override def onError(t: Throwable): Unit = {
    logIfInternal(t)
    responseObserver.onError(t)
  }
  override def onCompleted(): Unit =
    responseObserver.onCompleted()
}
object ErrorLoggingStreamObserver {

  def apply[T](responseObserver: StreamObserver[T], logger: Logger): ErrorLoggingStreamObserver[T] =
    responseObserver match {
      case s: ServerCallStreamObserver[T] =>
        new ErrorLoggingStreamObserver[T](s, logger)
      case other =>
        throw new IllegalArgumentException(
          s"Expected a ServerCallStreamObserver, received: ${other.getClass}")
    }
}
