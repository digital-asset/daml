// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.util

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.utils.ErrorDetails
import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}
import org.slf4j.Logger

import scala.concurrent.{Future, Promise}

object ClientCancelled extends Exception
abstract class ObserverWithResult[RespT, Result](logger: Logger)
    extends ClientResponseObserver[Any, RespT] {

  private var requestStream: ClientCallStreamObserver[_] = null

  def streamName: String

  def result: Future[Result] = promise.future

  def completeWith(): Future[Result]

  override def onNext(value: RespT): Unit = ()

  override def onError(t: Throwable): Unit = {
    logger.error(withStreamName(s"Received error: $t"))
    t match {
      case ex: io.grpc.StatusRuntimeException if isServerShuttingDownError(ex) =>
        logger.info(s"Stopping reading the stream due to the server being shut down.")
        promise.completeWith(completeWith())
      case ex if ex.getCause == ClientCancelled =>
        logger.info(s"Stopping reading the stream due to a client cancellation.")
        promise.completeWith(completeWith())
      case ex =>
        promise.failure(ex)
    }
  }

  override def beforeStart(requestStream: ClientCallStreamObserver[Any]): Unit = {
    this.requestStream = requestStream
  }

  def cancel(): Unit = {
    requestStream.cancel(null, ClientCancelled)
  }

  private def isServerShuttingDownError(ex: io.grpc.StatusRuntimeException): Boolean =
    ErrorDetails.matches(ex, LedgerApiErrors.ServerIsShuttingDown)

  override def onCompleted(): Unit = {
    logger.info(withStreamName(s"Stream completed."))
    promise.completeWith(completeWith())
  }

  private val promise: Promise[Result] = Promise[Result]()

  protected def withStreamName(message: String) = s"[$streamName] $message"

}
