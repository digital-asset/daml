// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.util

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.utils.ErrorDetails
import io.grpc.stub.StreamObserver
import org.slf4j.Logger

import scala.concurrent.{Future, Promise}

abstract class ObserverWithResult[T, Result](logger: Logger) extends StreamObserver[T] {

  def streamName: String

  def result: Future[Result] = promise.future

  def completeWith(): Future[Result]

  override def onNext(value: T): Unit = ()

  override def onError(t: Throwable): Unit = {
    logger.error(withStreamName(s"Received error: $t"))
    t match {
      case ex: io.grpc.StatusRuntimeException if isServerShuttingDownError(ex) =>
        logger.info(s"Stopping reading the stream due to the server being shut down.")
        promise.completeWith(completeWith())
      case ex =>
        promise.failure(ex)
    }
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
