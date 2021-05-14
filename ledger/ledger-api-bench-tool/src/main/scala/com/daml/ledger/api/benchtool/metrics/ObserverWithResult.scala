// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import io.grpc.stub.StreamObserver
import org.slf4j.Logger

import scala.concurrent.{Future, Promise}

abstract class ObserverWithResult[T](logger: Logger) extends StreamObserver[T] {

  def streamName: String

  def result: Future[Unit] = promise.future

  private val promise = Promise[Unit]()

  override def onNext(value: T): Unit = {
    ()
  }

  override def onError(t: Throwable): Unit = {
    logger.error(withStreamName(s"Received error: $t"))
    promise.failure(t)
  }

  override def onCompleted(): Unit = {
    logger.debug(withStreamName(s"Completed."))
    promise.success(())
  }

  protected def withStreamName(message: String) = s"[$streamName] $message"

}
