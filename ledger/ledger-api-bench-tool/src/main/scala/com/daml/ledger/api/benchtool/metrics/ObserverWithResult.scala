// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import io.grpc.stub.StreamObserver
import org.slf4j.Logger

import scala.concurrent.{Future, Promise}

class ObserverWithResult[T](logger: Logger) extends StreamObserver[T] {

  private val promise = Promise[Unit]()

  def result: Future[Unit] = promise.future

  override def onNext(value: T): Unit = {
    ()
  }

  override def onError(t: Throwable): Unit = {
    logger.error(s"Received error: $t")
    promise.failure(t)
  }

  override def onCompleted(): Unit = {
    logger.debug(s"Completed.")
    promise.success(())
  }

}
