// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.slf4j.Logger

import scala.concurrent.{Future, Promise}

// TODO: add metrics
class LogOnlyObserver[T](logger: Logger) extends ManagedStreamObserver[T] {

  // TODO: move this to a generic class
  private val result = Promise[Unit]()

  def completion: Future[Unit] = result.future

  override def onNext(value: T): Unit = {
    logger.debug(s"Received message: ${value.toString.take(20)}")
  }

  override def onError(t: Throwable): Unit = {
    logger.error(s"Received error: $t")
    result.failure(t)
  }

  override def onCompleted(): Unit = {
    logger.debug(s"Stream has completed.")
    result.success(())
  }

}
