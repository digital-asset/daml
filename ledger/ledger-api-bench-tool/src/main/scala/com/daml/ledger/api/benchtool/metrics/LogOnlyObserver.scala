// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.slf4j.LoggerFactory

// TODO: add metrics
class LogOnlyObserver[T] extends ManagedStreamObserver[T] {
  private val logger = LoggerFactory.getLogger(getClass)

  override def onNext(value: T): Unit = {
    logger.debug(s"Received message: $value")
  }

  override def onError(t: Throwable): Unit = {
    logger.error(s"Received error: $t")
  }

  override def onCompleted(): Unit = {
    logger.info(s"Stream has completed.")
  }

}
