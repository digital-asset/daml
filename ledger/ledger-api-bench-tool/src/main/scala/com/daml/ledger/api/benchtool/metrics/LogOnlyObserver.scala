// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.slf4j.Logger

// TODO: add metrics
class LogOnlyObserver[T](logger: Logger) extends ManagedStreamObserver[T] {

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
