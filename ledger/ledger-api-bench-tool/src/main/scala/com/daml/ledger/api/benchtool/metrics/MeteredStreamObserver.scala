// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.Logger

import scala.concurrent.Future

class MeteredStreamObserver[T](
    val streamName: String,
    logger: Logger,
    manager: MetricsManager[T],
) extends ObserverWithResult[T, StreamResult](logger) {

  override def onNext(value: T): Unit = {
    Thread.sleep(1000)
    manager.sendNewValue(value)
    super.onNext(value)
  }

  override def completeWith(): Future[StreamResult] = {
    logger.debug(withStreamName(s"Asking for stream result..."))
    manager.result()
  }

}
