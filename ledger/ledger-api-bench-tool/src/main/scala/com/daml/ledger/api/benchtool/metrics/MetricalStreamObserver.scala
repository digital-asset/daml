// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.ActorRef
import org.slf4j.Logger

class MetricalStreamObserver[T](
    val streamName: String,
    logger: Logger,
    metricsManager: ActorRef[MetricsManager.Message[T]],
) extends ObserverWithResult[T](logger) {
  import MetricsManager.Message._

  override def onNext(value: T): Unit = {
    metricsManager ! NewValue(value)
    super.onNext(value)
  }

  override def onCompleted(): Unit = {
    logger.debug(withStreamName(s"Sending $StreamCompleted notification."))
    metricsManager ! StreamCompleted()
    super.onCompleted()
  }

}
