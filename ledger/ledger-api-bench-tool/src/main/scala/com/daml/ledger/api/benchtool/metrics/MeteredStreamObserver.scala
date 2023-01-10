// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.Logger

import scala.concurrent.Future

class MeteredStreamObserver[T](
    val streamName: String,
    logger: Logger,
    manager: MetricsManager[T],
    itemCountingFunction: T => Long,
    maxItemCount: Option[Long],
) extends ObserverWithResult[T, BenchmarkResult](logger) {
  private var itemsCount = 0L

  override def onNext(value: T): Unit = {
    itemsCount += itemCountingFunction(value)
    manager.sendNewValue(value)
    super.onNext(value)
    if (maxItemCount.isDefined && itemsCount >= maxItemCount.get)
      cancel()
  }

  override def completeWith(): Future[BenchmarkResult] = {
    manager.result()
  }

}
