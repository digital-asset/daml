// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.Logger

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Var"))
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
    maxItemCount.filter(_ <= itemsCount).foreach { _ =>
      cancel()
    }
  }

  override def completeWith(): Future[BenchmarkResult] =
    manager.result()

}
