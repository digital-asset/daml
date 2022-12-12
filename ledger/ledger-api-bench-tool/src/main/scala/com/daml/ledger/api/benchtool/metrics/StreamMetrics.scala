// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object StreamMetrics {

  def observer[StreamElem](
      streamName: String,
      logInterval: FiniteDuration,
      metrics: List[Metric[StreamElem]],
      logger: Logger,
      exposedMetrics: Option[ExposedMetrics[StreamElem]] = None,
      itemCountingFunction: (StreamElem) => Long,
      maxItemCount: Option[Long],
  )(implicit
      system: ActorSystem[SpawnProtocol.Command],
      ec: ExecutionContext,
  ): Future[MeteredStreamObserver[StreamElem]] =
    MetricsManager(streamName, logInterval, metrics, exposedMetrics).map { manager =>
      new MeteredStreamObserver[StreamElem](
        streamName = streamName,
        logger = logger,
        manager = manager,
        itemCountingFunction = itemCountingFunction,
        maxItemCount = maxItemCount,
      )
    }

}
