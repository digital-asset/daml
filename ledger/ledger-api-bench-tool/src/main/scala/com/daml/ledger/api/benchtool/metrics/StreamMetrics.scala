// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.daml.metrics.Metrics
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object StreamMetrics {

  def observer[StreamElem](
      streamName: String,
      logInterval: FiniteDuration,
      metrics: List[Metric[StreamElem]],
      logger: Logger,
      damlMetrics: Metrics,
  )(implicit
      system: ActorSystem[SpawnProtocol.Command],
      ec: ExecutionContext,
  ): Future[MeteredStreamObserver[StreamElem]] =
    MetricsManager(streamName, logInterval, metrics, damlMetrics).map { manager =>
      new MeteredStreamObserver[StreamElem](streamName, logger, manager)
    }

}
