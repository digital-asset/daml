// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import akka.util.Timeout
import com.daml.ledger.api.benchtool.Config.StreamConfig.Objectives
import com.daml.ledger.api.benchtool.metrics.objectives.{MaxDelay, MinConsumptionSpeed}
import com.daml.ledger.api.benchtool.util.MetricReporter
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.google.protobuf.timestamp.Timestamp

import java.time.Clock
import scala.concurrent.Future
import scala.concurrent.duration._

object TransactionMetrics {
  implicit val timeout: Timeout = Timeout(3.seconds)
  import akka.actor.typed.scaladsl.AskPattern._

  def transactionsMetricsManager(
      streamName: String,
      logInterval: FiniteDuration,
      objectives: Objectives,
  )(implicit
      system: ActorSystem[SpawnProtocol.Command]
  ): Future[ActorRef[MetricsManager.Message]] = {
    system.ask(
      SpawnProtocol.Spawn(
        behavior = MetricsManager(
          streamName = streamName,
          metrics = transactionMetrics(objectives),
          logInterval = logInterval,
          reporter = MetricReporter.Default,
        ),
        name = s"${streamName}-manager",
        props = Props.empty,
        _,
      )
    )
  }

  def transactionTreesMetricsManager(
      streamName: String,
      logInterval: FiniteDuration,
      objectives: Objectives,
  )(implicit
      system: ActorSystem[SpawnProtocol.Command]
  ): Future[ActorRef[MetricsManager.Message]] = {
    system.ask(
      SpawnProtocol.Spawn(
        behavior = MetricsManager(
          streamName = streamName,
          metrics = transactionTreesMetrics(objectives),
          logInterval = logInterval,
          reporter = MetricReporter.Default,
        ),
        name = s"${streamName}-manager",
        props = Props.empty,
        _,
      )
    )
  }

  private def transactionMetrics(objectives: Objectives): List[Metric[GetTransactionsResponse]] =
    all[GetTransactionsResponse](
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
    )

  private def transactionTreesMetrics(
      objectives: Objectives
  ): List[Metric[GetTransactionTreesResponse]] =
    all[GetTransactionTreesResponse](
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
    )

  private def all[T](
      countingFunction: T => Int,
      sizingFunction: T => Long,
      recordTimeFunction: T => Seq[Timestamp],
      objectives: Objectives,
  ): List[Metric[T]] = {
    List[Metric[T]](
      CountRateMetric.empty[T](
        countingFunction = countingFunction
      ),
      TotalCountMetric.empty[T](
        countingFunction = countingFunction
      ),
      ConsumptionSpeedMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        objective = objectives.minConsumptionSpeed.map(MinConsumptionSpeed),
      ),
      DelayMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        clock = Clock.systemUTC(),
        objective = objectives.maxDelaySeconds.map(MaxDelay),
      ),
      SizeMetric.empty[T](
        sizingFunction = sizingFunction
      ),
    )
  }
}
