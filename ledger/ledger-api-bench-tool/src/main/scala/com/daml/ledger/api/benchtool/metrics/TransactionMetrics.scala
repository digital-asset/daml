// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import akka.util.Timeout
import com.daml.ledger.api.benchtool.Config.StreamConfig.Objectives
import com.daml.ledger.api.benchtool.metrics.objectives.{MaxDelay, MinConsumptionSpeed}
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
          metrics = transactionMetrics(logInterval, objectives),
          logInterval = logInterval,
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
          metrics = transactionTreesMetrics(logInterval, objectives),
          logInterval = logInterval,
        ),
        name = s"${streamName}-manager",
        props = Props.empty,
        _,
      )
    )
  }

  private def transactionMetrics(
      reportingPeriod: FiniteDuration,
      objectives: Objectives,
  ): List[Metric[GetTransactionsResponse]] =
    all[GetTransactionsResponse](
      reportingPeriod = reportingPeriod,
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
    )

  private def transactionTreesMetrics(
      reportingPeriod: FiniteDuration,
      objectives: Objectives,
  ): List[Metric[GetTransactionTreesResponse]] =
    all[GetTransactionTreesResponse](
      reportingPeriod = reportingPeriod,
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
    )

  private def all[T](
      reportingPeriod: FiniteDuration,
      countingFunction: T => Int,
      sizingFunction: T => Long,
      recordTimeFunction: T => Seq[Timestamp],
      objectives: Objectives,
  ): List[Metric[T]] = {
    val reportingPeriodMillis = reportingPeriod.toMillis
    val delayObjectives =
      objectives.maxDelaySeconds.map(MaxDelay).toList
    val consumptionSpeedObjectives =
      objectives.minConsumptionSpeed.map(MinConsumptionSpeed).toList
    List[Metric[T]](
      CountMetric.empty[T](reportingPeriodMillis, countingFunction),
      SizeMetric.empty[T](reportingPeriodMillis, sizingFunction),
      DelayMetric.empty[T](recordTimeFunction, delayObjectives, Clock.systemUTC()),
      ConsumptionSpeedMetric
        .empty[T](reportingPeriodMillis, recordTimeFunction, consumptionSpeedObjectives),
    )
  }
}
