// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.Config.StreamConfig.Objectives
import com.daml.ledger.api.benchtool.metrics.objectives.{MaxDelay, MinConsumptionSpeed}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.metrics.MetricName
import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration}
import scala.concurrent.duration.FiniteDuration

object MetricsSet {
  def toJavaDuration(duration: FiniteDuration) =
    Duration.ofNanos(duration.toNanos)

  def transactionMetrics(
      streamName: String,
      registry: MetricRegistry,
      objectives: Objectives,
      reportingPeriod: FiniteDuration,
  ): List[Metric[GetTransactionsResponse]] =
    all[GetTransactionsResponse](
      streamName: String,
      registry = registry,
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
      reportingPeriod = toJavaDuration(reportingPeriod),
    )

  def transactionTreesMetrics(
      streamName: String,
      registry: MetricRegistry,
      objectives: Objectives,
      reportingPeriod: FiniteDuration,
  ): List[Metric[GetTransactionTreesResponse]] =
    all[GetTransactionTreesResponse](
      streamName = streamName,
      registry = registry,
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
      reportingPeriod = toJavaDuration(reportingPeriod),
    )

  def activeContractsMetrics: List[Metric[GetActiveContractsResponse]] =
    List[Metric[GetActiveContractsResponse]](
      CountRateMetric.empty[GetActiveContractsResponse](
        countingFunction = _.activeContracts.length
      ),
      TotalCountMetric.empty[GetActiveContractsResponse](
        countingFunction = _.activeContracts.length.toLong
      ),
      SizeMetric.empty[GetActiveContractsResponse](
        sizingFunction = _.serializedSize.toLong
      ),
    )

  def completionsMetrics: List[Metric[CompletionStreamResponse]] =
    List[Metric[CompletionStreamResponse]](
      CountRateMetric.empty(
        countingFunction = _.completions.length
      ),
      TotalCountMetric.empty(
        countingFunction = _.completions.length.toLong
      ),
      SizeMetric.empty(
        sizingFunction = _.serializedSize.toLong
      ),
    )

  private def all[T](
      streamName: String,
      registry: MetricRegistry,
      reportingPeriod: Duration,
      countingFunction: T => Int,
      sizingFunction: T => Long,
      recordTimeFunction: T => Seq[Timestamp],
      objectives: Objectives,
  ): List[Metric[T]] = {
    val Prefix = MetricName.DAML :+ "bench_tool"

    def metricName(metricType: String): MetricName =
      Prefix :+ metricType :+ streamName

    val totalCountMetric = TotalCountMetric.empty[T](
      countingFunction = countingFunction.andThen(_.toLong)
    )
    TotalCountMetric.register(
      metric = totalCountMetric,
      name = metricName("count"),
      registry = registry,
    )

    val sizeMetric = SizeMetric.empty[T](
      sizingFunction = sizingFunction
    )
    SizeMetric.register(
      metric = sizeMetric,
      name = metricName("size"),
      registry = registry,
    )

    val delayMetric = DelayMetric.empty[T](
      recordTimeFunction = recordTimeFunction,
      clock = Clock.systemUTC(),
      objective = objectives.maxDelaySeconds.map(MaxDelay),
      slidingTimeWindow = reportingPeriod, //TODO: consider just fixed value
    )
    DelayMetric.register(
      metric = delayMetric,
      name = metricName("delay"),
      registry = registry,
    )

    List[Metric[T]](
      CountRateMetric.empty[T](
        countingFunction = countingFunction
      ),
      totalCountMetric,
      ConsumptionSpeedMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        objective = objectives.minConsumptionSpeed.map(MinConsumptionSpeed),
      ),
      delayMetric,
      sizeMetric,
    )
  }
}
