// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig._
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration}
import scala.concurrent.duration.FiniteDuration

object MetricsSet {
  def transactionMetrics(objectives: TransactionObjectives): List[Metric[GetTransactionsResponse]] =
    transactionMetrics[GetTransactionsResponse](
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
    )

  def transactionExposedMetrics(
      streamName: String,
      registry: MetricRegistry,
      slidingTimeWindow: FiniteDuration,
  ): ExposedMetrics[GetTransactionsResponse] =
    ExposedMetrics[GetTransactionsResponse](
      streamName = streamName,
      registry = registry,
      slidingTimeWindow = Duration.ofNanos(slidingTimeWindow.toNanos),
      countingFunction = _.transactions.length.toLong,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = Some(_.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      }),
    )

  def transactionTreesMetrics(
      objectives: TransactionObjectives
  ): List[Metric[GetTransactionTreesResponse]] =
    transactionMetrics[GetTransactionTreesResponse](
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
    )

  def transactionTreesExposedMetrics(
      streamName: String,
      registry: MetricRegistry,
      slidingTimeWindow: FiniteDuration,
  ): ExposedMetrics[GetTransactionTreesResponse] =
    ExposedMetrics[GetTransactionTreesResponse](
      streamName = streamName,
      registry = registry,
      slidingTimeWindow = Duration.ofNanos(slidingTimeWindow.toNanos),
      countingFunction = _.transactions.length.toLong,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = Some(_.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      }),
    )

  def activeContractsMetrics(objectives: RateObjectives): List[Metric[GetActiveContractsResponse]] =
    List[Metric[GetActiveContractsResponse]](
      CountRateMetric.empty[GetActiveContractsResponse](
        countingFunction = _.activeContracts.length,
        periodicObjectives = Nil,
        finalObjectives = List(
          objectives.minItemRate.map(CountRateMetric.RateObjective.MinRate),
          objectives.maxItemRate.map(CountRateMetric.RateObjective.MaxRate),
        ).flatten,
      ),
      TotalCountMetric.empty[GetActiveContractsResponse](
        countingFunction = _.activeContracts.length
      ),
      SizeMetric.empty[GetActiveContractsResponse](
        sizingFunction = _.serializedSize.toLong
      ),
    )

  def activeContractsExposedMetrics(
      streamName: String,
      registry: MetricRegistry,
      slidingTimeWindow: FiniteDuration,
  ): ExposedMetrics[GetActiveContractsResponse] =
    ExposedMetrics[GetActiveContractsResponse](
      streamName = streamName,
      registry = registry,
      slidingTimeWindow = Duration.ofNanos(slidingTimeWindow.toNanos),
      countingFunction = _.activeContracts.length.toLong,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = None,
    )

  def completionsMetrics(objectives: RateObjectives): List[Metric[CompletionStreamResponse]] =
    List[Metric[CompletionStreamResponse]](
      CountRateMetric.empty(
        countingFunction = _.completions.length,
        periodicObjectives = Nil,
        finalObjectives = List(
          objectives.minItemRate.map(CountRateMetric.RateObjective.MinRate),
          objectives.maxItemRate.map(CountRateMetric.RateObjective.MaxRate),
        ).flatten,
      ),
      TotalCountMetric.empty(
        countingFunction = _.completions.length
      ),
      SizeMetric.empty(
        sizingFunction = _.serializedSize.toLong
      ),
    )

  def completionsExposedMetrics(
      streamName: String,
      registry: MetricRegistry,
      slidingTimeWindow: FiniteDuration,
  ): ExposedMetrics[CompletionStreamResponse] =
    ExposedMetrics[CompletionStreamResponse](
      streamName = streamName,
      registry = registry,
      slidingTimeWindow = Duration.ofNanos(slidingTimeWindow.toNanos),
      countingFunction = _.completions.length.toLong,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = None,
    )

  private def transactionMetrics[T](
      countingFunction: T => Int,
      sizingFunction: T => Long,
      recordTimeFunction: T => Seq[Timestamp],
      objectives: TransactionObjectives,
  ): List[Metric[T]] = {
    List[Metric[T]](
      CountRateMetric.empty[T](
        countingFunction = countingFunction,
        periodicObjectives = Nil,
        finalObjectives = List(
          objectives.minItemRate.map(CountRateMetric.RateObjective.MinRate),
          objectives.maxItemRate.map(CountRateMetric.RateObjective.MaxRate),
        ).flatten,
      ),
      TotalCountMetric.empty[T](
        countingFunction = countingFunction
      ),
      ConsumptionSpeedMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        objective = objectives.minConsumptionSpeed.map(ConsumptionSpeedMetric.MinConsumptionSpeed),
      ),
      DelayMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        clock = Clock.systemUTC(),
        objective = objectives.maxDelaySeconds.map(DelayMetric.MaxDelay),
      ),
      SizeMetric.empty[T](
        sizingFunction = sizingFunction
      ),
    )
  }
}
