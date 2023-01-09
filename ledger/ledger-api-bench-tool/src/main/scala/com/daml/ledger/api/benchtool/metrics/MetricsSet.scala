// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.{Clock, Duration}

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig._
import com.daml.ledger.api.benchtool.metrics.metrics.TotalStreamRuntimeMetric
import com.daml.ledger.api.benchtool.metrics.metrics.TotalStreamRuntimeMetric.MaxDurationObjective
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.metrics.api.MetricHandle.Factory
import com.google.protobuf.timestamp.Timestamp

object MetricsSet {

  def transactionMetrics(
      configO: Option[TransactionObjectives]
  ): List[Metric[GetTransactionsResponse]] =
    transactionMetrics[GetTransactionsResponse](
      countingFunction = response => countFlatTransactionsEvents(response).toInt,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      configO = configO,
    )

  def transactionExposedMetrics(
      streamName: String,
      metricsFactory: Factory,
  ): ExposedMetrics[GetTransactionsResponse] =
    ExposedMetrics[GetTransactionsResponse](
      streamName = streamName,
      factory = metricsFactory,
      countingFunction = countFlatTransactionsEvents,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = Some(_.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      }),
    )

  def transactionTreesMetrics(
      configO: Option[TransactionObjectives]
  ): List[Metric[GetTransactionTreesResponse]] =
    transactionMetrics[GetTransactionTreesResponse](
      countingFunction = response => countTreeTransactionsEvents(response).toInt,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      configO = configO,
    )

  def transactionTreesExposedMetrics(
      streamName: String,
      metricsFactory: Factory,
  ): ExposedMetrics[GetTransactionTreesResponse] =
    ExposedMetrics[GetTransactionTreesResponse](
      streamName = streamName,
      factory = metricsFactory,
      countingFunction = countTreeTransactionsEvents,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = Some(_.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      }),
    )

  def activeContractsMetrics(
      configO: Option[AcsAndCompletionsObjectives]
  ): List[Metric[GetActiveContractsResponse]] =
    List[Metric[GetActiveContractsResponse]](
      CountRateMetric.empty[GetActiveContractsResponse](
        countingFunction = _.activeContracts.length,
        periodicObjectives = Nil,
        finalObjectives = List(
          configO.flatMap(_.minItemRate.map(CountRateMetric.RateObjective.MinRate)),
          configO.flatMap(_.maxItemRate.map(CountRateMetric.RateObjective.MaxRate)),
        ).flatten,
      ),
      TotalCountMetric.empty[GetActiveContractsResponse](
        countingFunction = countActiveContracts
      ),
      SizeMetric.empty[GetActiveContractsResponse](
        sizingFunction = _.serializedSize.toLong
      ),
    ) ++ optionalMetrics(configO)

  def activeContractsExposedMetrics(
      streamName: String,
      metricsFactory: Factory,
  ): ExposedMetrics[GetActiveContractsResponse] =
    ExposedMetrics[GetActiveContractsResponse](
      streamName = streamName,
      factory = metricsFactory,
      countingFunction = (response) => countActiveContracts(response).toLong,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = None,
    )

  def completionsMetrics(
      configO: Option[AcsAndCompletionsObjectives]
  ): List[Metric[CompletionStreamResponse]] =
    List[Metric[CompletionStreamResponse]](
      CountRateMetric.empty(
        countingFunction = _.completions.length,
        periodicObjectives = Nil,
        finalObjectives = List(
          configO.flatMap(_.minItemRate.map(CountRateMetric.RateObjective.MinRate)),
          configO.flatMap(_.maxItemRate.map(CountRateMetric.RateObjective.MaxRate)),
        ).flatten,
      ),
      TotalCountMetric.empty(
        countingFunction = countCompletions
      ),
      SizeMetric.empty(
        sizingFunction = _.serializedSize.toLong
      ),
    ) ++ optionalMetrics(configO)

  def completionsExposedMetrics(
      streamName: String,
      metricsFactory: Factory,
  ): ExposedMetrics[CompletionStreamResponse] =
    ExposedMetrics[CompletionStreamResponse](
      streamName = streamName,
      factory = metricsFactory,
      countingFunction = response => countCompletions(response).toLong,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = None,
    )

  private def transactionMetrics[T](
      countingFunction: T => Int,
      sizingFunction: T => Long,
      recordTimeFunction: T => Seq[Timestamp],
      configO: Option[TransactionObjectives],
  ): List[Metric[T]] = {
    List[Metric[T]](
      CountRateMetric.empty[T](
        countingFunction = countingFunction,
        periodicObjectives = Nil,
        finalObjectives = List(
          configO.flatMap(_.minItemRate.map(CountRateMetric.RateObjective.MinRate)),
          configO.flatMap(_.maxItemRate.map(CountRateMetric.RateObjective.MaxRate)),
        ).flatten,
      ),
      TotalCountMetric.empty[T](
        countingFunction = countingFunction
      ),
      ConsumptionSpeedMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        objective =
          configO.flatMap(_.minConsumptionSpeed.map(ConsumptionSpeedMetric.MinConsumptionSpeed)),
      ),
      DelayMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        clock = Clock.systemUTC(),
        objective = configO.flatMap(_.maxDelaySeconds.map(DelayMetric.MaxDelay)),
      ),
      SizeMetric.empty[T](
        sizingFunction = sizingFunction
      ),
    ) ++ optionalMetrics(configO)
  }

  def countActiveContracts(response: GetActiveContractsResponse): Int =
    response.activeContracts.length

  def countCompletions(response: CompletionStreamResponse): Int =
    response.completions.length

  def countFlatTransactionsEvents(response: GetTransactionsResponse): Long =
    response.transactions.foldLeft(0L)((acc, tx) => acc + tx.events.size)

  def countTreeTransactionsEvents(response: GetTransactionTreesResponse): Long =
    response.transactions.foldLeft(0L)((acc, tx) => acc + tx.eventsById.size)

  private def optionalMetrics[T](configO: Option[CommonObjectivesConfig]): List[Metric[T]] =
    configO.flatMap(createTotalRuntimeMetricO[T]).toList

  private def createTotalRuntimeMetricO[T](config: CommonObjectivesConfig): Option[Metric[T]] = {
    config.maxTotalStreamRuntimeDurationInMs.map(maxRuntimeInMillis =>
      TotalStreamRuntimeMetric.empty(
        clock = Clock.systemUTC(),
        startTime = Clock.systemUTC().instant(),
        objective = MaxDurationObjective(Duration.ofMillis(maxRuntimeInMillis)),
      )
    )
  }
}
