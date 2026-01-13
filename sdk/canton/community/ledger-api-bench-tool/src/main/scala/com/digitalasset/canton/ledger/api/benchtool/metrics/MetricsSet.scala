// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.*
import com.digitalasset.canton.ledger.api.benchtool.metrics.metrics.TotalRuntimeMetric
import com.digitalasset.canton.ledger.api.benchtool.metrics.metrics.TotalRuntimeMetric.MaxDurationObjective
import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration}
import scala.concurrent.duration.FiniteDuration

object MetricsSet {

  def transactionMetrics(
      configO: Option[TransactionObjectives]
  ): List[Metric[GetUpdatesResponse]] =
    transactionMetrics[GetUpdatesResponse](
      countingFunction = response => countTransactionsEvents(response).toInt,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.update.transaction
        .collect {
          case t if t.effectiveAt.isDefined => t.getEffectiveAt
        }
        .toList,
      configO = configO,
    )

  def transactionExposedMetrics(
      streamName: String,
      metricsFactory: LabeledMetricsFactory,
  ): ExposedMetrics[GetUpdatesResponse] =
    ExposedMetrics[GetUpdatesResponse](
      streamName = streamName,
      factory = metricsFactory,
      countingFunction = countTransactionsEvents,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = Some(
        _.update.transaction
          .collect {
            case t if t.effectiveAt.isDefined => t.getEffectiveAt
          }
          .toList
      ),
    )

  def activeContractsMetrics(
      configO: Option[AcsAndCompletionsObjectives]
  ): List[Metric[GetActiveContractsResponse]] =
    List[Metric[GetActiveContractsResponse]](
      CountRateMetric.empty[GetActiveContractsResponse](
        countingFunction = _.contractEntry.activeContract.knownSize,
        periodicObjectives = Nil,
        finalObjectives = List(
          configO.flatMap(_.minItemRate.map(CountRateMetric.RateObjective.MinRate.apply)),
          configO.flatMap(_.maxItemRate.map(CountRateMetric.RateObjective.MaxRate.apply)),
        ).flatten,
      ),
      TotalCountMetric.empty[GetActiveContractsResponse](
        countingFunction = countActiveContracts
      ),
      SizeMetric.empty[GetActiveContractsResponse](
        sizingFunction = _.serializedSize.toLong
      ),
    ) ++ optionalMaxDurationMetrics(configO)

  def activeContractsExposedMetrics(
      streamName: String,
      metricsFactory: LabeledMetricsFactory,
  ): ExposedMetrics[GetActiveContractsResponse] =
    ExposedMetrics[GetActiveContractsResponse](
      streamName = streamName,
      factory = metricsFactory,
      countingFunction = response => countActiveContracts(response).toLong,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = None,
    )

  def completionsMetrics(
      configO: Option[AcsAndCompletionsObjectives]
  ): List[Metric[CompletionStreamResponse]] =
    List[Metric[CompletionStreamResponse]](
      CountRateMetric.empty(
        countingFunction = _.completionResponse.completion.toList.length,
        periodicObjectives = Nil,
        finalObjectives = List(
          configO.flatMap(_.minItemRate.map(CountRateMetric.RateObjective.MinRate.apply)),
          configO.flatMap(_.maxItemRate.map(CountRateMetric.RateObjective.MaxRate.apply)),
        ).flatten,
      ),
      TotalCountMetric.empty(
        countingFunction = countCompletions
      ),
      SizeMetric.empty(
        sizingFunction = _.serializedSize.toLong
      ),
    ) ++ optionalMaxDurationMetrics(configO)

  def completionsExposedMetrics(
      streamName: String,
      metricsFactory: LabeledMetricsFactory,
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
  ): List[Metric[T]] =
    List[Metric[T]](
      CountRateMetric.empty[T](
        countingFunction = countingFunction,
        periodicObjectives = Nil,
        finalObjectives = List(
          configO.flatMap(_.minItemRate.map(CountRateMetric.RateObjective.MinRate.apply)),
          configO.flatMap(_.maxItemRate.map(CountRateMetric.RateObjective.MaxRate.apply)),
        ).flatten,
      ),
      TotalCountMetric.empty[T](
        countingFunction = countingFunction
      ),
      ConsumptionSpeedMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        objective = configO.flatMap(
          _.minConsumptionSpeed.map(ConsumptionSpeedMetric.MinConsumptionSpeed.apply)
        ),
      ),
      DelayMetric.empty[T](
        recordTimeFunction = recordTimeFunction,
        clock = Clock.systemUTC(),
        objective = configO.flatMap(_.maxDelaySeconds.map(DelayMetric.MaxDelay.apply)),
      ),
      SizeMetric.empty[T](
        sizingFunction = sizingFunction
      ),
    ) ++ optionalMaxDurationMetrics(configO)

  def countActiveContracts(response: GetActiveContractsResponse): Int =
    response.contractEntry.activeContract.knownSize

  def countCompletions(response: CompletionStreamResponse): Int =
    response.completionResponse.completion.toList.size

  def countTransactionsEvents(response: GetUpdatesResponse): Long =
    response.update.transaction.toList.foldLeft(0L)((acc, tx) => acc + tx.events.size)

  private def optionalMaxDurationMetrics[T](
      configO: Option[CommonObjectivesConfig]
  ): List[Metric[T]] = {
    for {
      config <- configO
      maxRuntime <- config.maxTotalStreamRuntimeDuration
    } yield createTotalRuntimeMetric[T](maxRuntime)
  }.toList

  def createTotalRuntimeMetric[T](maxRuntime: FiniteDuration): Metric[T] =
    TotalRuntimeMetric.empty(
      clock = Clock.systemUTC(),
      startTime = Clock.systemUTC().instant(),
      objective = MaxDurationObjective(maxValue = toJavaDuration(maxRuntime)),
    )

  protected[metrics] def toJavaDuration[T](maxStreamDuration: FiniteDuration): Duration =
    Duration.ofNanos(maxStreamDuration.toNanos)
}
