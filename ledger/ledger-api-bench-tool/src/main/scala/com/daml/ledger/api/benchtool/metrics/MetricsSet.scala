// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.Config.StreamConfig.Objectives
import com.daml.ledger.api.benchtool.metrics.objectives.{MaxDelay, MinConsumptionSpeed}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.google.protobuf.timestamp.Timestamp

import java.time.Clock

object MetricsSet {
  def transactionMetrics(objectives: Objectives): List[Metric[GetTransactionsResponse]] =
    all[GetTransactionsResponse](
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize.toLong,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
      objectives = objectives,
    )

  def transactionTreesMetrics(
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

  def activeContractsMetrics: List[Metric[GetActiveContractsResponse]] =
    List[Metric[GetActiveContractsResponse]](
      CountRateMetric.empty[GetActiveContractsResponse](
        countingFunction = _.activeContracts.length
      ),
      TotalCountMetric.empty[GetActiveContractsResponse](
        countingFunction = _.activeContracts.length
      ),
      SizeMetric.empty[GetActiveContractsResponse](
        sizingFunction = _.serializedSize.toLong
      ),
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
