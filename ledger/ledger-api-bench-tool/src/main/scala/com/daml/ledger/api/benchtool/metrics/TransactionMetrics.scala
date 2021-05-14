// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.duration.FiniteDuration

object TransactionMetrics {
  def transactionMetrics(reportingPeriod: FiniteDuration): List[Metric[GetTransactionsResponse]] =
    all[GetTransactionsResponse](
      reportingPeriod = reportingPeriod,
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
    )

  def transactionTreesMetrics(
      reportingPeriod: FiniteDuration
  ): List[Metric[GetTransactionTreesResponse]] =
    all[GetTransactionTreesResponse](
      reportingPeriod = reportingPeriod,
      countingFunction = _.transactions.length,
      sizingFunction = _.serializedSize,
      recordTimeFunction = _.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      },
    )

  private def all[T](
      reportingPeriod: FiniteDuration,
      countingFunction: T => Int,
      sizingFunction: T => Int,
      recordTimeFunction: T => Seq[Timestamp],
  ): List[Metric[T]] = {
    val reportingPeriodMillis = reportingPeriod.toMillis
    List[Metric[T]](
      Metric.TransactionCountMetric(reportingPeriodMillis, countingFunction),
      Metric.TransactionSizeMetric(reportingPeriodMillis, sizingFunction),
      Metric.ConsumptionDelayMetric(recordTimeFunction),
      Metric.ConsumptionSpeedMetric(reportingPeriodMillis, recordTimeFunction),
    )
  }
}
