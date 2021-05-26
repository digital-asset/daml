// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.Metric.rounded

final case class CountMetric[T](
    periodMillis: Long,
    countingFunction: T => Int,
    counter: Int = 0,
    lastCount: Int = 0,
) extends Metric[T] {

  override type V = CountMetric.Value

  override def onNext(value: T): CountMetric[T] =
    this.copy(counter = counter + countingFunction(value))

  override def periodicValue(): (Metric[T], CountMetric.Value) =
    (this.copy(lastCount = counter), CountMetric.Value(counter, periodicRate))

  override def finalValue(totalDurationSeconds: Double): CountMetric.Value =
    CountMetric.Value(
      totalCount = counter,
      ratePerSecond = totalRate(totalDurationSeconds),
    )

  private def periodicRate: Double = (counter - lastCount) * 1000.0 / periodMillis

  private def totalRate(totalDurationSeconds: Double): Double = counter / totalDurationSeconds
}

object CountMetric {
  final case class Value(totalCount: Int, ratePerSecond: Double) extends MetricValue {
    override def formatted: List[String] =
      List(
        s"total count: $totalCount [tx]",
        s"rate: ${rounded(ratePerSecond)} [tx/s]",
      )
  }

  def empty[T](
      periodMillis: Long,
      countingFunction: T => Int,
  ): CountMetric[T] = CountMetric[T](periodMillis, countingFunction)
}
