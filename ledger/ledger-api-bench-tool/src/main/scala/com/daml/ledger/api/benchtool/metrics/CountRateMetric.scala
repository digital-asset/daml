// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics
import java.time.Duration

final case class CountRateMetric[T](
    countingFunction: T => Int,
    counter: Int = 0,
    lastCount: Int = 0,
) extends Metric[T] {
  import CountRateMetric._

  override type V = Value

  override def onNext(value: T): CountRateMetric[T] =
    this.copy(counter = counter + countingFunction(value))

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) =
    (this.copy(lastCount = counter), Value(periodicRate(periodDuration)))

  override def finalValue(totalDuration: Duration): Value =
    Value(ratePerSecond = totalRate(totalDuration))

  private def periodicRate(periodDuration: Duration): Double =
    (counter - lastCount) * 1000.0 / periodDuration.toMillis

  private def totalRate(totalDuration: Duration): Double =
    counter / totalDuration.toMillis.toDouble * 1000.0
}

object CountRateMetric {
  final case class Value(ratePerSecond: Double) extends MetricValue

  def empty[T](
      countingFunction: T => Int
  ): CountRateMetric[T] = CountRateMetric[T](countingFunction)
}
