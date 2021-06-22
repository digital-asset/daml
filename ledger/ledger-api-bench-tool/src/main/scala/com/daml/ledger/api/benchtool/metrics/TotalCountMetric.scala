// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics
import com.codahale.metrics.{Counter, MetricRegistry}
import java.time.Duration

final case class TotalCountMetric[T](
    countingFunction: T => Long,
    counter: Counter,
    lastCount: Long = 0,
) extends Metric[T] {
  import TotalCountMetric._

  override type V = Value

  override def onNext(value: T): TotalCountMetric[T] = {
    counter.inc(countingFunction(value))
    this
  }

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) =
    (this.copy(lastCount = counter.getCount), Value(counter.getCount))

  override def finalValue(totalDuration: Duration): Value =
    Value(totalCount = counter.getCount)
}

object TotalCountMetric {
  final case class Value(totalCount: Long) extends MetricValue

  def empty[T](
      countingFunction: T => Int
  ): TotalCountMetric[T] = TotalCountMetric[T](countingFunction.andThen(_.toLong), new Counter)

  def register[T](metric: TotalCountMetric[T], name: String, registry: MetricRegistry): Counter = {
    registry.register(name, metric.counter)
  }
}
