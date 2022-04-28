// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.Duration

final case class TotalCountMetric[T](
    countingFunction: T => Int,
    counter: Int = 0,
    lastCount: Int = 0,
) extends Metric[T] {
  import TotalCountMetric._

  override type V = Value

  override def onNext(value: T): TotalCountMetric[T] =
    this.copy(counter = counter + countingFunction(value))

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) =
    (this.copy(lastCount = counter), Value(counter))

  override def finalValue(totalDuration: Duration): Value =
    Value(totalCount = counter)

  override def violatedFinalObjectives(totalDuration: Duration): List[(Objective, Value)] = Nil
}

object TotalCountMetric {
  final case class Value(totalCount: Int) extends MetricValue

  def empty[T](
      countingFunction: T => Int
  ): TotalCountMetric[T] = TotalCountMetric[T](countingFunction)
}
