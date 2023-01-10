// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.Duration

final case class SizeMetric[T](
    sizingBytesFunction: T => Long,
    currentSizeBytesBucket: Long = 0,
    sizeRateList: List[Double] = List.empty,
) extends Metric[T] {
  import SizeMetric._

  override type V = Value

  override def onNext(value: T): SizeMetric[T] = {
    val addedBytesSize = sizingBytesFunction(value)
    this.copy(currentSizeBytesBucket = currentSizeBytesBucket + addedBytesSize)
  }

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) = {
    val sizeRate = periodicSizeRate(periodDuration)
    val updatedMetric = this.copy(
      currentSizeBytesBucket = 0,
      sizeRateList = sizeRate :: sizeRateList,
    ) // ok to prepend because the list is used only to calculate mean value so the order doesn't matter
    (updatedMetric, Value(sizeRate))
  }

  override def finalValue(totalDuration: Duration): Value = {
    val value = sizeRateList match {
      case Nil => 0.0
      case rates => rates.sum / rates.length
    }
    Value(value)
  }

  override def violatedFinalObjectives(totalDuration: Duration): List[(Objective, Value)] = Nil

  private def periodicSizeRate(periodDuration: Duration): Double =
    (currentSizeBytesBucket.toDouble / periodDuration.toMillis) * 1000.0 / (1024 * 1024)
}

object SizeMetric {
  final case class Value(megabytesPerSecond: Double) extends MetricValue

  def empty[T](sizingFunction: T => Long): SizeMetric[T] =
    SizeMetric[T](sizingFunction)
}
