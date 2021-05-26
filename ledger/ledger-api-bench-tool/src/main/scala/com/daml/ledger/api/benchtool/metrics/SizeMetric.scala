// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.Metric.rounded

final case class SizeMetric[T](
    periodMillis: Long,
    sizingBytesFunction: T => Long,
    currentSizeBytesBucket: Long = 0,
    sizeRateList: List[Double] = List.empty,
) extends Metric[T] {

  override type V = SizeMetric.Value

  override def onNext(value: T): SizeMetric[T] =
    this.copy(currentSizeBytesBucket = currentSizeBytesBucket + sizingBytesFunction(value))

  override def periodicValue(): (Metric[T], SizeMetric.Value) = {
    val sizeRate = periodicSizeRate
    val updatedMetric = this.copy(
      currentSizeBytesBucket = 0,
      sizeRateList = sizeRate :: sizeRateList,
    ) // ok to prepend because the list is used only to calculate mean value so the order doesn't matter
    (updatedMetric, SizeMetric.Value(sizeRate))
  }

  override def finalValue(totalDurationSeconds: Double): SizeMetric.Value = {
    val value = sizeRateList match {
      case Nil => 0.0
      case rates => rates.sum / rates.length
    }
    SizeMetric.Value(value)
  }

  private def periodicSizeRate: Double =
    (currentSizeBytesBucket.toDouble / periodMillis) * 1000.0 / (1024 * 1024)
}

object SizeMetric {
  final case class Value(megabytesPerSecond: Double) extends MetricValue {
    override def formatted: List[String] =
      List(s"size rate: ${rounded(megabytesPerSecond)} [MB/s]")
  }

  def empty[T](periodMillis: Long, sizingFunction: T => Long): SizeMetric[T] =
    SizeMetric[T](periodMillis, sizingFunction)
}
