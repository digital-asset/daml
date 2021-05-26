// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.Metric.rounded
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant

final case class ConsumptionSpeedMetric[T](
    periodMillis: Long,
    recordTimeFunction: T => Seq[Timestamp],
    firstRecordTime: Option[Instant] = None,
    lastRecordTime: Option[Instant] = None,
) extends Metric[T] {

  override type Value = ConsumptionSpeedMetric.Value

  override def onNext(value: T): ConsumptionSpeedMetric[T] = {
    val recordTimes = recordTimeFunction(value)
    val updatedFirstRecordTime =
      firstRecordTime match {
        case None =>
          recordTimes.headOption.map { recordTime =>
            Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong)
          }
        case recordTime => recordTime
      }
    val updatedLastRecordTime = recordTimes.lastOption.map { recordTime =>
      Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong)
    }
    this.copy(
      firstRecordTime = updatedFirstRecordTime,
      lastRecordTime = updatedLastRecordTime,
    )
  }

  override def periodicValue(): (Metric[T], ConsumptionSpeedMetric.Value) = {
    val updatedMetric = this.copy(firstRecordTime = None, lastRecordTime = None)
    (updatedMetric, ConsumptionSpeedMetric.Value(periodicSpeed))
  }

  override def finalValue(totalDurationSeconds: Double): ConsumptionSpeedMetric.Value =
    ConsumptionSpeedMetric.Value(None)

  private def periodicSpeed: Option[Double] =
    (firstRecordTime, lastRecordTime) match {
      case (Some(first), Some(last)) =>
        Some((last.toEpochMilli - first.toEpochMilli) * 1.0 / periodMillis)
      case _ =>
        Some(0.0)
    }
}

object ConsumptionSpeedMetric {
  final case class Value(relativeSpeed: Option[Double]) extends MetricValue {
    override def formatted: List[String] =
      List(s"speed: ${relativeSpeed.map(rounded).getOrElse("-")} [-]")
  }

  def empty[T](
      periodMillis: Long,
      recordTimeFunction: T => Seq[Timestamp],
  ): ConsumptionSpeedMetric[T] =
    ConsumptionSpeedMetric(periodMillis, recordTimeFunction)
}
