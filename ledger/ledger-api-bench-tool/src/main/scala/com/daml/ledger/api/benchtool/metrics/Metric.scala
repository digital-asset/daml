// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.google.protobuf.timestamp.Timestamp

import java.time.{Duration, Instant}

trait Metric[T] {

  def onNext(value: T): Metric[T]

  def periodicUpdate(): (Metric[T], String)

  def completeInfo(totalDurationSeconds: Double): List[String]

  def name: String = getClass.getSimpleName

}

object Metric {
  case class CountMetric[T](
      periodMillis: Long,
      countingFunction: T => Int,
      counter: Int = 0,
      lastCount: Int = 0,
  ) extends Metric[T] {

    override def onNext(value: T): CountMetric[T] =
      this.copy(counter = counter + countingFunction(value))

    override def periodicUpdate(): (CountMetric[T], String) = {
      val update: String = s"rate: ${rounded(periodicRate)} [tx/s], count: $counter [tx]"
      val updatedMetric = this.copy(lastCount = counter)
      (updatedMetric, update)
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] =
      List(
        s"rate: ${rounded(totalRate(totalDurationSeconds))} [tx/s]",
        s"count: $counter [tx]",
      )

    private def periodicRate: Double = (counter - lastCount) * 1000.0 / periodMillis

    private def totalRate(totalDurationSeconds: Double): Double = counter / totalDurationSeconds
  }

  case class SizeMetric[T](
      periodMillis: Long,
      sizingFunction: T => Int,
      currentSizeBucket: Long = 0,
      sizeRateList: List[Double] = List.empty,
  ) extends Metric[T] {

    override def onNext(value: T): SizeMetric[T] =
      this.copy(currentSizeBucket = currentSizeBucket + sizingFunction(value))

    override def periodicUpdate(): (SizeMetric[T], String) = {
      val sizeRate = periodicSizeRate
      val update = s"size rate (interval): ${rounded(sizeRate)} [MB/s]"
      val updatedMetric = this.copy(
        currentSizeBucket = 0,
        sizeRateList = sizeRate :: sizeRateList,
      ) // ok to prepend because the list is used only to calculate mean value
      (updatedMetric, update)
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] =
      List(s"size rate: $totalSizeRate [MB/s]")

    private def periodicSizeRate: Double = currentSizeBucket * 1000.0 / periodMillis / 1024 / 1024

    private def totalSizeRate: String =
      sizeRateList match {
        case Nil => "not available"
        case rates => s"${rounded(rates.sum / rates.length)}"
      }
  }

  case class DelayMetric[T](
      recordTimeFunction: T => Seq[Timestamp],
      delaysInCurrentInterval: List[Duration] = List.empty,
  ) extends Metric[T] {

    override def onNext(value: T): DelayMetric[T] = {
      val now = Instant.now()
      val newDelays: List[Duration] = recordTimeFunction(value).toList.map { recordTime =>
        Duration.between(
          Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong),
          now,
        )
      }
      this.copy(delaysInCurrentInterval = delaysInCurrentInterval ::: newDelays)
    }

    override def periodicUpdate(): (DelayMetric[T], String) = {
      val update =
        s"mean delay (interval): ${periodicMeanDelay.map(_.getSeconds.toString).getOrElse("-")} [s]"
      val updatedMetric = this.copy(delaysInCurrentInterval = List.empty)
      (updatedMetric, update)
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] = List.empty

    private def periodicMeanDelay: Option[Duration] =
      if (delaysInCurrentInterval.nonEmpty)
        Some(
          delaysInCurrentInterval
            .reduceLeft(_.plus(_))
            .dividedBy(delaysInCurrentInterval.length.toLong)
        )
      else None
  }

  case class ConsumptionSpeedMetric[T](
      periodMillis: Long,
      recordTimeFunction: T => Seq[Timestamp],
      firstRecordTime: Option[Instant] = None,
      lastRecordTime: Option[Instant] = None,
  ) extends Metric[T] {

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

    override def periodicUpdate(): (ConsumptionSpeedMetric[T], String) = {
      val update = s"speed (interval): ${periodicSpeed.map(rounded).getOrElse("-")} [-]"
      val updatedMetric = this.copy(firstRecordTime = None, lastRecordTime = None)
      (updatedMetric, update)
    }

    override def completeInfo(totalDurationSeconds: Double): List[String] = List.empty

    private def periodicSpeed: Option[Double] =
      (firstRecordTime, lastRecordTime) match {
        case (Some(first), Some(last)) =>
          Some((last.toEpochMilli - first.toEpochMilli) * 1.0 / periodMillis)
        case _ =>
          None
      }
  }

  private def rounded(value: Double): String = "%.2f".format(value)
}
