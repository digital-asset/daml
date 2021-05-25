// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration, Instant}

trait Metric[Elem] {

  type Value <: Metric.MetricValue

  type Objective <: Metric.ServiceLevelObjective[Value]

  def onNext(value: Elem): Metric[Elem]

  def periodicValue(): (Metric[Elem], Value)

  def finalValue(totalDurationSeconds: Double): Value

  def violatedObjectives: Map[Objective, Value] = Map.empty

  def name: String = getClass.getSimpleName

}

object Metric {
  trait MetricValue {
    def formatted: List[String]
  }

  trait ServiceLevelObjective[MetricValueType <: MetricValue] {
    def isViolatedBy(metricValue: MetricValueType): Boolean
    def maxViolatingValue(first: MetricValueType, second: MetricValueType): MetricValueType
    def formatted: String
  }

  final case class CountMetric[T](
      periodMillis: Long,
      countingFunction: T => Int,
      counter: Int = 0,
      lastCount: Int = 0,
  ) extends Metric[T] {

    override type Value = CountMetric.Value

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

  final case class SizeMetric[T](
      periodMillis: Long,
      sizingBytesFunction: T => Long,
      currentSizeBytesBucket: Long = 0,
      sizeRateList: List[Double] = List.empty,
  ) extends Metric[T] {

    override type Value = SizeMetric.Value

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

  final case class DelayMetric[T](
      recordTimeFunction: T => Seq[Timestamp],
      clock: Clock,
      objectives: Map[ServiceLevelObjective[DelayMetric.Value], Option[DelayMetric.Value]],
      delaysInCurrentInterval: List[Duration] = List.empty,
  ) extends Metric[T] {

    override type Value = DelayMetric.Value
    override type Objective = ServiceLevelObjective[DelayMetric.Value]

    override def onNext(value: T): DelayMetric[T] = {
      val now = clock.instant()
      val newDelays: List[Duration] = recordTimeFunction(value).toList.map { recordTime =>
        Duration.between(
          Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong),
          now,
        )
      }
      this.copy(delaysInCurrentInterval = delaysInCurrentInterval ::: newDelays)
    }

    private def updatedObjectives(
        newValue: DelayMetric.Value
    ): Map[ServiceLevelObjective[DelayMetric.Value], Option[DelayMetric.Value]] = {
      objectives
        .map { case (objective, currentViolatingValue) =>
          // verify if the new value violates objective's requirements
          if (objective.isViolatedBy(newValue)) {
            currentViolatingValue match {
              case None =>
                // if the new value violates objective's requirements and there is no other violating value,
                // record the new value
                objective -> Some(newValue)
              case Some(currentValue) =>
                // if the new value violates objective's requirements and there is already a value that violates
                // requirements, record the maximum value of the two
                objective -> Some(objective.maxViolatingValue(currentValue, newValue))
            }
          } else {
            objective -> currentViolatingValue
          }
        }
    }

    override def periodicValue(): (Metric[T], DelayMetric.Value) = {
      val value: DelayMetric.Value = DelayMetric.Value(periodicMeanDelay.map(_.getSeconds))
      val updatedMetric = this.copy(
        delaysInCurrentInterval = List.empty,
        objectives = updatedObjectives(value),
      )
      (updatedMetric, value)
    }

    override def finalValue(totalDurationSeconds: Double): DelayMetric.Value =
      DelayMetric.Value(None)

    override def violatedObjectives
        : Map[ServiceLevelObjective[DelayMetric.Value], DelayMetric.Value] =
      objectives
        .collect {
          case (objective, value) if value.isDefined => objective -> value.get
        }

    private def periodicMeanDelay: Option[Duration] =
      if (delaysInCurrentInterval.nonEmpty)
        Some(
          delaysInCurrentInterval
            .reduceLeft(_.plus(_))
            .dividedBy(delaysInCurrentInterval.length.toLong)
        )
      else None
  }

  object DelayMetric {

    def empty[T](
        recordTimeFunction: T => Seq[Timestamp],
        objectives: List[ServiceLevelObjective[Value]],
        clock: Clock,
    ): DelayMetric[T] =
      DelayMetric(
        recordTimeFunction = recordTimeFunction,
        clock = clock,
        objectives = objectives.map(objective => objective -> None).toMap,
      )

    final case class Value(meanDelaySeconds: Option[Long]) extends MetricValue {
      override def formatted: List[String] =
        List(s"mean delay: ${meanDelaySeconds.getOrElse("-")} [s]")
    }

    object Value {
      implicit val valueOrdering: Ordering[Value] = (x: Value, y: Value) => {
        (x.meanDelaySeconds, y.meanDelaySeconds) match {
          case (Some(xx), Some(yy)) =>
            if (xx < yy) -1
            else if (xx > yy) 1
            else 0
          case (Some(_), None) => 1
          case (None, Some(_)) => -1
          case (None, None) => 0
        }
      }
    }

    final case class MaxDelay(maxDelaySeconds: Long) extends ServiceLevelObjective[Value] {
      override def isViolatedBy(metricValue: Value): Boolean =
        metricValue.meanDelaySeconds.exists(_ > maxDelaySeconds)

      override def maxViolatingValue(first: Value, second: Value): Value =
        Ordering[Value].max(first, second)

      override def formatted: String =
        s"max allowed delay: $maxDelaySeconds [s]"
    }
  }

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

  private def rounded(value: Double): String = "%.2f".format(value)
}
