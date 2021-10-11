// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.util

import com.daml.ledger.api.benchtool.metrics._
import com.daml.ledger.api.benchtool.metrics.objectives.{
  MaxDelay,
  MinConsumptionSpeed,
  ServiceLevelObjective,
}

import java.time.Duration

trait MetricReporter {
  def formattedValues(values: List[MetricValue]): String
  def finalReport(streamName: String, metrics: List[Metric[_]], duration: Duration): String
}

object MetricReporter {
  object Default extends MetricReporter {

    def formattedValues(values: List[MetricValue]): String =
      values.map(shortMetricReport).mkString(", ")

    def finalReport(
        streamName: String,
        metrics: List[Metric[_]],
        duration: Duration,
    ): String = {
      def valueFormat(label: String, value: String): String =
        s"""[$streamName][final-value] $label: $value"""
      def failureFormat(info: String): String = s"""[$streamName][failure] $info"""

      val reports = metrics.flatMap { metric =>
        val finalValue = metric.finalValue(duration)

        val valueLog: Option[String] =
          if (includeInFinalReport(metric))
            Some(valueFormat(metricName(metric), formattedValue(finalValue)))
          else
            None

        val violatedObjective: Option[String] = metric.violatedObjective
          .map { case (objective, value) =>
            val info =
              s"${objectiveName(objective)}: required: ${formattedObjectiveValue(objective)}, metered: ${formattedValue(value)}"
            failureFormat(info)
          }

        valueLog.toList ::: violatedObjective.toList
      }

      val durationLog = valueFormat("Duration [s]", s"${duration.toMillis.toDouble / 1000}")
      val reportWidth = 80
      val bar = "=" * reportWidth
      s"""
         |$bar
         | BENCHMARK RESULTS: $streamName
         |$bar
         |$durationLog
         |${reports.mkString("\n")}
         |$bar""".stripMargin
    }

    private def includeInFinalReport(metric: Metric[_]): Boolean = metric match {
      case _: ConsumptionSpeedMetric[_] => false
      case _: DelayMetric[_] => false
      case _ => true
    }

    private def metricName(metric: Metric[_]): String = metric match {
      case _: ConsumptionSpeedMetric[_] => "Consumption speed [-]"
      case _: CountRateMetric[_] => "Item rate [item/s]"
      case _: DelayMetric[_] => "Mean delay [s]"
      case _: SizeMetric[_] => "Size rate [MB/s]"
      case _: TotalCountMetric[_] => "Total item count [item]"
    }

    private def shortMetricReport(value: MetricValue): String =
      s"${shortMetricName(value)}: ${formattedValue(value)}"

    private def shortMetricName(value: MetricValue): String = value match {
      case _: ConsumptionSpeedMetric.Value => "speed [-]"
      case _: CountRateMetric.Value => "rate [item/s]"
      case _: DelayMetric.Value => "delay [s]"
      case _: SizeMetric.Value => "rate [MB/s]"
      case _: TotalCountMetric.Value => "count [item]"
    }

    private def formattedValue(value: MetricValue): String = value match {
      case v: ConsumptionSpeedMetric.Value =>
        s"${v.relativeSpeed.map(rounded).getOrElse("-")}"
      case v: CountRateMetric.Value =>
        s"${rounded(v.ratePerSecond)}"
      case v: DelayMetric.Value =>
        s"${v.meanDelaySeconds.getOrElse("-")}"
      case v: SizeMetric.Value =>
        s"${rounded(v.megabytesPerSecond)}"
      case v: TotalCountMetric.Value =>
        s"${v.totalCount}"
    }

    private def objectiveName(objective: ServiceLevelObjective[_]): String =
      objective match {
        case _: MaxDelay =>
          s"Maximum record time delay [s]"
        case _: MinConsumptionSpeed =>
          s"Minimum consumption speed [-]"
      }

    private def formattedObjectiveValue(objective: ServiceLevelObjective[_]): String =
      objective match {
        case obj: MaxDelay =>
          s"${obj.maxDelaySeconds}"
        case obj: MinConsumptionSpeed =>
          s"${obj.minSpeed}"
      }

    private def rounded(value: Double): String = "%.2f".format(value)

  }
}
