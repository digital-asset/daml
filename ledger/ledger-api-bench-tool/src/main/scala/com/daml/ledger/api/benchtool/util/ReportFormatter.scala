// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.util

import com.daml.ledger.api.benchtool.metrics.MetricsCollector.Response.{FinalReport, PeriodicReport}
import com.daml.ledger.api.benchtool.metrics._
import com.daml.ledger.api.benchtool.metrics.metrics.TotalStreamRuntimeMetric

object ReportFormatter {
  def formatPeriodicReport(streamName: String, periodicReport: PeriodicReport): String = {
    val values = periodicReport.values.map(shortMetricReport).mkString(", ")
    s"[$streamName] $values"
  }

  def formatFinalReport(streamName: String, finalReport: FinalReport): String = {
    def valueFormat(label: String, value: String): String =
      s"""[$streamName][final-value] $label: $value"""
    def failureFormat(info: String): String = s"""[$streamName][failure] $info"""

    val reports = finalReport.metricsData.flatMap { metricData =>
      val valueLog: Option[String] =
        if (includeInFinalReport(metricData.value))
          Some(valueFormat(metricName(metricData.value), formattedValue(metricData.value)))
        else
          None

      val violatedObjectives: List[String] =
        metricData.violatedObjectives.map { case (objective, value) =>
          val info =
            s"${objectiveName(objective)}: required: ${formattedObjectiveValue(objective)}, metered: ${formattedValue(value)}"
          failureFormat(info)
        }

      valueLog.toList ::: violatedObjectives
    }

    val durationLog =
      valueFormat("Duration [s]", s"${finalReport.totalDuration.toMillis.toDouble / 1000}")
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

  private def includeInFinalReport(value: MetricValue): Boolean = value match {
    case _: ConsumptionSpeedMetric.Value => false
    case _: DelayMetric.Value => false
    case _ => true
  }

  private def metricName(value: MetricValue): String = value match {
    case _: ConsumptionSpeedMetric.Value => "Consumption speed [-]"
    case _: CountRateMetric.Value => "Item rate [item/s]"
    case _: DelayMetric.Value => "Mean delay [s]"
    case _: SizeMetric.Value => "Size rate [MB/s]"
    case _: TotalCountMetric.Value => "Total item count [item]"
    case _: LatencyMetric.Value => "Average latency (millis)"
    case _: TotalStreamRuntimeMetric.Value => "Total stream runtime [ms]"
  }

  private def shortMetricReport(value: MetricValue): String =
    s"${shortMetricName(value)}: ${formattedValue(value)}"

  private def shortMetricName(value: MetricValue): String = value match {
    case _: ConsumptionSpeedMetric.Value => "speed [-]"
    case _: CountRateMetric.Value => "rate [item/s]"
    case _: DelayMetric.Value => "delay [s]"
    case _: SizeMetric.Value => "rate [MB/s]"
    case _: TotalCountMetric.Value => "count [item]"
    case _: LatencyMetric.Value => "Average latency (millis)"
    case _: TotalStreamRuntimeMetric.Value => "Total stream runtime [ms]"
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
    case v: LatencyMetric.Value =>
      s"${v.latencyNanos / 1000000.0d}"
    case v: TotalStreamRuntimeMetric.Value =>
      v.v.toMillis.toString
  }

  private def objectiveName(objective: ServiceLevelObjective[_]): String =
    objective match {
      case _: DelayMetric.MaxDelay =>
        s"Maximum record time delay [s]"
      case _: ConsumptionSpeedMetric.MinConsumptionSpeed =>
        s"Minimum consumption speed [-]"
      case _: CountRateMetric.RateObjective.MinRate =>
        s"Minimum item rate [item/s]"
      case _: CountRateMetric.RateObjective.MaxRate =>
        s"Maximum item rate [item/s]"
      case _: LatencyMetric.MaxLatency =>
        "Maximum latency (millis)"
      case _: TotalStreamRuntimeMetric.MaxDurationObjective =>
        "Total stream runtime [ms]"
    }

  private def formattedObjectiveValue(objective: ServiceLevelObjective[_]): String =
    objective match {
      case obj: DelayMetric.MaxDelay =>
        obj.maxDelaySeconds.toString
      case obj: ConsumptionSpeedMetric.MinConsumptionSpeed =>
        obj.minSpeed.toString
      case obj: CountRateMetric.RateObjective.MinRate =>
        obj.minAllowedRatePerSecond.toString
      case obj: CountRateMetric.RateObjective.MaxRate =>
        obj.minAllowedRatePerSecond.toString
      case obj: LatencyMetric.MaxLatency =>
        obj.millis.toString
      case obj: TotalStreamRuntimeMetric.MaxDurationObjective =>
        obj.maxValue.toMillis.toString
    }

  private def rounded(value: Double): String = "%.2f".format(value)

}
