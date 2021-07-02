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
      values.map(formattedValue).mkString(", ")

    def finalReport(
        streamName: String,
        metrics: List[Metric[_]],
        duration: Duration,
    ): String = {
      def indented(str: String, spaces: Int = 4): String = s"${" " * spaces}$str"
      val reports = metrics.map { metric =>
        val finalValue = formattedValue(metric.finalValue(duration))

        val violatedObjective: Option[String] = metric.violatedObjective
          .map { case (objective, value) =>
            s"""!!! FAILURE
               |!!! OBJECTIVE NOT MET: ${formattedObjective(objective)} (actual: ${formattedValue(
              value
            )})
               |""".stripMargin
          }

        val all: String = (List(finalValue) ::: violatedObjective.toList)
          .map(indented(_))
          .mkString("\n")

        s"""${metric.name}:
           |$all""".stripMargin
      }
      val reportWidth = 80
      val bar = "=" * reportWidth
      s"""
         |$bar
         |Stream: $streamName
         |Total duration: ${duration.toMillis.toDouble / 1000} [s]
         |${reports.mkString("\n")}
         |$bar""".stripMargin
    }

    private def formattedValue(value: MetricValue): String = value match {
      case v: ConsumptionSpeedMetric.Value =>
        s"speed: ${v.relativeSpeed.map(rounded).getOrElse("-")} [-]"
      case v: CountRateMetric.Value =>
        s"rate: ${rounded(v.ratePerSecond)} [tx/s]"
      case v: DelayMetric.Value =>
        s"mean delay: ${v.meanDelaySeconds.getOrElse("-")} [s]"
      case v: SizeMetric.Value =>
        s"size rate: ${rounded(v.megabytesPerSecond)} [MB/s]"
      case v: TotalCountMetric.Value =>
        s"total count: ${v.totalCount} [tx]"
    }

    private def formattedObjective(objective: ServiceLevelObjective[_]): String =
      objective match {
        case obj: MaxDelay =>
          s"max allowed mean period delay: ${obj.maxDelaySeconds} [s]"
        case obj: MinConsumptionSpeed =>
          s"min allowed period consumption speed: ${obj.minSpeed} [-]"
      }

    private def rounded(value: Double): String = "%.2f".format(value)

  }
}
