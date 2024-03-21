// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.reporters.MetricsReporter
import pureconfig.{ConfigReader, ConvertHelpers}
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._

final case class HistogramDefinition(nameRegex: String, bucketBoundaries: Seq[Double])

final case class MetricsConfig(
    reporter: MetricsReporter,
    reportingInterval: FiniteDuration,
    histograms: Seq[HistogramDefinition],
)

object MetricsConfig {

  val DefaultMetricsReportingInterval: FiniteDuration = 10.seconds

  implicit val metricReporterReader: ConfigReader[MetricsReporter] = {
    ConfigReader.fromString[MetricsReporter](ConvertHelpers.catchReadError { s =>
      MetricsReporter.parseMetricsReporter(s.toLowerCase())
    })
  }

  implicit val histogramDefinitionReader: ConfigReader[HistogramDefinition] =
    deriveReader[HistogramDefinition]

  implicit val metricsConfigReader: ConfigReader[MetricsConfig] =
    ConfigReader.forProduct3[MetricsConfig, MetricsReporter, FiniteDuration, Option[
      Seq[HistogramDefinition]
    ]](
      "reporter",
      "reporting-interval",
      "histograms",
    ) { (reporter, reportingInterval, optionalHistograms) =>
      MetricsConfig(reporter, reportingInterval, optionalHistograms.getOrElse(Seq.empty))
    }
}
