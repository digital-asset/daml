// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.cliopts

import com.daml.metrics.api.reporters.MetricsReporter
import scala.concurrent.duration.{Duration, FiniteDuration, NANOSECONDS}
import scala.util.Try

object Metrics {
  private case class DurationFormat(unwrap: FiniteDuration)

  // We're trying to parse the java duration first for backwards compatibility as
  // removing it and only supporting the scala duration variant would be a breaking change.
  private implicit val scoptDurationFormat: scopt.Read[DurationFormat] = scopt.Read.reads {
    duration =>
      Try {
        Duration.fromNanos(
          java.time.Duration.parse(duration).toNanos
        )
      }.orElse(Try {
        Duration(duration)
      }).flatMap(duration =>
        Try {
          if (!duration.isFinite)
            throw new IllegalArgumentException(s"Input duration $duration is not finite")
          else DurationFormat(FiniteDuration(duration.toNanos, NANOSECONDS))
        }
      ).get
  }

  def metricsReporterParse[C](parser: scopt.OptionParser[C])(
      metricsReporter: Setter[C, Option[MetricsReporter]],
      metricsReportingInterval: Setter[C, FiniteDuration],
  ): Unit = {
    import parser.opt

    opt[MetricsReporter]("metrics-reporter")
      .action((reporter, config) => metricsReporter(_ => Some(reporter), config))
      .optional()
      .text(s"Start a metrics reporter. ${MetricsReporter.cliHint}")

    opt[DurationFormat]("metrics-reporting-interval")
      .action((interval, config) => metricsReportingInterval(_ => interval.unwrap, config))
      .optional()
      .text("Set metric reporting interval.")

    ()
  }
}
