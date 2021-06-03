// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.cliopts

import com.daml.metrics.MetricsReporter

import scala.concurrent.duration.FiniteDuration

object Metrics {

  def metricsReporterParse[C](parser: scopt.OptionParser[C])(
      metricsReporter: Setter[C, Option[MetricsReporter]],
      metricsReportingInterval: Setter[C, FiniteDuration],
  ): Unit = {
    import parser.opt

    opt[MetricsReporter]("metrics-reporter")
      .action((reporter, config) => metricsReporter(_ => Some(reporter), config))
      .optional()
      .text(s"Start a metrics reporter. ${MetricsReporter.cliHint}")

    opt[FiniteDuration]("metrics-reporting-interval")
      .action((interval, config) => metricsReportingInterval(_ => interval, config))
      .optional()
      .text("Set metric reporting interval.")
    ()
  }
}
