// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.metrics.MetricsReporter

import scala.concurrent.duration.{Duration, _}

final case class MetricsConfig(
    reporter: Option[MetricsReporter],
    reportingInterval: Duration,
)

object MetricsConfig {
  val DefaultMetricsConfig: MetricsConfig = MetricsConfig(
    reporter = None,
    reportingInterval = 10.seconds,
  )
}
