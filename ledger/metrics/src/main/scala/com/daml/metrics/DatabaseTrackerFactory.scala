// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.instrumentation.hikaricp.HikariTelemetry

object DatabaseTrackerFactory {

  def metricsTrackerFactory(openTelemetry: OpenTelemetry) = {
    val telemetry = HikariTelemetry.create(openTelemetry)
    telemetry.createMetricsTrackerFactory()
  }

  val ForTesting = metricsTrackerFactory(OpenTelemetry.noop())

}
