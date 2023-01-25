// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.http.DamlHttpMetrics
import io.opentelemetry.api.metrics.{Meter => OtelMeter}

case class Oauth2MiddlewareMetrics(otelMeter: OtelMeter) {

  val openTelemetryFactory = new OpenTelemetryMetricsFactory(otelMeter)

  val http = new DamlHttpMetrics(openTelemetryFactory, "oauth2-middleware")

}
