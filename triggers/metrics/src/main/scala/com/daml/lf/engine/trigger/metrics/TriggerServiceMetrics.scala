// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.metrics

import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.metrics.http.DamlHttpMetrics
import io.opentelemetry.api.metrics.{Meter => OtelMeter}

case class TriggerServiceMetrics(otelMeter: OtelMeter) {

  val openTelemetryFactory = new OpenTelemetryFactory(otelMeter)

  val http = new DamlHttpMetrics(openTelemetryFactory, "trigger-service")

}
