// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics.{Gauge, MetricRegistry}
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Tracer

package object metrics {

  val ParticipantTracer: Tracer = GlobalOpenTelemetry.getTracer("participant")

  private[metrics] def registerGauge(
      name: MetricName,
      gaugeSupplier: MetricSupplier[Gauge[_]],
      registry: MetricRegistry,
  ): Unit =
    registry.synchronized {
      registry.remove(name)
      registry.gauge(name, gaugeSupplier)
      ()
    }

}
