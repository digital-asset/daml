// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.codahale.metrics.MetricRegistry.MetricSupplier

import io.opentelemetry.OpenTelemetry
import io.opentelemetry.trace.Tracer

package object metrics {

  val OpenTelemetryTracer: Tracer = OpenTelemetry.getTracer("pkv")

  val ApplicationId: String = "daml.application_id"
  val CommandId: String = "daml.command_id"
  val CorrelationId: String = "daml.correlation_id"
  val Submitter: String = "daml.submitter"
  val WorkflowId: String = "daml.workflow.id"

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
