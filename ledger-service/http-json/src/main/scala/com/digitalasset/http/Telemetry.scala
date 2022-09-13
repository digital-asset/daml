package com.daml.http

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer

abstract class Telemetry {
  def tracer: Tracer
  def openTelemetry: OpenTelemetry
}

object Telemetry {
  case class
  case class NoOpTelemetry
}