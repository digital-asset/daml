// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.tracing

import com.daml.http.tracing.TracingConfig.Tracer

case class TracingConfig(tracer: Tracer = Tracer())

object TracingConfig {

  /** Configuration for when trace context should be propagated */
  sealed trait Propagation
  object Propagation {
    case object Enabled extends Propagation
    case object Disabled extends Propagation
  }

  case class Tracer(exporter: Exporter = Exporter.Disabled, sampler: Sampler = Sampler.AlwaysOn())

  sealed trait Sampler {
    def parentBased: Boolean
  }
  object Sampler {
    final case class AlwaysOn(parentBased: Boolean = true) extends Sampler
    final case class AlwaysOff(parentBased: Boolean = true) extends Sampler
    final case class TraceIdRatio(ratio: Double, parentBased: Boolean = true) extends Sampler
  }

  /** Configuration for how to export spans */
  sealed trait Exporter
  object Exporter {
    final case object Disabled extends Exporter
    final case class Jaeger(address: String = "localhost", port: Int = 14250) extends Exporter
    final case class Zipkin(address: String = "localhost", port: Int = 9411) extends Exporter
  }
}
