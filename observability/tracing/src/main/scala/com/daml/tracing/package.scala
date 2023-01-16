// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Tracer

package object tracing {
  val OpenTelemetryTracer: Tracer = GlobalOpenTelemetry.getTracer("com.daml.telemetry")
}
