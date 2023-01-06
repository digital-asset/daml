// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.instrumentation.runtimemetrics.{
  BufferPools,
  Classes,
  Cpu,
  GarbageCollector,
  MemoryPools,
  Threads,
}

object JvmMetricSet {
  def registerObservers(openTelemetry: OpenTelemetry): Unit = {
    BufferPools.registerObservers(openTelemetry)
    Classes.registerObservers(openTelemetry)
    Cpu.registerObservers(openTelemetry)
    MemoryPools.registerObservers(openTelemetry)
    Threads.registerObservers(openTelemetry)
    GarbageCollector.registerObservers(openTelemetry)
  }
}
