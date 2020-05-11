// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{MetricRegistry, Timer}

final class DatabaseMetrics private[metrics] (
    registry: MetricRegistry,
    base: MetricName,
    val name: String,
) {

  val overallTimer: Timer = registry.timer(base :+ name)
  val waitTimer: Timer = registry.timer(base :+ name :+ "wait")
  val executionTimer: Timer = registry.timer(base :+ name :+ "exec")
  val deserializationTimer: Timer = registry.timer(base :+ name :+ "deserialization")

}
