// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{MetricRegistry, Timer}

final class DatabaseMetrics private[metrics] (
    registry: MetricRegistry,
    prefix: MetricName,
    val name: String,
) {

  val waitTimer: Timer = registry.timer(prefix :+ name :+ "wait")
  val executionTimer: Timer = registry.timer(prefix :+ name :+ "exec")
  val translationTimer: Timer = registry.timer(prefix :+ name :+ "translation")

}

object DatabaseMetrics {

  private[metrics] def apply(registry: MetricRegistry, prefix: MetricName)(
      name: String,
  ): DatabaseMetrics =
    new DatabaseMetrics(registry, prefix, name)

}
