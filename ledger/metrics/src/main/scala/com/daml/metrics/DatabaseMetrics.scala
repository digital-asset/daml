// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{MetricRegistry, Timer}

class DatabaseMetrics private[metrics] (
    registry: MetricRegistry,
    prefix: MetricName,
    val name: String,
) {
  protected val dbPrefix: MetricName = prefix :+ name

  val waitTimer: Timer = registry.timer(dbPrefix :+ "wait")
  val executionTimer: Timer = registry.timer(dbPrefix :+ "exec")
  val translationTimer: Timer = registry.timer(dbPrefix :+ "translation")
  val compressionTimer: Timer = registry.timer(dbPrefix :+ "compression")
  val commitTimer: Timer = registry.timer(dbPrefix :+ "commit")
  val queryTimer: Timer = registry.timer(dbPrefix :+ "query")
}

object DatabaseMetrics {

  def ForTesting(metricsName: String): DatabaseMetrics =
    new DatabaseMetrics(
      registry = new MetricRegistry(),
      prefix = MetricName("ForTesting"),
      name = metricsName,
    )
}
