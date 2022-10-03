// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.Timer

import com.codahale.metrics.{MetricRegistry}

class DatabaseMetrics private[metrics] (
    override val prefix: MetricName,
    val name: String,
    override val registry: MetricRegistry,
) extends MetricHandle.Factory {
  protected val dbPrefix: MetricName = prefix :+ name

  val waitTimer: Timer = timer(dbPrefix :+ "wait")
  val executionTimer: Timer = timer(dbPrefix :+ "exec")
  val translationTimer: Timer = timer(dbPrefix :+ "translation")
  val compressionTimer: Timer = timer(dbPrefix :+ "compression")
  val commitTimer: Timer = timer(dbPrefix :+ "commit")
  val queryTimer: Timer = timer(dbPrefix :+ "query")
}

object DatabaseMetrics {

  def ForTesting(metricsName: String): DatabaseMetrics =
    new DatabaseMetrics(
      registry = new MetricRegistry(),
      prefix = MetricName("ForTesting"),
      name = metricsName,
    )
}
