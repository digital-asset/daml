// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.prometheus.client.Histogram

class DatabaseMetricsNative private[metrics](
    prefix: MetricName,
    val name: String,
) {
  protected val dbPrefix: MetricName = prefix :+ name
  
  val waitTimer: Histogram = histogram(dbPrefix :+ "wait")
  val executionTimer: Histogram = histogram(dbPrefix :+ "exec")
  val translationTimer: Histogram = histogram(dbPrefix :+ "translation")
  val compressionTimer: Histogram = histogram(dbPrefix :+ "compression")
  val commitTimer: Histogram = histogram(dbPrefix :+ "commit")
  val queryTimer: Histogram = histogram(dbPrefix :+ "query")
}


