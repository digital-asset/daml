// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.prometheus.client.Summary

class DatabaseMetrics private[metrics] (
    prefix: MetricName,
    val name: String,
) {
  protected val dbPrefix: MetricName = prefix :+ name

  val waitTimer: Summary = summary(dbPrefix :+ "wait")
  val executionTimer: Summary = summary(dbPrefix :+ "exec")
  val translationTimer: Summary = summary(dbPrefix :+ "translation")
  val compressionTimer: Summary = summary(dbPrefix :+ "compression")
  val commitTimer: Summary = summary(dbPrefix :+ "commit")
  val queryTimer: Summary = summary(dbPrefix :+ "query")
}


