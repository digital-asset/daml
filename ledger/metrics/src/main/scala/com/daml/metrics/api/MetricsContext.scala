// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import io.opentelemetry.api.common.Attributes

case class MetricsContext(labels: Map[String, String]) {

  lazy val asAttributes: Attributes = {
    labels
      .foldLeft(Attributes.builder()) { case (builder, (key, value)) =>
        builder.put(key, value)
      }
      .build()
  }

  def merge(context: MetricsContext): MetricsContext = this.copy(labels = labels ++ context.labels)

}

object MetricsContext {

  val Empty: MetricsContext = MetricsContext(Map.empty)

  def withEmptyMetricsContext[T](run: MetricsContext => T): T = run(Empty)

}
