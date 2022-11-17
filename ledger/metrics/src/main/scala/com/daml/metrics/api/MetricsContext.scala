// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import io.opentelemetry.api.common.Attributes

/** *
  * Represents labels that are added to metrics
  * Note:
  *  - This is supported only by the OpenTelemetry metrics implementation,
  *  the Dropwizard implementation just ignores the labels as it supports only metric names
  */
case class MetricsContext(labels: Map[String, String]) {

  lazy val asAttributes: Attributes = {
    labels
      .foldLeft(Attributes.builder()) { case (builder, (key, value)) =>
        builder.put(key, value)
      }
      .build()
  }

  /** Merge the current metric context with the given context.
    * This produced labels represent a union of the labels defined by the two contexts,
    * with the label value found in the given context overriding any values with the same key in the current context.
    */
  def merge(context: MetricsContext): MetricsContext = this.copy(labels = labels ++ context.labels)

}

object MetricsContext {

  val Empty: MetricsContext = MetricsContext(Map.empty[String, String])

  def apply(labels: (String, String)*): MetricsContext =
    MetricsContext(labels.toMap)

  def withEmptyMetricsContext[T](run: MetricsContext => T): T = run(Empty)

  def withMetricLabels[T](labels: (String, String)*)(run: MetricsContext => T): T = run(
    MetricsContext(Map(labels: _*))
  )

  def withExtraMetricLabels[T](labels: (String, String)*)(run: MetricsContext => T)(implicit
      metrics: MetricsContext
  ): T = run(
    metrics.merge(MetricsContext(Map(labels: _*)))
  )

}
