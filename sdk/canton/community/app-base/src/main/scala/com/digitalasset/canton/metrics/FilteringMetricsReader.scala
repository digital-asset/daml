// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.{MetricsFilter, MetricsFilterConfig}
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.`export`.{CollectionRegistration, MetricReader}

import java.util
import java.util.stream.Collectors

class FilteringMetricsReader private (filters: Seq[MetricsFilterConfig], parent: MetricReader)
    extends MetricReader {

  private val filter = new MetricsFilter(filters)

  override def register(registration: CollectionRegistration): Unit =
    parent.register(new CollectionRegistration {
      override def collectAllMetrics(): util.Collection[MetricData] = {
        registration
          .collectAllMetrics()
          .stream()
          .filter(x => filter.includeMetric(x.getName))
          .collect(Collectors.toList())
      }
    })

  override def forceFlush(): CompletableResultCode = parent.forceFlush()

  override def shutdown(): CompletableResultCode = parent.shutdown()

  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    parent.getAggregationTemporality(instrumentType)
}

object FilteringMetricsReader {
  def create(filters: Seq[MetricsFilterConfig], parent: MetricReader): MetricReader =
    if (filters.isEmpty) parent else new FilteringMetricsReader(filters, parent)
}
