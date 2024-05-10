// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.`export`.MetricExporter

import java.util
import scala.jdk.CollectionConverters.*

class LogReporter(logAsInfo: Boolean, val loggerFactory: NamedLoggerFactory)
    extends MetricExporter
    with NamedLogging
    with NoTracing {

  override def `export`(metrics: util.Collection[MetricData]): CompletableResultCode = {
    MetricValue.allFromMetricData(metrics.asScala).foreach { case (value, metadata) =>
      val str = s"${metadata.getName}: ${value}"
      if (logAsInfo)
        noTracingLogger.info(str)
      else noTracingLogger.debug(str)
    }
    CompletableResultCode.ofSuccess()
  }

  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE
}
