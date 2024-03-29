// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.opentelemetry

import java.util
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.`export`.MetricExporter
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters.CollectionHasAsScala

class Slf4jMetricExporter(
    logAsInfo: Boolean,
    logger: Logger = LoggerFactory.getLogger("logging-metrics-exporter"),
) extends MetricExporter {

  override def `export`(
      metrics: util.Collection[MetricData]
  ): CompletableResultCode = {
    (new CompletableResultCode()).whenComplete(() => {
      metrics.asScala.foreach(metricData =>
        if (logAsInfo) logger.info(s"$metricData")
        else logger.debug(s"$metricData")
      )
    })
  }

  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE
}
