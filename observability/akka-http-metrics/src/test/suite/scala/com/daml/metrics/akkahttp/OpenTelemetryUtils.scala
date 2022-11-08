// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import com.daml.metrics.api.MetricHandle
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import io.opentelemetry.api.metrics.{Meter => OtelMeter}
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.opentelemetry.sdk.testing.time.TestClock
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/** Base class to manage metrics in a test.
  * Use the metric factory to create the metrics.
  * Use the getCurrentValue and getHistogramValues to extract the current data from a metric.
  * @see WebSocketMetricsSpec.TestMetrics
  */
abstract class TestMetricsBase {
  final val SecondNanos = 1_000_000_000L;

  final val testNumbers = new AtomicInteger()

  val metricReader: InMemoryMetricReader = InMemoryMetricReader.create();
  val testClock: TestClock = TestClock.create();
  val sdkMeterProvider: SdkMeterProvider =
    SdkMeterProvider.builder().setClock(testClock).registerMetricReader(metricReader).build();
  val metricFactory = new OpenTelemetryFactory {
    override val otelMeter: OtelMeter = sdkMeterProvider.get("test")
  }

  private def metricData(metric: MetricHandle): MetricData = {
    // required to force the in memory reader to report the recent updates
    testClock.advance(Duration.ofNanos(SecondNanos))
    import scala.jdk.CollectionConverters._
    metricReader.collectAllMetrics.asScala.filter(_.getName == metric.name).head
  }

  def getCurrentValue(metric: MetricHandle): Long = {
    import scala.jdk.CollectionConverters._
    metricData(metric).getLongSumData().getPoints().asScala.headOption.map(_.getValue).getOrElse(0L)
  }

  def getHistogramValues(metric: MetricHandle): HistogramData = {
    import scala.jdk.CollectionConverters._
    val data = metricData(metric)
    val point = data.getHistogramData().getPoints().asScala.head

    HistogramData(
      point.getCount,
      point.getSum.toLong,
      point.getCounts.asScala.map(_.longValue).toList,
    )
  }

}
