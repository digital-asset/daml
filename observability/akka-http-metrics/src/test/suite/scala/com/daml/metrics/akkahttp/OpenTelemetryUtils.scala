// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import scala.annotation.tailrec

import com.daml.metrics.api.MetricName
import com.daml.metrics.api.MetricHandle
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import io.opentelemetry.api.metrics.{Meter => OtelMeter}
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.opentelemetry.sdk.testing.time.TestClock
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/** Data structure used to compare the content of histogram metrics
  * The exact recorded values are not accessible, using the other attributes to check that
  * the data was correctly recorded.
  */
case class HistogramData(
    count: Long,
    sum: Long,
    bucketCounts: List[Long],
)

object HistogramData {
  final val defaultBucketBoundaries =
    List(5L, 10L, 25L, 50L, 75L, 100L, 250L, 500L, 750L, 1000L, 2500L, 5000L, 7500L, 10000L)
  final val bucketCountsZero = List.fill(15)(0L)

  /** Generates a HistogramData instance, from the given value, to compare with the data
    * extracted from a metric.
    */
  def apply(value: Long): HistogramData = {
    HistogramData(List(value))
  }

  /** Generates an HistogramData instance, from the given values, to compare with the data
    * extracted from a metric.
    */
  def apply(values: List[Long]): HistogramData = {
    computeData(values, HistogramData(0, 0, bucketCountsZero))
  }

  @tailrec private def computeData(values: List[Long], acc: HistogramData): HistogramData = {
    values match {
      case head :: tail =>
        computeData(
          tail,
          HistogramData(
            acc.count + 1,
            acc.sum + head,
            updateBucketCounts(acc.bucketCounts, defaultBucketBoundaries, head),
          ),
        )
      case Nil =>
        acc
    }
  }

  private def updateBucketCounts(
      counts: List[Long],
      boundaries: List[Long],
      value: Long,
  ): List[Long] = {
    boundaries match {
      case head :: tail =>
        if (value <= head) {
          (counts.head + 1) :: counts.tail
        } else {
          (counts.head) :: updateBucketCounts(counts.tail, tail, value)
        }
      case Nil =>
        // last bucket
        List(counts.head + 1)
    }
  }
}

/** Base class to manage metrics in a test.
  * Use the metric factory to create the metrics.
  * Use the getCounterValue and getHistogramValues to extract the current data from a metric.
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
    override val prefix: MetricName = MetricName("test")
    override val otelMeter: OtelMeter = sdkMeterProvider.get("test")
  }

  private def metricData(metric: MetricHandle): MetricData = {
    // required to force the in memory reader to report the recent updates
    testClock.advance(Duration.ofNanos(SecondNanos))
    import scala.jdk.CollectionConverters._
    metricReader.collectAllMetrics.asScala.filter(_.getName == metric.name).head
  }

  def getCounterValue(metric: MetricHandle): Long = {
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
