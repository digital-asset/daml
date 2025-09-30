// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.ledger.api.benchtool.util.TimeUtil
import com.google.protobuf.timestamp.Timestamp

import java.time.Clock

final class ExposedMetrics[T](
    counterMetric: ExposedMetrics.CounterMetric[T],
    bytesProcessedMetric: ExposedMetrics.BytesProcessedMetric[T],
    delayMetric: Option[ExposedMetrics.DelayMetric[T]],
    latestRecordTimeMetric: Option[ExposedMetrics.LatestRecordTimeMetric[T]],
    clock: Clock,
) {
  def onNext(elem: T): Unit = {
    counterMetric.counter.inc(counterMetric.countingFunction(elem))(MetricsContext.Empty)
    bytesProcessedMetric.bytesProcessed.inc(bytesProcessedMetric.sizingFunction(elem))(
      MetricsContext.Empty
    )
    delayMetric.foreach { metric =>
      val now = clock.instant()
      metric
        .recordTimeFunction(elem)
        .foreach { recordTime =>
          val delay = TimeUtil.durationBetween(recordTime, now)
          metric.delays.update(delay.getSeconds)
        }
    }
    latestRecordTimeMetric.foreach { metric =>
      metric
        .recordTimeFunction(elem)
        .lastOption
        .foreach(recordTime => metric.latestRecordTime.updateValue(recordTime.seconds))
    }
  }

}

object ExposedMetrics {
  private val Prefix: MetricName = MetricName.Daml :+ "bench_tool"

  final case class CounterMetric[T](counter: Counter, countingFunction: T => Long)
  final case class BytesProcessedMetric[T](bytesProcessed: Counter, sizingFunction: T => Long)
  final case class DelayMetric[T](delays: Histogram, recordTimeFunction: T => Seq[Timestamp])
  final case class LatestRecordTimeMetric[T](
      latestRecordTime: Gauge[Long],
      recordTimeFunction: T => Seq[Timestamp],
  )

  def apply[T](
      streamName: String,
      factory: LabeledMetricsFactory,
      countingFunction: T => Long,
      sizingFunction: T => Long,
      recordTimeFunction: Option[T => Seq[Timestamp]],
      clock: Clock = Clock.systemUTC(),
  ): ExposedMetrics[T] = {
    val counterMetric = CounterMetric[T](
      counter = factory.counter(
        MetricInfo(Prefix :+ "count" :+ streamName, "", MetricQualification.Debug)
      ),
      countingFunction = countingFunction,
    )
    val bytesProcessedMetric = BytesProcessedMetric[T](
      bytesProcessed = factory.counter(
        MetricInfo(Prefix :+ "bytes_read" :+ streamName, "", MetricQualification.Debug)
      ),
      sizingFunction = sizingFunction,
    )
    val delayMetric = recordTimeFunction.map { f =>
      DelayMetric[T](
        delays = factory.histogram(
          MetricInfo(Prefix :+ "delay" :+ streamName, "", MetricQualification.Debug)
        ),
        recordTimeFunction = f,
      )
    }
    val latestRecordTimeMetric = recordTimeFunction.map { f =>
      LatestRecordTimeMetric[T](
        latestRecordTime = factory.gauge(
          MetricInfo(Prefix :+ "latest_record_time" :+ streamName, "", MetricQualification.Debug),
          0L,
        )(MetricsContext.Empty),
        recordTimeFunction = f,
      )
    }

    new ExposedMetrics[T](
      counterMetric = counterMetric,
      bytesProcessedMetric = bytesProcessedMetric,
      delayMetric = delayMetric,
      latestRecordTimeMetric = latestRecordTimeMetric,
      clock = clock,
    )
  }
}
