// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.{Clock, Duration}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricRegistry, SlidingTimeWindowArrayReservoir}
import com.codahale.{metrics => codahale}
import com.daml.ledger.api.benchtool.util.TimeUtil
import com.daml.metrics.MetricHandle.{
  Counter,
  DropwizardCounter,
  DropwizardGauge,
  DropwizardHistogram,
  Gauge,
  Histogram,
}
import com.daml.metrics.{Gauges, MetricName}
import com.google.protobuf.timestamp.Timestamp

final class ExposedMetrics[T](
    counterMetric: ExposedMetrics.CounterMetric[T],
    bytesProcessedMetric: ExposedMetrics.BytesProcessedMetric[T],
    delayMetric: Option[ExposedMetrics.DelayMetric[T]],
    latestRecordTimeMetric: Option[ExposedMetrics.LatestRecordTimeMetric[T]],
    clock: Clock,
) {
  def onNext(elem: T): Unit = {
    counterMetric.counter.inc(counterMetric.countingFunction(elem))
    bytesProcessedMetric.bytesProcessed.inc(bytesProcessedMetric.sizingFunction(elem))
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

  case class CounterMetric[T](counter: Counter, countingFunction: T => Long)
  case class BytesProcessedMetric[T](bytesProcessed: Counter, sizingFunction: T => Long)
  case class DelayMetric[T](delays: Histogram, recordTimeFunction: T => Seq[Timestamp])
  case class LatestRecordTimeMetric[T](
      latestRecordTime: Gauge[Long],
      recordTimeFunction: T => Seq[Timestamp],
  )

  def apply[T](
      streamName: String,
      registry: MetricRegistry,
      slidingTimeWindow: Duration,
      countingFunction: T => Long,
      sizingFunction: T => Long,
      recordTimeFunction: Option[T => Seq[Timestamp]],
      clock: Clock = Clock.systemUTC(),
  ): ExposedMetrics[T] = {
    val counterMetric = CounterMetric[T](
      counter = DropwizardCounter(
        Prefix :+ "count" :+ streamName,
        registry.counter(Prefix :+ "count" :+ streamName),
      ),
      countingFunction = countingFunction,
    )
    val bytesProcessedMetric = BytesProcessedMetric[T](
      bytesProcessed = DropwizardCounter(
        Prefix :+ "bytes_read" :+ streamName,
        registry.counter(Prefix :+ "bytes_read" :+ streamName),
      ),
      sizingFunction = sizingFunction,
    )
    val delaysHistogram = new codahale.Histogram(
      new SlidingTimeWindowArrayReservoir(slidingTimeWindow.toNanos, TimeUnit.NANOSECONDS)
    )
    val delayMetric = recordTimeFunction.map { f =>
      DelayMetric[T](
        delays = DropwizardHistogram(
          Prefix :+ "delay" :+ streamName,
          registry.register(Prefix :+ "delay" :+ streamName, delaysHistogram),
        ),
        recordTimeFunction = f,
      )
    }
    val latestRecordTimeMetric = recordTimeFunction.map { f =>
      LatestRecordTimeMetric[T](
        latestRecordTime = DropwizardGauge(
          Prefix :+ "latest_record_time" :+ streamName,
          registry
            .register(Prefix :+ "latest_record_time" :+ streamName, new Gauges.VarGauge[Long](0L)),
        ),
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
