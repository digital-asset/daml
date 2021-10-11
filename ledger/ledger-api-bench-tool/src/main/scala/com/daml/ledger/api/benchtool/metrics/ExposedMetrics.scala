// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.codahale.metrics.{Counter, Histogram, MetricRegistry, SlidingTimeWindowArrayReservoir}
import com.daml.ledger.api.benchtool.util.TimeUtil
import com.daml.metrics.{MetricName, VarGauge}
import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration}
import java.util.concurrent.TimeUnit

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
  private val Prefix: MetricName = MetricName.DAML :+ "bench_tool"

  case class CounterMetric[T](counter: Counter, countingFunction: T => Long)
  case class BytesProcessedMetric[T](bytesProcessed: Counter, sizingFunction: T => Long)
  case class DelayMetric[T](delays: Histogram, recordTimeFunction: T => Seq[Timestamp])
  case class LatestRecordTimeMetric[T](
      latestRecordTime: VarGauge[Long],
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
      counter = registry.counter(Prefix :+ "count" :+ streamName),
      countingFunction = countingFunction,
    )
    val bytesProcessedMetric = BytesProcessedMetric[T](
      bytesProcessed = registry.counter(Prefix :+ "bytes_read" :+ streamName),
      sizingFunction = sizingFunction,
    )
    val delaysHistogram = new Histogram(
      new SlidingTimeWindowArrayReservoir(slidingTimeWindow.toNanos, TimeUnit.NANOSECONDS)
    )
    val delayMetric = recordTimeFunction.map { f =>
      DelayMetric[T](
        delays = registry.register(Prefix :+ "delay" :+ streamName, delaysHistogram),
        recordTimeFunction = f,
      )
    }
    val latestRecordTimeMetric = recordTimeFunction.map { f =>
      LatestRecordTimeMetric[T](
        latestRecordTime =
          registry.register(Prefix :+ "latest_record_time" :+ streamName, new VarGauge[Long](0L)),
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
