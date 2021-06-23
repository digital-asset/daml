// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.codahale.metrics.{Counter, Histogram, MetricRegistry, SlidingTimeWindowArrayReservoir}
import com.daml.ledger.api.benchtool.util.TimeUtil
import com.daml.metrics.{MetricName, VarGauge}
import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration}
import java.util.concurrent.TimeUnit

final class DamlMetrics[T](
    counter: Counter,
    bytesProcessed: Counter,
    delays: Histogram,
    latestRecordTime: VarGauge[Long],
    countingFunction: T => Long,
    sizingFunction: T => Long,
    recordTimeFunction: T => Seq[Timestamp],
    clock: Clock,
) {
  def onNext(elem: T): Unit = {
    counter.inc(countingFunction(elem))
    bytesProcessed.inc(sizingFunction(elem))

    val now = clock.instant()
    val recordTimes = recordTimeFunction(elem).toList
    val recordDelays = recordTimes.map(TimeUtil.durationBetween(_, now))
    recordDelays.foreach(delay => delays.update(delay.getSeconds))
    recordTimes.lastOption
      .foreach(recordTime => latestRecordTime.updateValue(recordTime.seconds))
  }

}

object DamlMetrics {
  private val Prefix: MetricName = MetricName.DAML :+ "bench_tool"

  def apply[T](
      streamName: String,
      registry: MetricRegistry,
      slidingTimeWindow: Duration,
      countingFunction: T => Long,
      sizingFunction: T => Long,
      recordTimeFunction: T => Seq[Timestamp],
      clock: Clock,
  ): DamlMetrics[T] = {
    val counter: Counter = registry.counter(Prefix :+ "count" :+ streamName)
    val bytesProcessed: Counter = registry.counter(Prefix :+ "bytes_read" :+ streamName)
    val histogram = new Histogram(
      new SlidingTimeWindowArrayReservoir(slidingTimeWindow.toNanos, TimeUnit.NANOSECONDS)
    )
    val delays = registry.register(Prefix :+ "delay" :+ streamName, histogram)
    val latestRecordTime: VarGauge[Long] =
      registry.register(Prefix :+ "latest_record_time" :+ streamName, new VarGauge[Long](0L))

    new DamlMetrics[T](
      counter = counter,
      bytesProcessed = bytesProcessed,
      delays = delays,
      latestRecordTime = latestRecordTime,
      countingFunction = countingFunction,
      sizingFunction = sizingFunction,
      recordTimeFunction = recordTimeFunction,
      clock = clock,
    )
  }

}
