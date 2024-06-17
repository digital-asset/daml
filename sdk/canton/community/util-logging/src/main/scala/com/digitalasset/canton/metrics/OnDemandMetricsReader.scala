// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.`export`.{MetricProducer, MetricReader, MetricReaderFactory}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import java.util.{Timer, TimerTask}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait OnDemandMetricsReader {
  // Return the previous and the current set of metrics
  def read(): (Seq[MetricData], Seq[MetricData])
}

object OnDemandMetricsReader {

  object NoOpOnDemandMetricsReader$ extends OnDemandMetricsReader {
    override def read(): (Seq[MetricData], Seq[MetricData]) = (Seq.empty, Seq.empty)
  }

}

class OpenTelemetryOnDemandMetricsReader(healthDumpMetricFrequency: FiniteDuration)
    extends MetricReaderFactory
    with MetricReader
    with OnDemandMetricsReader {

  private val logger = LoggerFactory.getLogger(getClass)

  private val optionalProducer = new AtomicReference[Option[MetricProducer]](None)
  private val lastRead = new AtomicReference[Seq[MetricData]](Seq.empty)
  private val timer = new Timer()

  override def apply(producer: MetricProducer): MetricReader = {
    timer.schedule(
      new TimerTask {
        override def run(): Unit = {
          lastRead.set(collectMetrics())
        }
      },
      healthDumpMetricFrequency.toMillis,
      healthDumpMetricFrequency.toMillis,
    )
    optionalProducer.set(Some(producer))
    this
  }

  override def getPreferredTemporality: AggregationTemporality = AggregationTemporality.CUMULATIVE

  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def shutdown(): CompletableResultCode = {
    timer.cancel()
    optionalProducer.set(None)
    CompletableResultCode.ofSuccess()
  }

  override def read(): (Seq[MetricData], Seq[MetricData]) = {
    (lastRead.get(), collectMetrics())
  }

  private def collectMetrics(): Seq[MetricData] = {
    optionalProducer
      .get()
      .map { producer =>
        producer.collectAllMetrics().asScala.toSeq
      }
      .getOrElse {
        logger.warn("Could not read metrics as the producer is not set.")
        Seq.empty
      }
  }

}
