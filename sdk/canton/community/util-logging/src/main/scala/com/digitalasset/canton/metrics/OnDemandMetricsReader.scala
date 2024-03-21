// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.`export`.{MetricProducer, MetricReader, MetricReaderFactory}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait OnDemandMetricsReader {
  def read(): Seq[MetricData]
}

object OnDemandMetricsReader {

  object NoOpOnDemandMetricsReader$ extends OnDemandMetricsReader {
    override def read(): Seq[MetricData] = Seq.empty
  }

}

class OpenTelemetryOnDemandMetricsReader
    extends MetricReaderFactory
    with MetricReader
    with OnDemandMetricsReader {

  private val logger = LoggerFactory.getLogger(getClass)

  private val optionalProducer = new AtomicReference[Option[MetricProducer]](None)

  override def apply(producer: MetricProducer): MetricReader = {
    optionalProducer.set(Some(producer))
    this
  }

  override def getPreferredTemporality: AggregationTemporality = AggregationTemporality.CUMULATIVE

  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def shutdown(): CompletableResultCode = {
    optionalProducer.set(None)
    CompletableResultCode.ofSuccess()
  }

  override def read(): Seq[MetricData] = {
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
