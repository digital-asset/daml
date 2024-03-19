// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.export.{CollectionRegistration, MetricReader}
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

class OpenTelemetryOnDemandMetricsReader extends MetricReader with OnDemandMetricsReader {

  private val logger = LoggerFactory.getLogger(getClass)

  private val optionalProducer = new AtomicReference[Option[CollectionRegistration]](None)

  override def register(registration: CollectionRegistration): Unit =
    optionalProducer.set(Some(registration))

  override def forceFlush(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE

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
