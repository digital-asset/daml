// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.metrics.{Meter => OtelMeter}
import com.daml.metrics.api.{MetricHandle, MetricName}
import com.daml.metrics.api.dropwizard.DropwizardFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.metrics.grpc.GrpcServerMetrics

object Metrics {
  lazy val ForTesting = new Metrics(new MetricRegistry, GlobalOpenTelemetry.getMeter("test"))
}

final class Metrics(override val registry: MetricRegistry, val otelMeter: OtelMeter)
    extends DropwizardFactory {

  object test {
    private val prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(prefix, "db", registry)
  }

  object daml extends DropwizardFactory {
    val prefix: MetricName = MetricName.Daml
    override val registry = Metrics.this.registry

    object commands extends CommandMetrics(prefix :+ "commands", registry)

    object execution extends ExecutionMetrics(prefix :+ "execution", registry)

    object lapi extends LAPIMetrics(prefix :+ "lapi", registry)

    object userManagement extends UserManagementMetrics(prefix :+ "user_management", registry)

    object partyRecordStore
        extends PartyRecordStoreMetrics(prefix :+ "party_record_store", registry)

    object index extends IndexMetrics(prefix :+ "index", registry)

    object indexer extends IndexerMetrics(prefix :+ "indexer", registry)

    object parallelIndexer extends ParallelIndexerMetrics(prefix :+ "parallel_indexer", registry)

    object services extends ServicesMetrics(prefix :+ "services", registry)

    object HttpJsonApi extends HttpJsonApiMetrics(prefix :+ "http_json_api", registry, otelMeter)

    object grpc extends OpenTelemetryFactory with GrpcServerMetrics {

      private val grpcServerMetricsPrefix = prefix :+ "grpc" :+ "server"

      override def otelMeter: OtelMeter = Metrics.this.otelMeter
      override val callTimer: MetricHandle.Timer = timer(grpcServerMetricsPrefix)
      override val messagesSent: MetricHandle.Meter = meter(
        grpcServerMetricsPrefix :+ "messages" :+ "sent"
      )
      override val messagesReceived: MetricHandle.Meter = meter(
        grpcServerMetricsPrefix :+ "messages" :+ "received"
      )
      override val messagesSentSize: MetricHandle.Histogram = histogram(
        grpcServerMetricsPrefix :+ "messages" :+ "sent" :+ "bytes"
      )
      override val messagesReceivedSize: MetricHandle.Histogram = histogram(
        grpcServerMetricsPrefix :+ "messages" :+ "received" :+ "bytes"
      )
      override val callsStarted: MetricHandle.Meter = meter(
        grpcServerMetricsPrefix :+ "started"
      )
      override val callsFinished: MetricHandle.Meter = meter(
        grpcServerMetricsPrefix :+ "handled"
      )
    }

  }
}
