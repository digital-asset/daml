// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, MetricsFactory}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.metrics.Meter

object Metrics {

  def apply(registry: MetricRegistry, otelMeter: Meter) =
    new Metrics(
      new DropwizardMetricsFactory(registry),
      new OpenTelemetryMetricsFactory(otelMeter),
      registry,
    )

  lazy val ForTesting: Metrics = {
    val registry = new MetricRegistry
    new Metrics(
      new DropwizardMetricsFactory(registry),
      new OpenTelemetryMetricsFactory(GlobalOpenTelemetry.getMeter("test")),
      registry,
    )
  }
}

final class Metrics(
    val defaultMetricsFactory: MetricsFactory,
    val labelMetricsFactory: LabeledMetricsFactory,
    val registry: MetricRegistry,
) {

  val executorServiceMetrics = new ExecutorServiceMetrics(labelMetricsFactory)

  object test {
    private val prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(prefix, "db", defaultMetricsFactory)
  }

  object daml {
    val prefix: MetricName = MetricName.Daml

    object commands extends CommandMetrics(prefix :+ "commands", defaultMetricsFactory)

    object execution extends ExecutionMetrics(prefix :+ "execution", defaultMetricsFactory)

    object lapi extends LAPIMetrics(prefix :+ "lapi", defaultMetricsFactory)

    object userManagement
        extends UserManagementMetrics(prefix :+ "user_management", defaultMetricsFactory)

    object partyRecordStore
        extends PartyRecordStoreMetrics(prefix :+ "party_record_store", defaultMetricsFactory)

    object identityProviderConfigStore
        extends IdentityProviderConfigStoreMetrics(
          prefix :+ "identity_provider_config_store",
          defaultMetricsFactory,
        )

    object index extends IndexMetrics(prefix :+ "index", defaultMetricsFactory)

    object indexer extends IndexerMetrics(prefix :+ "indexer", defaultMetricsFactory)

    object indexerEvents extends IndexedUpdatesMetrics(prefix :+ "indexer", labelMetricsFactory)

    object parallelIndexer
        extends ParallelIndexerMetrics(prefix :+ "parallel_indexer", defaultMetricsFactory)

    object services
        extends ServicesMetrics(prefix :+ "services", defaultMetricsFactory, labelMetricsFactory)

    object grpc extends DamlGrpcServerMetrics(labelMetricsFactory, "participant")

  }
}
