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
    val labeledMetricsFactory: LabeledMetricsFactory,
    val registry: MetricRegistry,
) {

  val executorServiceMetrics = new ExecutorServiceMetrics(labeledMetricsFactory)

  object daml {
    val prefix: MetricName = MetricName.Daml

    object commands extends CommandMetrics(prefix :+ "commands", defaultMetricsFactory)

    object execution
        extends ExecutionMetrics(
          prefix :+ "execution",
          defaultMetricsFactory,
          labeledMetricsFactory,
        )

    object lapi extends LAPIMetrics(prefix :+ "lapi", defaultMetricsFactory)

    object userManagement
        extends UserManagementMetrics(
          prefix :+ "user_management",
          labeledMetricsFactory,
        )

    object partyRecordStore
        extends PartyRecordStoreMetrics(
          prefix :+ "party_record_store",
          labeledMetricsFactory,
        )

    object identityProviderConfigStore
        extends IdentityProviderConfigStoreMetrics(
          prefix :+ "identity_provider_config_store",
          labeledMetricsFactory,
        )

    object index
        extends IndexMetrics(prefix :+ "index", defaultMetricsFactory, labeledMetricsFactory)

    object indexer extends IndexerMetrics(prefix :+ "indexer", defaultMetricsFactory)

    object indexerEvents extends IndexedUpdatesMetrics(prefix :+ "indexer", labeledMetricsFactory)

    object parallelIndexer
        extends ParallelIndexerMetrics(
          prefix :+ "parallel_indexer",
          defaultMetricsFactory,
          labeledMetricsFactory,
        )

    object services
        extends ServicesMetrics(prefix :+ "services", defaultMetricsFactory, labeledMetricsFactory)

    object grpc extends DamlGrpcServerMetrics(labeledMetricsFactory, "participant")

    object health extends HealthMetrics(labeledMetricsFactory)

  }
}
