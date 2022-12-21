// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.metrics.Meter

object Metrics {

  def apply(registry: MetricRegistry, otelMeter: Meter) =
    new Metrics(new DropwizardMetricsFactory(registry), new OpenTelemetryFactory(otelMeter))

  lazy val ForTesting = new Metrics(
    new DropwizardMetricsFactory(new MetricRegistry),
    new OpenTelemetryFactory(GlobalOpenTelemetry.getMeter("test")),
  )
}

final class Metrics(
    val dropwizardFactory: DropwizardMetricsFactory,
    val openTelemetryFactory: OpenTelemetryFactory,
) {

  val executorServiceMetrics = new ExecutorServiceMetrics(openTelemetryFactory)

  object test {
    private val prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(prefix, "db", dropwizardFactory)
  }

  object daml {
    val prefix: MetricName = MetricName.Daml

    object commands extends CommandMetrics(prefix :+ "commands", dropwizardFactory)

    object execution extends ExecutionMetrics(prefix :+ "execution", dropwizardFactory)

    object lapi extends LAPIMetrics(prefix :+ "lapi", dropwizardFactory)

    object userManagement
        extends UserManagementMetrics(prefix :+ "user_management", dropwizardFactory)

    object partyRecordStore
        extends PartyRecordStoreMetrics(prefix :+ "party_record_store", dropwizardFactory)

    object identityProviderConfigStore
        extends IdentityProviderConfigStoreMetrics(
          prefix :+ "identity_provider_config_store",
          dropwizardFactory,
        )

    object index extends IndexMetrics(prefix :+ "index", dropwizardFactory)

    object indexer extends IndexerMetrics(prefix :+ "indexer", dropwizardFactory)

    object indexerEvents extends IndexedUpdatesMetrics(prefix :+ "indexer", openTelemetryFactory)

    object parallelIndexer
        extends ParallelIndexerMetrics(prefix :+ "parallel_indexer", dropwizardFactory)

    object services extends ServicesMetrics(prefix :+ "services", dropwizardFactory)

    object grpc extends DamlGrpcServerMetrics(openTelemetryFactory, "participant")

  }
}
