// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import io.opentelemetry.api.metrics.Meter

object Metrics {

  def apply(otelMeter: Meter) =
    new Metrics(new OpenTelemetryFactory(otelMeter))

  lazy val ForTesting = new Metrics(
    NoOpMetricsFactory
  )
}

final class Metrics(
    metricsFactory: Factory
) {

  val executorServiceMetrics = new ExecutorServiceMetrics(metricsFactory)

  object test {
    private val prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(prefix, "db", metricsFactory)
  }

  object daml {
    val prefix: MetricName = MetricName.Daml

    object commands extends CommandMetrics(prefix :+ "commands", metricsFactory)

    object execution extends ExecutionMetrics(prefix :+ "execution", metricsFactory)

    object lapi extends LAPIMetrics(prefix :+ "lapi", metricsFactory)

    object userManagement extends UserManagementMetrics(prefix :+ "user_management", metricsFactory)

    object partyRecordStore
        extends PartyRecordStoreMetrics(prefix :+ "party_record_store", metricsFactory)

    object identityProviderConfigStore
        extends IdentityProviderConfigStoreMetrics(
          prefix :+ "identity_provider_config_store",
          metricsFactory,
        )

    object index extends IndexMetrics(prefix :+ "index", metricsFactory)

    object indexer extends IndexerMetrics(prefix :+ "indexer", metricsFactory)

    object indexerEvents extends IndexedUpdatesMetrics(prefix :+ "indexer", metricsFactory)

    object parallelIndexer
        extends ParallelIndexerMetrics(prefix :+ "parallel_indexer", metricsFactory)

    object services extends ServicesMetrics(prefix :+ "services", metricsFactory)

    object grpc extends DamlGrpcServerMetrics(metricsFactory, "participant")

  }
}
