// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.metrics.{Meter => OtelMeter}

object Metrics {
  lazy val ForTesting = new Metrics(new MetricRegistry, GlobalOpenTelemetry.getMeter("test"))
}

final class Metrics(override val registry: MetricRegistry, val otelMeter: OtelMeter)
    extends DropwizardFactory {

  val openTelemetryFactory: OpenTelemetryFactory = new OpenTelemetryFactory(otelMeter)

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

    object identityProviderConfigStore
        extends IdentityProviderConfigStoreMetrics(
          prefix :+ "identity_provider_config_store",
          registry,
        )

    object index extends IndexMetrics(prefix :+ "index", registry)

    object indexer extends IndexerMetrics(prefix :+ "indexer", registry)

    object indexerEvents extends IndexedUpdatesMetrics(prefix :+ "indexer", openTelemetryFactory)

    object parallelIndexer extends ParallelIndexerMetrics(prefix :+ "parallel_indexer", registry)

    object services extends ServicesMetrics(prefix :+ "services", registry)

    object grpc extends DamlGrpcServerMetrics(openTelemetryFactory, "participant")

  }
}
