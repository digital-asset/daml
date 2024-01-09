// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, MetricsFactory}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.daml.metrics.{ExecutorServiceMetrics, HealthMetrics}
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.sdk.metrics.SdkMeterProvider

import scala.annotation.nowarn

object Metrics {

  def apply(registry: MetricRegistry, otelMeter: Meter, reportExecutionContextMetrics: Boolean) =
    new Metrics(
      new DropwizardMetricsFactory(registry),
      new OpenTelemetryMetricsFactory(otelMeter),
      registry,
      reportExecutionContextMetrics,
    )

  lazy val ForTesting: Metrics = {
    val registry = new MetricRegistry
    new Metrics(
      new DropwizardMetricsFactory(registry),
      new OpenTelemetryMetricsFactory(SdkMeterProvider.builder().build().get("for_testing")),
      registry,
      reportExecutionContextMetrics = true,
    )
  }
}

final class Metrics(
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0")
    val defaultMetricsFactory: MetricsFactory,
    val labeledMetricsFactory: LabeledMetricsFactory,
    val registry: MetricRegistry,
    reportExecutionContextMetrics: Boolean,
) {

  private val executorServiceMetricsFactory =
    if (reportExecutionContextMetrics)
      labeledMetricsFactory
    else
      NoOpMetricsFactory

  val executorServiceMetrics = new ExecutorServiceMetrics(executorServiceMetricsFactory)

  object daml {
    val prefix: MetricName = MetricName.Daml

    @nowarn("cat=deprecation")
    object commands extends CommandMetrics(prefix :+ "commands", defaultMetricsFactory)

    @nowarn("cat=deprecation")
    object execution
        extends ExecutionMetrics(
          prefix :+ "execution",
          defaultMetricsFactory,
          labeledMetricsFactory,
        )

    @nowarn("cat=deprecation")
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

    @nowarn("cat=deprecation")
    object index
        extends IndexMetrics(prefix :+ "index", defaultMetricsFactory, labeledMetricsFactory)

    @nowarn("cat=deprecation")
    object indexer extends IndexerMetrics(prefix :+ "indexer", defaultMetricsFactory)

    object indexerEvents extends IndexedUpdatesMetrics(prefix :+ "indexer", labeledMetricsFactory)

    @nowarn("cat=deprecation")
    object parallelIndexer
        extends ParallelIndexerMetrics(
          prefix :+ "parallel_indexer",
          defaultMetricsFactory,
          labeledMetricsFactory,
        )

    @nowarn("cat=deprecation")
    object services
        extends ServicesMetrics(prefix :+ "services", defaultMetricsFactory, labeledMetricsFactory)

    object grpc extends DamlGrpcServerMetrics(labeledMetricsFactory, "participant")

    object health extends HealthMetrics(labeledMetricsFactory)
  }
}
