// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.daml.metrics.{ExecutorServiceMetrics, HealthMetrics}
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.sdk.metrics.SdkMeterProvider

object Metrics {

  def apply(registry: MetricRegistry, otelMeter: Meter, reportExecutionContextMetrics: Boolean) =
    new Metrics(
      new DropwizardMetricsFactory(registry) with LabeledMetricsFactory,
      new OpenTelemetryMetricsFactory(otelMeter),
      registry,
      reportExecutionContextMetrics,
    )

  lazy val ForTesting: Metrics = {
    val registry = new MetricRegistry
    new Metrics(
      new DropwizardMetricsFactory(registry) with LabeledMetricsFactory,
      new OpenTelemetryMetricsFactory(SdkMeterProvider.builder().build().get("for_testing")),
      registry,
      reportExecutionContextMetrics = true,
    )
  }
}

final class Metrics(
    val dropWizardMetricsFactory: LabeledMetricsFactory,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
    val registry: MetricRegistry,
    reportExecutionContextMetrics: Boolean,
) {

  private val executorServiceMetricsFactory =
    if (reportExecutionContextMetrics)
      openTelemetryMetricsFactory
    else
      NoOpMetricsFactory

  val executorServiceMetrics = new ExecutorServiceMetrics(executorServiceMetricsFactory)

  object daml {
    val prefix: MetricName = MetricName.Daml

    object commands extends CommandMetrics(prefix :+ "commands", dropWizardMetricsFactory)

    object execution
        extends ExecutionMetrics(
          prefix :+ "execution",
          dropWizardMetricsFactory,
          openTelemetryMetricsFactory,
        )

    object lapi extends LAPIMetrics(prefix :+ "lapi", dropWizardMetricsFactory)

    object userManagement
        extends UserManagementMetrics(
          prefix :+ "user_management",
          openTelemetryMetricsFactory,
        )

    object partyRecordStore
        extends PartyRecordStoreMetrics(
          prefix :+ "party_record_store",
          openTelemetryMetricsFactory,
        )

    object identityProviderConfigStore
        extends IdentityProviderConfigStoreMetrics(
          prefix :+ "identity_provider_config_store",
          openTelemetryMetricsFactory,
        )

    object index
        extends IndexMetrics(
          prefix :+ "index",
          dropWizardMetricsFactory,
          openTelemetryMetricsFactory,
        )

    object indexer extends IndexerMetrics(prefix :+ "indexer", dropWizardMetricsFactory)

    object indexerEvents
        extends IndexedUpdatesMetrics(prefix :+ "indexer", openTelemetryMetricsFactory)

    object parallelIndexer
        extends ParallelIndexerMetrics(
          prefix :+ "parallel_indexer",
          dropWizardMetricsFactory,
          openTelemetryMetricsFactory,
        )

    object services
        extends ServicesMetrics(
          prefix :+ "services",
          dropWizardMetricsFactory,
          openTelemetryMetricsFactory,
        )

    object grpc extends DamlGrpcServerMetrics(openTelemetryMetricsFactory, "participant")

    object health extends HealthMetrics(openTelemetryMetricsFactory)
  }
}
