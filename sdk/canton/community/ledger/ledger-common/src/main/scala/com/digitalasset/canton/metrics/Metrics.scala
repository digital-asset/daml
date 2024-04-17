// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.sdk.metrics.SdkMeterProvider

object Metrics extends LazyLogging {

  lazy val KNOWN_METRICS = Set("booh")

  def apply(prefix: MetricName, otelMeter: Meter) =
    new Metrics(
      prefix,
      new OpenTelemetryMetricsFactory(otelMeter, KNOWN_METRICS, Some(logger.underlying)),
    )

  lazy val ForTesting: Metrics = {
    new Metrics(
      MetricName("est"),
      new OpenTelemetryMetricsFactory(
        SdkMeterProvider.builder().build().get("for_testing"),
        KNOWN_METRICS,
        Some(logger.underlying),
      ),
    )
  }
}

final class Metrics(prefix: MetricName, val openTelemetryMetricsFactory: LabeledMetricsFactory) {

  object commands extends CommandMetrics(prefix :+ "commands", openTelemetryMetricsFactory)

  object execution
      extends ExecutionMetrics(
        prefix :+ "execution",
        openTelemetryMetricsFactory,
      )

  object lapi extends LAPIMetrics(prefix :+ "lapi", openTelemetryMetricsFactory)

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
        openTelemetryMetricsFactory,
      )

  object indexer extends IndexerMetrics(prefix :+ "indexer", openTelemetryMetricsFactory)

  object indexerEvents
      extends IndexedUpdatesMetrics(prefix :+ "indexer", openTelemetryMetricsFactory)

  object parallelIndexer
      extends ParallelIndexerMetrics(
        prefix :+ "parallel_indexer",
        openTelemetryMetricsFactory,
      )

  object services
      extends ServicesMetrics(
        prefix :+ "services",
        openTelemetryMetricsFactory,
      )

  object grpc extends DamlGrpcServerMetrics(openTelemetryMetricsFactory, "participant")

  object health extends HealthMetrics(openTelemetryMetricsFactory)

}
