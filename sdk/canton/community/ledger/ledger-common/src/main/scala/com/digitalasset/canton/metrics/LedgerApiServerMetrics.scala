// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.daml.metrics.grpc.{DamlGrpcServerHistograms, DamlGrpcServerMetrics}
import com.daml.metrics.{DatabaseMetricsHistograms, HealthMetrics}
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.metrics.Meter

object LedgerApiServerMetrics extends LazyLogging {

  def apply(prefix: MetricName, otelMeter: Meter) = {
    val inventory = new HistogramInventory
    val histograms = new LedgerApiServerHistograms(prefix)(inventory)
    new LedgerApiServerMetrics(
      histograms,
      new OpenTelemetryMetricsFactory(
        otelMeter,
        inventory.registered().map(_.name.toString()).toSet,
        Some(logger.underlying),
      ),
    )
  }

  lazy val ForTesting: LedgerApiServerMetrics = {
    val prefix = MetricName("test")
    val histograms = new LedgerApiServerHistograms(prefix)(new HistogramInventory)
    new LedgerApiServerMetrics(
      histograms,
      NoOpMetricsFactory,
    )
  }
}

final class LedgerApiServerMetrics(
    inventory: LedgerApiServerHistograms,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  private val prefix = inventory.prefix

  object commands extends CommandMetrics(inventory.commands, openTelemetryMetricsFactory)

  object execution
      extends ExecutionMetrics(
        inventory.execution,
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
        inventory.index,
        openTelemetryMetricsFactory,
      )

  object indexer extends IndexerMetrics(prefix :+ "indexer", openTelemetryMetricsFactory)

  object indexerEvents
      extends IndexedUpdatesMetrics(prefix :+ "indexer", openTelemetryMetricsFactory)

  object parallelIndexer
      extends ParallelIndexerMetrics(inventory.parallelIndexer, openTelemetryMetricsFactory)

  object services
      extends ServicesMetrics(
        inventory = inventory.services,
        openTelemetryMetricsFactory,
      )

  object grpc extends DamlGrpcServerMetrics(openTelemetryMetricsFactory, "participant")

  object health extends HealthMetrics(openTelemetryMetricsFactory)

}

class LedgerApiServerHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {



  private[metrics] val services = new ServicesHistograms(prefix :+ "services")
  private[metrics] val commands = new CommandHistograms(prefix :+ "commands")
  private[metrics] val execution = new ExecutionHistograms(prefix :+ "execution")
  private[metrics] val index = new IndexHistograms(prefix :+ "index")
  private[metrics] val parallelIndexer = new ParallelIndexerHistograms(prefix :+ "parallel_indexer")

  private val _grpc = new DamlGrpcServerHistograms()
  // the ledger api server creates these metrics all over the place, but their prefix
  // is anyway hardcoded
  private val _db = new DatabaseMetricsHistograms()


}
