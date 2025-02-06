// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import scala.annotation.unused

object LedgerApiServerMetrics extends LazyLogging {

  def apply(prefix: MetricName, otelMeter: Meter): LedgerApiServerMetrics = {
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

  val commands: CommandMetrics = new CommandMetrics(inventory.commands, openTelemetryMetricsFactory)

  val execution: ExecutionMetrics = new ExecutionMetrics(
    inventory.execution,
    openTelemetryMetricsFactory,
  )

  val lapi = new LAPIMetrics(prefix :+ "lapi", openTelemetryMetricsFactory)

  val userManagement = new UserManagementMetrics(
    prefix :+ "user_management",
    openTelemetryMetricsFactory,
  )

  val partyRecordStore = new PartyRecordStoreMetrics(
    prefix :+ "party_record_store",
    openTelemetryMetricsFactory,
  )

  val identityProviderConfigStore = new IdentityProviderConfigStoreMetrics(
    prefix :+ "identity_provider_config_store",
    openTelemetryMetricsFactory,
  )

  val index = new IndexMetrics(
    inventory.index,
    openTelemetryMetricsFactory,
  )

  val indexer = new IndexerMetrics(inventory.indexer, openTelemetryMetricsFactory)

  val services = new ServicesMetrics(
    inventory = inventory.services,
    openTelemetryMetricsFactory,
  )

  val grpc = new DamlGrpcServerMetrics(openTelemetryMetricsFactory, "participant")

  val health = new HealthMetrics(openTelemetryMetricsFactory)

}

final class LedgerApiServerHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val services = new ServicesHistograms(prefix :+ "services")
  private[metrics] val commands = new CommandHistograms(prefix :+ "commands")
  private[metrics] val execution = new ExecutionHistograms(prefix :+ "execution")
  private[metrics] val index = new IndexHistograms(prefix :+ "index")
  private[metrics] val indexer = new IndexerHistograms(prefix :+ "indexer")

  @unused
  private val _grpc = new DamlGrpcServerHistograms()
  // the ledger api server creates these metrics all over the place, but their prefix
  // is anyway hardcoded
  @unused
  private val _db = new DatabaseMetricsHistograms()

}
