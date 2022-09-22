// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{Gauge, MetricRegistry, SharedMetricRegistries}
import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._

object Metrics {
  def fromSharedMetricRegistries(registryName: String): Metrics =
    new Metrics(SharedMetricRegistries.getOrCreate(registryName))
}

final class Metrics(val registry: MetricRegistry) {

  def register(name: MetricName, gaugeSupplier: MetricSupplier[Gauge[_]]): Unit =
    registerGauge(name, gaugeSupplier, registry)

  object test {
    private val Prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(Prefix, "db", registry)
  }

  object daml extends MetricHandle.Factory {
    override val prefix: MetricName = MetricName.Daml // move in the constructor ?
    override val registry = Metrics.this.registry

    object commands extends CommandMetrics(prefix :+ "commands", registry)

    object execution extends ExecutionMetrics(prefix :+ "execution", registry)

    object lapi extends LAPIMetrics(prefix :+ "lapi", registry)

    object userManagement extends UserManagementMetrics(prefix :+ "user_management", registry)

    object partyRecordStore
        extends PartyRecordStoreMetrics(prefix :+ "party_record_store", registry)

    object index extends IndexMetrics(prefix :+ "index", registry)

    object indexer extends IndexerMetrics(prefix :+ "indexer", registry)

    object parallelIndexer extends ParallelIndexerMetrics(prefix :+ "parallel_indexer", registry)

    object services extends ServicesMetrics(prefix :+ "services", registry)

    object HttpJsonApi extends HttpJsonApiMetrics(prefix :+ "http_json_api", registry)

  }
}
