// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}

object Metrics {
  def fromSharedMetricRegistries(registryName: String): Metrics =
    new Metrics(SharedMetricRegistries.getOrCreate(registryName))
}

final class Metrics(override val registry: MetricRegistry) extends MetricHandle.Factory {
  override val prefix = MetricName("")

  object test {
    private val prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(prefix, "db", registry)
  }

  object daml extends MetricHandle.Factory {
    override val prefix: MetricName = MetricName.Daml
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
