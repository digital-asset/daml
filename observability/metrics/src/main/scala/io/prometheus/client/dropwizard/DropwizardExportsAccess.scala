// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package io.prometheus.client.dropwizard

import com.codahale.metrics.{MetricRegistry, Snapshot}
import io.prometheus.client.Collector
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder

/** This is a work-around to open up package-private `DropwizardExports` methods so that we can expose
  * min/mean/max metrics. Those values are absent in the official prometheus implementation but are
  * an essential part of the ledger performance reporting and feature heavily in other back-end
  * integrations as well as metrics dashboards.
  *
  * @param metricRegistry metric registry instance to wrap
  */
class DropwizardExportsAccess(metricRegistry: MetricRegistry)
    extends DropwizardExports(metricRegistry) {

  final val sampleBuilder = new DefaultSampleBuilder()

  override def fromSnapshotAndCount(
      dropwizardName: String,
      snapshot: Snapshot,
      count: Long,
      factor: Double,
      helpMessage: String,
  ): Collector.MetricFamilySamples =
    super.fromSnapshotAndCount(dropwizardName, snapshot, count, factor, helpMessage)
}
