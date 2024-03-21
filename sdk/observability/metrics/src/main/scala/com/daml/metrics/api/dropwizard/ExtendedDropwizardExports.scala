// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.dropwizard

import java.util.Collections

import com.codahale.metrics.{MetricRegistry, Snapshot}
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.dropwizard.DropwizardExportsAccess

import scala.jdk.CollectionConverters._

final class ExtendedDropwizardExports(metricRegistry: MetricRegistry)
    extends DropwizardExportsAccess(metricRegistry) {

  override def fromSnapshotAndCount(
      dropwizardName: String,
      snapshot: Snapshot,
      count: Long,
      factor: Double,
      helpMessage: String,
  ): MetricFamilySamples = {

    val basicMetricFamilySamples =
      super.fromSnapshotAndCount(dropwizardName, snapshot, count, factor, helpMessage)

    val extendedMetrics = basicMetricFamilySamples.samples.asScala ++ List(
      sampleBuilder
        .createSample(
          dropwizardName,
          "_min",
          EmptyJavaList,
          EmptyJavaList,
          snapshot.getMin * factor,
        ),
      sampleBuilder.createSample(
        dropwizardName,
        "_mean",
        EmptyJavaList,
        EmptyJavaList,
        snapshot.getMean * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "_max",
        EmptyJavaList,
        EmptyJavaList,
        snapshot.getMax * factor,
      ),
    )

    new MetricFamilySamples(
      basicMetricFamilySamples.name,
      basicMetricFamilySamples.`type`,
      basicMetricFamilySamples.help,
      extendedMetrics.asJava,
    )
  }

  private val EmptyJavaList = Collections.emptyList[String]()
}
