// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.metrics

import java.util

import com.codahale.metrics.jvm.{
  ClassLoadingGaugeSet,
  GarbageCollectorMetricSet,
  JvmAttributeGaugeSet,
  MemoryUsageGaugeSet,
  ThreadStatesGaugeSet
}
import com.codahale.metrics.{Metric, MetricRegistry, MetricSet}
import JvmMetricSet._

import scala.collection.JavaConverters._

class JvmMetricSet extends MetricSet {
  private val metricSets = Map(
    "class_loader" -> new ClassLoadingGaugeSet,
    "garbage_collector" -> new GarbageCollectorMetricSet,
    "attributes" -> new JvmAttributeGaugeSet,
    "memory_usage" -> new MemoryUsageGaugeSet,
    "thread_states" -> new ThreadStatesGaugeSet,
  )

  override def getMetrics: util.Map[String, Metric] =
    metricSets.flatMap {
      case (metricSetName, metricSet) =>
        metricSet.getMetrics.asScala.map {
          case (metricName, metric) =>
            MetricRegistry.name(Prefix, metricSetName, metricName) -> metric
        }
    }.asJava
}

object JvmMetricSet {
  private val Prefix = "jvm"
}
