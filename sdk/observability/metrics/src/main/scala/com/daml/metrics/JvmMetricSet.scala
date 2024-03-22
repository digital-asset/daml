// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util

import io.opentelemetry.instrumentation.runtimemetrics.{GarbageCollector, MemoryPools}
import com.codahale.metrics.jvm.{
  ClassLoadingGaugeSet,
  GarbageCollectorMetricSet,
  JvmAttributeGaugeSet,
  MemoryUsageGaugeSet,
  ThreadStatesGaugeSet,
}
import com.codahale.metrics.{Metric, MetricSet}
import com.daml.metrics.JvmMetricSet._
import com.daml.metrics.api.MetricName

import scala.jdk.CollectionConverters._

class JvmMetricSet extends MetricSet {
  private val metricSets = Map(
    "class_loader" -> new ClassLoadingGaugeSet,
    "garbage_collector" -> new GarbageCollectorMetricSet,
    "attributes" -> new JvmAttributeGaugeSet,
    "memory_usage" -> new MemoryUsageGaugeSet,
    "thread_states" -> new ThreadStatesGaugeSet,
  )

  override def getMetrics: util.Map[String, Metric] =
    metricSets.flatMap { case (metricSetName, metricSet) =>
      val metricSetPrefix = Prefix :+ metricSetName
      metricSet.getMetrics.asScala.map { case (metricName, metric) =>
        (metricSetPrefix :+ metricName).toString -> metric
      }
    }.asJava
}

object JvmMetricSet {
  private val Prefix = MetricName("jvm")

  def registerObservers(): Unit = {
//    BufferPools.registerObservers(openTelemetry)
//    Classes.registerObservers(openTelemetry)
//    Cpu.registerObservers(openTelemetry)
//    Threads.registerObservers(openTelemetry)
    MemoryPools.registerObservers()
    GarbageCollector.registerObservers()
  }
}
