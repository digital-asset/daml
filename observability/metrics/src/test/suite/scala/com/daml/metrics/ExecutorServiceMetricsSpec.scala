// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.UUID
import java.util.concurrent.Executors

import com.daml.metrics.ExecutorServiceMetrics.{
  CommonMetricsName,
  ForkJoinMetricsName,
  NameLabelKey,
  ThreadPoolMetricsName,
}
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExecutorServiceMetricsSpec
    extends AnyWordSpec
    with Matchers
    with Eventually
    with MetricValues {

  private val factory = new InMemoryMetricsFactory
  private val metrics = new ExecutorServiceMetrics(factory)

  "Instrumenting thread pools" should {

    "register all async gauges" in {
      val configuredNumberOfThreads = 5
      val testName = UUID.randomUUID().toString
      metrics.monitorExecutorService(
        testName,
        Executors.newFixedThreadPool(configuredNumberOfThreads),
      )

      val registeredGauges = factory.asyncGaugeValues(LabelFilter(NameLabelKey, testName))
      eventually {
        registeredGauges(ThreadPoolMetricsName.CorePoolSize) shouldBe configuredNumberOfThreads
        registeredGauges(ThreadPoolMetricsName.MaxPoolSize) shouldBe configuredNumberOfThreads
        registeredGauges(ThreadPoolMetricsName.LargestPoolSize) shouldBe configuredNumberOfThreads
        registeredGauges(ThreadPoolMetricsName.CompletedTasks) shouldBe 0
        registeredGauges(ThreadPoolMetricsName.SubmittedTasks) shouldBe 0
        registeredGauges(CommonMetricsName.ActiveThreads) shouldBe 0
        registeredGauges(CommonMetricsName.PoolSize) shouldBe 0
        registeredGauges(CommonMetricsName.QueuedTasks) shouldBe 0
      }
    }

  }

  "Instrumenting fork join executors" should {

    "register all async gauges" in {
      val parallelism = 5
      val testName = UUID.randomUUID().toString

      metrics.monitorExecutorService(
        testName,
        Executors.newWorkStealingPool(parallelism),
      )

      val registeredGauges = factory.asyncGaugeValues(LabelFilter(NameLabelKey, testName))
      eventually {
        registeredGauges(ForkJoinMetricsName.RunningThreads) shouldBe 0
        registeredGauges(ForkJoinMetricsName.StolenTasks) shouldBe 0
        registeredGauges(ForkJoinMetricsName.ExecutingQueuedTasks) shouldBe 0
        registeredGauges(CommonMetricsName.ActiveThreads) shouldBe 0
        registeredGauges(CommonMetricsName.PoolSize) shouldBe 0
        registeredGauges(CommonMetricsName.QueuedTasks) shouldBe 0
      }
    }

  }

}
