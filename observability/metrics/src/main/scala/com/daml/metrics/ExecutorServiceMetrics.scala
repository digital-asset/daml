// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.{ExecutorService, ForkJoinPool, ThreadPoolExecutor}

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.{MetricName, MetricsContext}
import org.slf4j.LoggerFactory

class ExecutorServiceMetrics(factory: Factory) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val prefix = MetricName("daml", "executor")
  private val PoolMetricsPrefix: MetricName = prefix :+ "pool"
  private val TasksMetricsPrefix: MetricName = prefix :+ "tasks"
  private val ThreadsMetricsPrefix: MetricName = prefix :+ "threads"

  def monitorExecutorService(name: String, executor: ExecutorService): ExecutorService = {
    executor match {
      case forkJoinPool: ForkJoinPool =>
        monitorForkJoin(name, forkJoinPool)
        forkJoinPool
      case threadPoolExecutor: ThreadPoolExecutor =>
        monitorThreadPool(name, threadPoolExecutor)
        threadPoolExecutor
      case other =>
        logger.info(s"Cannot monitor executor of type ${other.getClass}")
        other
    }
  }

  def monitorForkJoin(name: String, executor: ForkJoinPool): Unit = {
    MetricsContext.withMetricLabels("name" -> name, "type" -> "fork_join") { implicit mc =>
      poolSizeGauge(() => executor.getPoolSize)
      activeThreadsGauge(() => executor.getActiveThreadCount)
      factory.gaugeWithSupplier(
        ThreadsMetricsPrefix :+ "running",
        () => executor.getRunningThreadCount,
        "Estimate of the number of worker threads that are not blocked waiting to join tasks or for other managed synchronization.",
      )
      factory.gaugeWithSupplier(
        TasksMetricsPrefix :+ "stolen",
        () => executor.getStealCount,
        "Estimate of the total number of completed tasks that were executed by a thread other than their submitter.",
      )
      // The following 2 gauges are very similar, but the `getQueuedTaskCount` returns only the queue sizes starting
      // from index 1, therefore skipping the first queue. This is done assuming that the first queue represents tasks not yet assigned
      // to a worker.
      factory.gaugeWithSupplier(
        TasksMetricsPrefix :+ "executing" :+ "queued",
        () => executor.getQueuedTaskCount,
        "Estimate of the total number of tasks currently held in queues by worker threads (but not including tasks submitted to the pool that have not begun executing).",
      )
      queuedTasksGauge(() => executor.getQueuedSubmissionCount)
    }
  }

  def monitorThreadPool(name: String, executor: ThreadPoolExecutor): Unit = {
    MetricsContext.withMetricLabels("name" -> name, "type" -> "thread_pool") { implicit mc =>
      poolSizeGauge(() => executor.getPoolSize)
      factory.gaugeWithSupplier(
        PoolMetricsPrefix :+ "core",
        () => executor.getCorePoolSize,
        "Core number of threads.",
      )
      factory.gaugeWithSupplier(
        PoolMetricsPrefix :+ "max",
        () => executor.getMaximumPoolSize,
        "Maximum allowed number of threads.",
      )
      factory.gaugeWithSupplier(
        PoolMetricsPrefix :+ "largest",
        () => executor.getMaximumPoolSize,
        "Largest number of threads that have ever simultaneously been in the pool.",
      )
      activeThreadsGauge(() => executor.getActiveCount)
      factory.gaugeWithSupplier(
        TasksMetricsPrefix :+ "completed",
        () => executor.getCompletedTaskCount,
        "Approximate total number of tasks that have completed execution.",
      )
      factory.gaugeWithSupplier(
        TasksMetricsPrefix :+ "submitted",
        () => executor.getTaskCount,
        "Approximate total number of tasks that have ever been scheduled for execution.",
      )
      queuedTasksGauge(() => executor.getQueue.size)
      factory.gaugeWithSupplier(
        TasksMetricsPrefix :+ "queue" :+ "remaining",
        () => executor.getQueue.remainingCapacity,
        "Additional elements that this queue can ideally accept without blocking.",
      )
    }
  }

  private def poolSizeGauge(size: () => Int)(implicit mc: MetricsContext): Unit = {
    factory.gaugeWithSupplier(
      PoolMetricsPrefix :+ "size",
      size,
      "Number of worker threads present in the pool.",
    )
  }

  private def activeThreadsGauge(activeThreads: () => Int)(implicit mc: MetricsContext): Unit = {
    factory.gaugeWithSupplier(
      ThreadsMetricsPrefix :+ "active",
      activeThreads,
      "Estimate of the number of threads that executing tasks.",
    )
  }

  private def queuedTasksGauge(queueSize: () => Int)(implicit mc: MetricsContext): Unit = {
    factory.gaugeWithSupplier(
      TasksMetricsPrefix :+ "queued",
      queueSize,
      "Approximate number of tasks that are queued for execution.",
    )
  }

}
