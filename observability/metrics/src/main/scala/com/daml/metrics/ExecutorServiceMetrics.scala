// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.{ExecutorService, ForkJoinPool, ThreadPoolExecutor}

import com.daml.metrics.ExecutorServiceMetrics.{
  CommonMetricsName,
  ForkJoinMetricsName,
  NameLabelKey,
  ThreadPoolMetricsName,
}
import com.daml.metrics.InstrumentedExecutorServiceMetrics.InstrumentedExecutorService
import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard
import org.slf4j.LoggerFactory

class ExecutorServiceMetrics(factory: Factory) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val instrumentedExecutorServiceMetrics = new InstrumentedExecutorServiceMetrics(factory)

  def monitorExecutorService(name: String, executor: ExecutorService): ExecutorService = {
    executor match {
      case forkJoinPool: ForkJoinPool =>
        monitorForkJoin(name, forkJoinPool)
        new InstrumentedExecutorService(forkJoinPool, instrumentedExecutorServiceMetrics, name)
      case threadPoolExecutor: ThreadPoolExecutor =>
        monitorThreadPool(name, threadPoolExecutor)
        new InstrumentedExecutorService(
          threadPoolExecutor,
          instrumentedExecutorServiceMetrics,
          name,
        )
      case other =>
        logger.info(s"Cannot monitor executor of type ${other.getClass}")
        other
    }
  }

  private def monitorForkJoin(name: String, executor: ForkJoinPool): Unit = {
    MetricsContext.withMetricLabels(NameLabelKey -> name, "type" -> "fork_join") { implicit mc =>
      poolSizeGauge(() => executor.getPoolSize)
      activeThreadsGauge(() => executor.getActiveThreadCount)
      factory.gaugeWithSupplier(
        ForkJoinMetricsName.RunningThreads,
        () => executor.getRunningThreadCount,
        "Estimate of the number of worker threads that are not blocked waiting to join tasks or for other managed synchronization.",
      )
      factory.gaugeWithSupplier(
        ForkJoinMetricsName.StolenTasks,
        () => executor.getStealCount,
        "Estimate of the total number of completed tasks that were executed by a thread other than their submitter.",
      )
      // The following 2 gauges are very similar, but the `getQueuedTaskCount` returns only the queue sizes starting
      // from index 1, therefore skipping the first queue. This is done assuming that the first queue represents tasks not yet assigned
      // to a worker.
      factory.gaugeWithSupplier(
        ForkJoinMetricsName.ExecutingQueuedTasks,
        () => executor.getQueuedTaskCount,
        "Estimate of the total number of tasks currently held in queues by worker threads (but not including tasks submitted to the pool that have not begun executing).",
      )
      queuedTasksGauge(() => executor.getQueuedSubmissionCount)
    }
  }

  private def monitorThreadPool(name: String, executor: ThreadPoolExecutor): Unit = {
    MetricsContext.withMetricLabels("name" -> name, "type" -> "thread_pool") { implicit mc =>
      poolSizeGauge(() => executor.getPoolSize)
      factory.gaugeWithSupplier(
        ThreadPoolMetricsName.CorePoolSize,
        () => executor.getCorePoolSize,
        "Core number of threads.",
      )
      factory.gaugeWithSupplier(
        ThreadPoolMetricsName.MaxPoolSize,
        () => executor.getMaximumPoolSize,
        "Maximum allowed number of threads.",
      )
      factory.gaugeWithSupplier(
        ThreadPoolMetricsName.LargestPoolSize,
        () => executor.getMaximumPoolSize,
        "Largest number of threads that have ever simultaneously been in the pool.",
      )
      activeThreadsGauge(() => executor.getActiveCount)
      factory.gaugeWithSupplier(
        ThreadPoolMetricsName.CompletedTasks,
        () => executor.getCompletedTaskCount,
        "Approximate total number of tasks that have completed execution.",
      )
      factory.gaugeWithSupplier(
        ThreadPoolMetricsName.SubmittedTasks,
        () => executor.getTaskCount,
        "Approximate total number of tasks that have ever been scheduled for execution.",
      )
      queuedTasksGauge(() => executor.getQueue.size)
      discard {
        factory.gaugeWithSupplier(
          ThreadPoolMetricsName.RemainingQueueCapacity,
          () => executor.getQueue.remainingCapacity,
          "Additional elements that this queue can ideally accept without blocking.",
        )
      }
    }
  }

  private def poolSizeGauge(size: () => Int)(implicit mc: MetricsContext): Unit = discard {
    factory.gaugeWithSupplier(
      CommonMetricsName.PoolSize,
      size,
      "Number of worker threads present in the pool.",
    )
  }

  private def activeThreadsGauge(activeThreads: () => Int)(implicit mc: MetricsContext): Unit =
    discard {
      factory.gaugeWithSupplier(
        CommonMetricsName.ActiveThreads,
        activeThreads,
        "Estimate of the number of threads that executing tasks.",
      )
    }

  private def queuedTasksGauge(queueSize: () => Int)(implicit mc: MetricsContext): Unit = discard {
    factory.gaugeWithSupplier(
      CommonMetricsName.QueuedTasks,
      queueSize,
      "Approximate number of tasks that are queued for execution.",
    )
  }

}
object ExecutorServiceMetrics {

  val NameLabelKey = "name"

  val Prefix: MetricName = MetricName("daml", "executor")
  private val PoolMetricsPrefix: MetricName = Prefix :+ "pool"
  private val TasksMetricsPrefix: MetricName = Prefix :+ "tasks"
  private val ThreadsMetricsPrefix: MetricName = Prefix :+ "threads"

  object ThreadPoolMetricsName {

    val CorePoolSize: MetricName = PoolMetricsPrefix :+ "core"
    val MaxPoolSize: MetricName = PoolMetricsPrefix :+ "max"
    val LargestPoolSize: MetricName = PoolMetricsPrefix :+ "largest"
    val CompletedTasks: MetricName = TasksMetricsPrefix :+ "completed"
    val SubmittedTasks: MetricName = TasksMetricsPrefix :+ "submitted"
    val RemainingQueueCapacity: MetricName = TasksMetricsPrefix :+ "queue" :+ "remaining"

  }

  object ForkJoinMetricsName {

    val RunningThreads: MetricName = ThreadsMetricsPrefix :+ "running"
    val StolenTasks: MetricName = TasksMetricsPrefix :+ "stolen"
    val ExecutingQueuedTasks: MetricName = TasksMetricsPrefix :+ "executing" :+ "queued"

  }

  object CommonMetricsName {

    val PoolSize: MetricName = PoolMetricsPrefix :+ "size"
    val ActiveThreads: MetricName = ThreadsMetricsPrefix :+ "active"
    val QueuedTasks: MetricName = TasksMetricsPrefix :+ "queued"

  }

}
