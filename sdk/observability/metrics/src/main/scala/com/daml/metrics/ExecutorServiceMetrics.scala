// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util
import java.util.concurrent.{
  Callable,
  ExecutorService,
  ForkJoinPool,
  Future,
  ThreadPoolExecutor,
  TimeUnit,
}
import com.daml.metrics.ExecutorServiceMetrics.{
  CommonMetricsName,
  ExecutorServiceWithCleanup,
  ForkJoinMetricsName,
  NameLabelKey,
  ThreadPoolMetricsName,
  TypeLabelKey,
}
import com.daml.metrics.InstrumentedExecutorServiceMetrics.InstrumentedExecutorService
import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricsContext}
import org.slf4j.LoggerFactory

class ExecutorServiceMetrics(factory: LabeledMetricsFactory) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val instrumentedExecutorServiceMetrics = new InstrumentedExecutorServiceMetrics(factory)

  def monitorExecutorService(
      name: String,
      executor: ExecutorService,
  ): ExecutorService = {
    /* Handles the removal of registered gauges for the executor service.
     * As all the gauges registered are async the removal could've been also done when
     * trying to read the value of a gauge, but this would not prevent warnings being logged when an executor with
     * the same name is registered before the gauges for a previous one was removed.
     * */
    executor match {
      case forkJoinPool: ForkJoinPool =>
        MetricsContext.withMetricLabels(NameLabelKey -> name, TypeLabelKey -> "fork_join") {
          implicit mc =>
            val monitoringHandle = monitorForkJoin(forkJoinPool)
            val instrumentedExecutor =
              new InstrumentedExecutorService(forkJoinPool, instrumentedExecutorServiceMetrics)
            new ExecutorServiceWithCleanup(instrumentedExecutor, monitoringHandle)
        }
      case threadPoolExecutor: ThreadPoolExecutor =>
        MetricsContext.withMetricLabels(NameLabelKey -> name, TypeLabelKey -> "thread_pool") {
          implicit mc =>
            val monitoringHandle = monitorThreadPool(threadPoolExecutor)
            val instrumentedExecutor = new InstrumentedExecutorService(
              threadPoolExecutor,
              instrumentedExecutorServiceMetrics,
            )
            new ExecutorServiceWithCleanup(instrumentedExecutor, monitoringHandle)
        }
      case other =>
        logger.warn(
          s"Cannot monitor executor of type ${other.getClass}. Proceeding without metrics."
        )
        other
    }
  }

  private def monitorForkJoin(
      executor: ForkJoinPool
  )(implicit mc: MetricsContext): AutoCloseable = {
    val poolSizeCloseableGauge = poolSizeGauge(() => executor.getPoolSize)
    val activeThreadsCloseableGauge = activeThreadsGauge(() => executor.getActiveThreadCount)
    val runningThreadsCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ForkJoinMetricsName.RunningThreads,
        summary =
          "Estimate of the number of worker threads that are not blocked waiting to join tasks or for other managed synchronization.",
        qualification = MetricQualification.Debug,
      ),
      () => executor.getRunningThreadCount,
    )
    val stolenTasksCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ForkJoinMetricsName.StolenTasks,
        summary =
          "Estimate of the total number of completed tasks that were executed by a thread other than their submitter.",
        qualification = MetricQualification.Debug,
      ),
      () => executor.getStealCount,
    )
    // The following 2 gauges are very similar, but the `getQueuedTaskCount` returns only the queue sizes starting
    // from index 1, therefore skipping the first queue. This is done assuming that the first queue represents tasks not yet assigned
    // to a worker.
    val executingQueuedTasksCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ForkJoinMetricsName.ExecutingQueuedTasks,
        "Estimate of the total number of tasks currently held in queues by worker threads (but not including tasks submitted to the pool that have not begun executing).",
        qualification = MetricQualification.Debug,
      ),
      () => executor.getQueuedTaskCount,
    )
    val queuedSubmissionCountCloseableGauge =
      queuedTasksGauge(() => executor.getQueuedSubmissionCount)
    () => {
      Seq(
        poolSizeCloseableGauge,
        activeThreadsCloseableGauge,
        runningThreadsCloseableGauge,
        stolenTasksCloseableGauge,
        executingQueuedTasksCloseableGauge,
        queuedSubmissionCountCloseableGauge,
      ).foreach(_.close())
    }
  }

  private def monitorThreadPool(
      executor: ThreadPoolExecutor
  )(implicit mc: MetricsContext): AutoCloseable = {
    val poolSizeCloseableGauge = poolSizeGauge(() => executor.getPoolSize)
    val corePoolSizeCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ThreadPoolMetricsName.CorePoolSize,
        "Core number of threads.",
        MetricQualification.Debug,
      ),
      () => executor.getCorePoolSize,
    )
    val maxPoolSizeCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ThreadPoolMetricsName.MaxPoolSize,
        "Maximum allowed number of threads.",
        MetricQualification.Debug,
      ),
      () => executor.getMaximumPoolSize,
    )
    val largestPoolSizeCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ThreadPoolMetricsName.LargestPoolSize,
        "Largest number of threads that have ever simultaneously been in the pool.",
        MetricQualification.Debug,
      ),
      () => executor.getMaximumPoolSize,
    )
    val activeThreadsCloseableGauge = activeThreadsGauge(() => executor.getActiveCount)
    val completedTasksCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ThreadPoolMetricsName.CompletedTasks,
        "Approximate total number of tasks that have completed execution.",
        MetricQualification.Debug,
      ),
      () => executor.getCompletedTaskCount,
    )
    val submittedTasksCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ThreadPoolMetricsName.SubmittedTasks,
        "Approximate total number of tasks that have ever been scheduled for execution.",
        MetricQualification.Debug,
      ),
      () => executor.getTaskCount,
    )
    val queuedTasksCloseableGauge = queuedTasksGauge(() => executor.getQueue.size)
    val remainingQueueCapacityCloseableGauge = factory.gaugeWithSupplier(
      MetricInfo(
        ThreadPoolMetricsName.RemainingQueueCapacity,
        "Additional elements that this queue can ideally accept without blocking.",
        MetricQualification.Debug,
      ),
      () => executor.getQueue.remainingCapacity,
    )
    () =>
      Seq(
        poolSizeCloseableGauge,
        corePoolSizeCloseableGauge,
        largestPoolSizeCloseableGauge,
        maxPoolSizeCloseableGauge,
        activeThreadsCloseableGauge,
        completedTasksCloseableGauge,
        submittedTasksCloseableGauge,
        queuedTasksCloseableGauge,
        remainingQueueCapacityCloseableGauge,
      ).foreach(_.close())
  }

  private def poolSizeGauge(size: () => Int)(implicit mc: MetricsContext) =
    factory.gaugeWithSupplier(
      MetricInfo(
        CommonMetricsName.PoolSize,
        "Number of worker threads present in the pool.",
        MetricQualification.Debug,
      ),
      size,
    )

  private def activeThreadsGauge(activeThreads: () => Int)(implicit mc: MetricsContext) =
    factory.gaugeWithSupplier(
      MetricInfo(
        CommonMetricsName.ActiveThreads,
        "Estimate of the number of threads that are executing tasks.",
        MetricQualification.Debug,
      ),
      activeThreads,
    )

  private def queuedTasksGauge(queueSize: () => Int)(implicit mc: MetricsContext) =
    factory.gaugeWithSupplier(
      MetricInfo(
        CommonMetricsName.QueuedTasks,
        "Approximate number of tasks that are queued for execution.",
        MetricQualification.Debug,
      ),
      queueSize,
    )

}
object ExecutorServiceMetrics {

  val NameLabelKey = "name"
  val TypeLabelKey = "type"

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

  private class ExecutorServiceWithCleanup(
      val delegate: ExecutorService,
      resourceCleaning: AutoCloseable,
  ) extends ExecutorService {
    override def shutdown(): Unit = {
      resourceCleaning.close()
      delegate.shutdown()
    }
    override def shutdownNow(): util.List[Runnable] = {
      resourceCleaning.close()
      delegate.shutdownNow()
    }

    override def isShutdown: Boolean = delegate.isShutdown
    override def isTerminated: Boolean = delegate.isTerminated
    override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean =
      delegate.awaitTermination(l, timeUnit)
    override def submit[T](
        callable: Callable[T]
    ): Future[T] = delegate.submit(callable)
    override def submit[T](
        runnable: Runnable,
        t: T,
    ): Future[T] = delegate.submit(runnable, t)
    override def submit(runnable: Runnable): Future[_] =
      delegate.submit(runnable)
    override def invokeAll[T](
        collection: util.Collection[_ <: Callable[T]]
    ): util.List[Future[T]] = delegate.invokeAll(collection)
    override def invokeAll[T](
        collection: util.Collection[_ <: Callable[T]],
        l: Long,
        timeUnit: TimeUnit,
    ): util.List[Future[T]] = delegate.invokeAll(collection, l, timeUnit)
    override def invokeAny[T](
        collection: util.Collection[_ <: Callable[T]]
    ): T = delegate.invokeAny(collection)
    override def invokeAny[T](
        collection: util.Collection[_ <: Callable[T]],
        l: Long,
        timeUnit: TimeUnit,
    ): T = delegate.invokeAny(collection, l, timeUnit)
    override def execute(runnable: Runnable): Unit = delegate.execute(runnable)
  }

}
