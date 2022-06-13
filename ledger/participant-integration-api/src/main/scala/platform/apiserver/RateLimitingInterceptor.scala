// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.error.definitions.DamlError
import com.daml.error.definitions.LedgerApiErrors.{
  HeapMemoryOverLimit,
  MaximumNumberOfStreams,
  ThreadpoolOverloaded,
}
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.metrics.{MetricName, Metrics}
import com.daml.platform.apiserver.RateLimitingInterceptor._
import com.daml.platform.apiserver.TenuredMemoryPool.findTenuredMemoryPool
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.configuration.ServerRole
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc._
import org.slf4j.LoggerFactory

import java.lang.management._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import javax.management.ObjectName
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.{Either, Try}

private[apiserver] final class RateLimitingInterceptor(
    metrics: Metrics,
    config: RateLimitingConfig,
    tenuredMemoryPool: Option[MemoryPoolMXBean],
    memoryMxBean: GcThrottledMemoryBean,
) extends ServerInterceptor {

  private implicit val logger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(getClass)

  /** Match naming in [[com.codahale.metrics.InstrumentedExecutorService]] */
  private case class InstrumentedCount(name: String, prefix: MetricName) {
    private val submitted = metrics.registry.meter(MetricRegistry.name(prefix, "submitted"))
    private val running = metrics.registry.counter(MetricRegistry.name(prefix, "running"))
    private val completed = metrics.registry.meter(MetricRegistry.name(prefix, "completed"))
    def queueSize: Long = submitted.getCount - running.getCount - completed.getCount
  }

  private val apiServices =
    InstrumentedCount("Api Services Threadpool", metrics.daml.lapi.threadpool.apiServices)
  private val indexDbThreadpool = InstrumentedCount(
    "Index Database Connection Threadpool",
    MetricName(metrics.daml.index.db.threadpool.connection, ServerRole.ApiServer.threadPoolSuffix),
  )

  private val activeStreamsName = metrics.daml.lapi.streams.activeName
  private val activeStreamsCounter = metrics.daml.lapi.streams.active

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val isStream = !call.getMethodDescriptor.getType.serverSendsOneMessage()
    serviceOverloaded(fullMethodName, isStream) match {

      case Left(damlError) =>
        val statusRuntimeException = damlError.asGrpcError
        call.close(statusRuntimeException.getStatus, statusRuntimeException.getTrailers)
        new ServerCall.Listener[ReqT]() {}

      case Right(()) if isStream =>
        val delegate = next.startCall(call, headers)
        val listener =
          new OnCloseCallListener(delegate, runOnceOnTermination = () => activeStreamsCounter.dec())
        activeStreamsCounter.inc() // Only do after call above has returned
        listener

      case Right(()) =>
        next.startCall(call, headers)

    }

  }

  private def serviceOverloaded(
      fullMethodName: String,
      isStream: Boolean,
  ): Either[DamlError, Unit] = {
    if (doNonLimit.contains(fullMethodName)) {
      UnderLimit
    } else {
      for {
        _ <- memoryUnderLimit(fullMethodName)
        _ <- queueUnderLimit(apiServices, config.maxApiServicesQueueSize, fullMethodName)
        _ <- queueUnderLimit(
          indexDbThreadpool,
          config.maxApiServicesIndexDbQueueSize,
          fullMethodName,
        )
        _ <- if (isStream) streamsUnderLimit(fullMethodName) else UnderLimit
      } yield ()
    }
  }

  private def memoryUnderLimit(fullMethodName: String): Either[DamlError, Unit] = {

    tenuredMemoryPool.fold[Either[DamlError, Unit]](UnderLimit) { p =>
      if (p.isCollectionUsageThresholdExceeded) {
        val expectedThreshold =
          config.calculateCollectionUsageThreshold(p.getCollectionUsage.getMax)
        if (p.getCollectionUsageThreshold == expectedThreshold) {
          // Based on a combination of JvmMetricSet and MemoryUsageGaugeSet
          val poolBeanMetricPrefix =
            "jvm_memory_usage_pools_%s".format(p.getName.replaceAll("\\s+", "_"))
          val damlError = HeapMemoryOverLimit.Rejection(
            memoryPool = p.getName,
            limit = p.getCollectionUsageThreshold,
            metricPrefix = poolBeanMetricPrefix,
            fullMethodName = fullMethodName,
          )
          gc()
          Left(damlError)
        } else {
          // In experimental testing the size of the tenured memory pool did not change.  However the API docs,
          // see https://docs.oracle.com/javase/8/docs/api/java/lang/management/MemoryUsage.html
          // say 'The maximum amount of memory may change over time'.  If we detect this situation we
          // recalculate and reset the threshold
          logger.warn(
            s"Detected change in max pool memory, updating collection usage threshold  from ${p.getCollectionUsageThreshold} to $expectedThreshold"
          )
          p.setCollectionUsageThreshold(expectedThreshold)
          UnderLimit
        }
      } else {
        UnderLimit
      }
    }
  }

  /** When the collected tenured memory pool usage exceeds the threshold this state will continue even if memory
    * has been freed up if no garbage collection takes place.  For this reason when we are over limit we also
    * run garbage collection on every request to ensure the collection usage stats are as up to date as possible
    * to thus stop rate limiting as soon as possible.
    *
    * We use a throttled memory bean to ensure that even if the server is under heavy rate limited load calls
    * to the underlying system gc are limited.
    */

  private def gc(): Unit = {
    memoryMxBean.gc()
  }

  private def queueUnderLimit(
      count: InstrumentedCount,
      limit: Int,
      fullMethodName: String,
  ): Either[DamlError, Unit] = {
    val queued = count.queueSize
    if (queued > limit) {
      Left(
        ThreadpoolOverloaded.Rejection(
          name = count.name,
          queued = queued,
          limit = limit,
          metricPrefix = count.prefix,
          fullMethodName = fullMethodName,
        )
      )
    } else UnderLimit
  }

  private def streamsUnderLimit(fullMethodName: String): Either[DamlError, Unit] = {
    if (activeStreamsCounter.getCount >= config.maxStreams) {
      Left(
        MaximumNumberOfStreams.Rejection(
          value = activeStreamsCounter.getCount,
          limit = config.maxStreams,
          metricPrefix = activeStreamsName,
          fullMethodName = fullMethodName,
        )
      )
    } else {
      UnderLimit
    }
  }

}

object RateLimitingInterceptor {

  def apply(metrics: Metrics, config: RateLimitingConfig): RateLimitingInterceptor = {
    apply(
      metrics = metrics,
      config = config,
      tenuredMemoryPools = ManagementFactory.getMemoryPoolMXBeans.asScala.toList,
      memoryMxBean = ManagementFactory.getMemoryMXBean,
    )
  }

  def apply(
      metrics: Metrics,
      config: RateLimitingConfig,
      tenuredMemoryPools: List[MemoryPoolMXBean],
      memoryMxBean: MemoryMXBean,
  ): RateLimitingInterceptor = {
    new RateLimitingInterceptor(
      metrics = metrics,
      config = config,
      tenuredMemoryPool = findTenuredMemoryPool(config, tenuredMemoryPools),
      memoryMxBean = new GcThrottledMemoryBean(memoryMxBean),
    )
  }

  private val UnderLimit: Either[DamlError, Unit] = Right(())

  val doNonLimit: Set[String] = Set(
    "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
    "grpc.health.v1.Health/Check",
    "grpc.health.v1.Health/Watch",
  )

  private class OnCloseCallListener[RespT](
      delegate: ServerCall.Listener[RespT],
      runOnceOnTermination: () => Unit,
  ) extends SimpleForwardingServerCallListener[RespT](delegate) {
    private val logger = LoggerFactory.getLogger(getClass)
    private val onTerminationCalled = new AtomicBoolean()

    private def runOnClose(): Unit = {
      if (onTerminationCalled.compareAndSet(false, true)) {
        Try(runOnceOnTermination()).failed
          .foreach(logger.warn(s"Exception calling onClose method", _))
      }
    }

    override def onCancel(): Unit = {
      runOnClose()
      super.onCancel()
    }
    override def onComplete(): Unit = {
      runOnClose()
      super.onComplete()
    }

  }

}

class GcThrottledMemoryBean(delegate: MemoryMXBean, delayBetweenCalls: Duration = 1.seconds)
    extends MemoryMXBean {

  private val lastCall = new AtomicLong()

  /** Only GC if we have not called gc for at least [[delayBetweenCalls]]
    */
  override def gc(): Unit = {
    val last = lastCall.get()
    val now = System.currentTimeMillis()
    if (now - last > delayBetweenCalls.toMillis && lastCall.compareAndSet(last, now)) delegate.gc()
  }

  // Delegated methods
  override def getObjectPendingFinalizationCount: Int = delegate.getObjectPendingFinalizationCount
  override def getHeapMemoryUsage: MemoryUsage = delegate.getHeapMemoryUsage
  override def getNonHeapMemoryUsage: MemoryUsage = delegate.getNonHeapMemoryUsage
  override def isVerbose: Boolean = delegate.isVerbose
  override def setVerbose(value: Boolean): Unit = delegate.setVerbose(value)
  override def getObjectName: ObjectName = delegate.getObjectName
}

object TenuredMemoryPool {

  private val logger = LoggerFactory.getLogger(getClass)

  def findTenuredMemoryPool(
      config: RateLimitingConfig,
      memoryPoolMxBeans: List[MemoryPoolMXBean],
  ): Option[MemoryPoolMXBean] = {
    candidates(memoryPoolMxBeans).sortBy(_.getCollectionUsage.getMax).lastOption match {
      case None =>
        logger.error("Could not find tenured memory pool")
        None
      case Some(pool) =>
        val threshold = config.calculateCollectionUsageThreshold(pool.getCollectionUsage.getMax)
        logger.info(
          s"Using 'tenured' memory pool ${pool.getName}.  Setting its collection pool threshold to $threshold"
        )
        pool.setCollectionUsageThreshold(threshold)
        Some(pool)
    }
  }

  private def candidates(memoryPoolMxBeans: List[MemoryPoolMXBean]): List[MemoryPoolMXBean] =
    memoryPoolMxBeans.filter(p =>
      p.getType == MemoryType.HEAP && p.isCollectionUsageThresholdSupported
    )
}
