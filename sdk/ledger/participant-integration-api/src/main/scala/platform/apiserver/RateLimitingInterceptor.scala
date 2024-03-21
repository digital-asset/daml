// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.{MetricName, Metrics}
import com.daml.platform.apiserver.RateLimitingInterceptor.doNonLimit
import com.daml.platform.apiserver.TenuredMemoryPool.findTenuredMemoryPool
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.configuration.ServerRole
import io.grpc.Status.Code
import io.grpc._
import io.grpc.protobuf.StatusProto
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import java.lang.management.{
  ManagementFactory,
  MemoryMXBean,
  MemoryPoolMXBean,
  MemoryType,
  MemoryUsage,
}
import java.util.concurrent.atomic.AtomicLong
import javax.management.ObjectName
import scala.jdk.CollectionConverters.ListHasAsScala

private[apiserver] final class RateLimitingInterceptor(
    metrics: Metrics,
    config: RateLimitingConfig,
    tenuredMemoryPool: Option[MemoryPoolMXBean],
    memoryMxBean: GcThrottledMemoryBean,
) extends ServerInterceptor {

  private val logger = LoggerFactory.getLogger(getClass)

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

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    serviceOverloaded(call.getMethodDescriptor.getFullMethodName) match {
      case Some(errorMessage) =>
        val rpcStatus = com.google.rpc.Status
          .newBuilder()
          .setCode(Code.ABORTED.value())
          .setMessage(errorMessage)
          .build()

        logger.info(s"gRPC call rejected: $rpcStatus")

        val exception = StatusProto.toStatusRuntimeException(rpcStatus)

        call.close(exception.getStatus, exception.getTrailers)
        new ServerCall.Listener[ReqT]() {}

      case None =>
        next.startCall(call, headers)
    }

  }

  private def serviceOverloaded(fullMethodName: String): Option[String] = {
    if (doNonLimit.contains(fullMethodName)) {
      None
    } else {
      (for {
        _ <- memoryOverloaded(fullMethodName)
        _ <- metricOverloaded(fullMethodName, apiServices, config.maxApiServicesQueueSize)
        _ <- metricOverloaded(
          fullMethodName,
          indexDbThreadpool,
          config.maxApiServicesIndexDbQueueSize,
        )
      } yield ()).fold(Some.apply, _ => None)
    }
  }

  private def memoryOverloaded(fullMethodName: String): Either[String, Unit] = {
    tenuredMemoryPool.fold[Either[String, Unit]](Right(())) { p =>
      if (p.isCollectionUsageThresholdExceeded) {
        val expectedThreshold =
          config.calculateCollectionUsageThreshold(p.getCollectionUsage.getMax)
        if (p.getCollectionUsageThreshold == expectedThreshold) {
          // Based on a combination of JvmMetricSet and MemoryUsageGaugeSet
          val poolBeanMetricPrefix = s"jvm_memory_usage_pools_${p.getName}"
          val rpcStatus =
            s"""
               | The ${p.getName} collection usage threshold has exceeded the maximum (${p.getCollectionUsageThreshold}).
               | The rejected call was $fullMethodName.
               | Jvm memory metrics are available at $poolBeanMetricPrefix
            """.stripMargin
          gc()
          Left[String, Unit](rpcStatus)
        } else {
          // In experimental testing the size of the tenured memory pool did not change.  However the API docs,
          // see https://docs.oracle.com/javase/8/docs/api/java/lang/management/MemoryUsage.html
          // say 'The maximum amount of memory may change over time'.  If we detect this situation we
          // recalculate and reset the threshold
          logger.warn(
            s"Detected change in max pool memory, updating collection usage threshold  from ${p.getCollectionUsageThreshold} to $expectedThreshold"
          )
          p.setCollectionUsageThreshold(expectedThreshold)
          Right(())
        }
      } else {
        Right(())
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

  private def metricOverloaded(
      fullMethodName: String,
      count: InstrumentedCount,
      limit: Int,
  ): Either[String, Unit] = {
    val queued = count.queueSize
    if (queued > limit) {
      val rpcStatus =
        s"""
           | The ${count.name} queue size ($queued) has exceeded the maximum ($limit).
           | The rejected call was $fullMethodName.
           | Api services metrics are available at ${count.prefix}.
          """.stripMargin

      Left(rpcStatus)
    } else {
      Right(())
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

  val doNonLimit: Set[String] = Set(
    "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
    "grpc.health.v1.Health/Check",
    "grpc.health.v1.Health/Watch",
  )
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
