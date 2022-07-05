// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.error.definitions.LedgerApiErrors.HeapMemoryOverLimit
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}

import java.lang.management.{MemoryMXBean, MemoryPoolMXBean, MemoryType, MemoryUsage}
import java.util.concurrent.atomic.AtomicLong
import javax.management.ObjectName
import scala.concurrent.duration.{Duration, DurationInt}

object MemoryCheck {

  private implicit val logger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(getClass)

  def apply(
      tenuredMemoryPools: List[MemoryPoolMXBean],
      memoryMxBean: MemoryMXBean,
      config: RateLimitingConfig,
  ): LimitResultCheck = {
    apply(
      findTenuredMemoryPool(config, tenuredMemoryPools),
      new GcThrottledMemoryBean(memoryMxBean),
      config,
    )
  }

  def apply(
      tenuredMemoryPool: Option[MemoryPoolMXBean],
      memoryMxBean: GcThrottledMemoryBean,
      config: RateLimitingConfig,
  ): LimitResultCheck = (fullMethodName, _) => {

    tenuredMemoryPool.fold[LimitResult](UnderLimit) { p =>
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
          gc(memoryMxBean)
          OverLimit(damlError)
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

  private def gc(memoryMxBean: GcThrottledMemoryBean): Unit = {
    memoryMxBean.gc()
  }

  private[ratelimiting] class GcThrottledMemoryBean(
      delegate: MemoryMXBean,
      delayBetweenCalls: Duration = 1.seconds,
  ) extends MemoryMXBean {

    private val lastCall = new AtomicLong()

    /** Only GC if we have not called gc for at least [[delayBetweenCalls]]
      */
    override def gc(): Unit = {
      val last = lastCall.get()
      val now = System.currentTimeMillis()
      if (now - last > delayBetweenCalls.toMillis && lastCall.compareAndSet(last, now))
        delegate.gc()
    }

    // Delegated methods
    override def getObjectPendingFinalizationCount: Int = delegate.getObjectPendingFinalizationCount
    override def getHeapMemoryUsage: MemoryUsage = delegate.getHeapMemoryUsage
    override def getNonHeapMemoryUsage: MemoryUsage = delegate.getNonHeapMemoryUsage
    override def isVerbose: Boolean = delegate.isVerbose
    override def setVerbose(value: Boolean): Unit = delegate.setVerbose(value)
    override def getObjectName: ObjectName = delegate.getObjectName
  }

  private[ratelimiting] def findTenuredMemoryPool(
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
