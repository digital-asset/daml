// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.{MetricName, Metrics}
import com.daml.platform.apiserver.RateLimitingInterceptor.doNonLimit
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.configuration.ServerRole
import io.grpc.Status.Code
import io.grpc._
import io.grpc.protobuf.StatusProto
import org.slf4j.LoggerFactory

import java.lang.management.{MemoryMXBean, MemoryPoolMXBean, MemoryType}

private[apiserver] final class RateLimitingInterceptor(
    metrics: Metrics,
    config: RateLimitingConfig,
    memoryPoolMxBeans: List[MemoryPoolMXBean],
    memoryMxBean: MemoryMXBean,
) extends ServerInterceptor {

  private val logger = LoggerFactory.getLogger(getClass)

  private val tenuredMemoryPool: Option[MemoryPoolMXBean] =
    memoryPoolMxBeans.find(p =>
      p.getType == MemoryType.HEAP && p.isCollectionUsageThresholdSupported
    )

  tenuredMemoryPool.foreach { p =>
    p.setCollectionUsageThreshold(config.collectionUsageThreshold(p.getCollectionUsage.getMax))
  }

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
        // Based on a combination of JvmMetricSet and MemoryUsageGaugeSet
        val poolBeanMetricPrefix = s"jvm_memory_usage_pools_${p.getName}"
        val rpcStatus =
          s"""
             | The ${p.getName} collection  has exceeded the maximum (${p.getCollectionUsageThreshold}).
             | The rejected call was $fullMethodName.
             | Jvm memory metrics are available at $poolBeanMetricPrefix
          """.stripMargin
        gc()
        Left[String, Unit](rpcStatus)
      } else {
        Right(())
      }
    }
  }

  /** When the collected tenured memory pool usage exceeds the threshold this state will continue even if memory
    * has been freed up if not garbage collection takes place.  For this reason when we are over limit we also
    * run garbage collection on every request to ensure the collection usage stats are as up to date as possible
    * to thus stop rate limiting as soon as possible.
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
  val doNonLimit = Set(
    "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
    "grpc.health.v1.Health/Check",
    "grpc.health.v1.Health/Watch",
  )
}
