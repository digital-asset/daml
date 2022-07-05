// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.metrics.{MetricName, Metrics}
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}
import com.daml.platform.apiserver.ratelimiting.RateLimitingInterceptor._
import com.daml.platform.apiserver.ratelimiting.ThreadpoolCheck.ThreadpoolCount
import com.daml.platform.configuration.ServerRole
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}
import org.slf4j.LoggerFactory

import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryPoolMXBean}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

private[apiserver] final class RateLimitingInterceptor(
    metrics: Metrics,
    checks: List[LimitResultCheck],
) extends ServerInterceptor {

  private val activeStreamsCounter = metrics.daml.lapi.streams.active

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val isStream = !call.getMethodDescriptor.getType.serverSendsOneMessage()
    serviceOverloaded(fullMethodName, isStream) match {

      case OverLimit(damlError) =>
        val statusRuntimeException = damlError.asGrpcError
        call.close(statusRuntimeException.getStatus, statusRuntimeException.getTrailers)
        new ServerCall.Listener[ReqT]() {}

      case UnderLimit if isStream =>
        val delegate = next.startCall(call, headers)
        val listener =
          new OnCloseCallListener(delegate, runOnceOnTermination = () => activeStreamsCounter.dec())
        activeStreamsCounter.inc() // Only do after call above has returned
        listener

      case UnderLimit =>
        next.startCall(call, headers)

    }

  }

  private def serviceOverloaded(
      fullMethodName: String,
      isStream: Boolean,
  ): LimitResult = {
    if (doNonLimit.contains(fullMethodName)) {
      UnderLimit
    } else {
      checks.traverse(fullMethodName, isStream)
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

    val apiServices: ThreadpoolCount = new ThreadpoolCount(metrics)(
      "Api Services Threadpool",
      metrics.daml.lapi.threadpool.apiServices,
    )
    val indexDbThreadpool: ThreadpoolCount = new ThreadpoolCount(metrics)(
      "Index Database Connection Threadpool",
      MetricName(metrics.daml.index.db.threadpool.connection, ServerRole.ApiServer.threadPoolSuffix),
    )

    val activeStreamsName = metrics.daml.lapi.streams.activeName
    val activeStreamsCounter = metrics.daml.lapi.streams.active

    new RateLimitingInterceptor(
      metrics = metrics,
      checks = List[LimitResultCheck](
        MemoryCheck(tenuredMemoryPools, memoryMxBean, config),
        ThreadpoolCheck(apiServices, config.maxApiServicesQueueSize),
        ThreadpoolCheck(indexDbThreadpool, config.maxApiServicesIndexDbQueueSize),
        StreamCheck(activeStreamsCounter, activeStreamsName, config.maxStreams),
      ),
    )
  }

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
