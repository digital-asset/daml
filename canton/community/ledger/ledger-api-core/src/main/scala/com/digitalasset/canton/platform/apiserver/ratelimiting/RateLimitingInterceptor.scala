// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}
import com.digitalasset.canton.platform.apiserver.ratelimiting.RateLimitingInterceptor.*
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}
import org.slf4j.LoggerFactory

import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryPoolMXBean}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

final class RateLimitingInterceptor(
    metrics: Metrics,
    checks: List[LimitResultCheck],
) extends ServerInterceptor {

  private val activeStreamsGauge = metrics.daml.lapi.streams.active

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
          new OnCloseCallListener(
            delegate,
            runOnceOnTermination = () => activeStreamsGauge.updateValue(_ - 1),
          )
        activeStreamsGauge.updateValue(_ + 1) // Only do after call above has returned
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

  def apply(
      loggerFactory: NamedLoggerFactory,
      metrics: Metrics,
      config: RateLimitingConfig,
      additionalChecks: List[LimitResultCheck] = List.empty,
  ): RateLimitingInterceptor = {
    apply(
      loggerFactory = loggerFactory,
      metrics = metrics,
      config = config,
      tenuredMemoryPools = ManagementFactory.getMemoryPoolMXBeans.asScala.toList,
      memoryMxBean = ManagementFactory.getMemoryMXBean,
      additionalChecks = additionalChecks,
    )
  }

  def apply(
      loggerFactory: NamedLoggerFactory,
      metrics: Metrics,
      config: RateLimitingConfig,
      tenuredMemoryPools: List[MemoryPoolMXBean],
      memoryMxBean: MemoryMXBean,
      additionalChecks: List[LimitResultCheck],
  ): RateLimitingInterceptor = {

    val activeStreamsName = metrics.daml.lapi.streams.activeName
    val activeStreamsCounter = metrics.daml.lapi.streams.active

    new RateLimitingInterceptor(
      metrics = metrics,
      checks = List[LimitResultCheck](
        MemoryCheck(tenuredMemoryPools, memoryMxBean, config, loggerFactory),
        StreamCheck(activeStreamsCounter, activeStreamsName, config.maxStreams, loggerFactory),
      ) ::: additionalChecks,
    )
  }

  private val doNonLimit: Set[String] = Set(
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
