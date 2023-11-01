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
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryPoolMXBean}
import scala.jdk.CollectionConverters.ListHasAsScala

final class RateLimitingInterceptor(
    checks: List[LimitResultCheck]
) extends ServerInterceptor {

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
      checks = List[LimitResultCheck](
        MemoryCheck(tenuredMemoryPools, memoryMxBean, config, loggerFactory),
        StreamCheck(activeStreamsCounter, activeStreamsName, config.maxStreams, loggerFactory),
      ) ::: additionalChecks
    )
  }

  private val doNonLimit: Set[String] = Set(
    "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
    "grpc.health.v1.Health/Check",
    "grpc.health.v1.Health/Watch",
  )

}
