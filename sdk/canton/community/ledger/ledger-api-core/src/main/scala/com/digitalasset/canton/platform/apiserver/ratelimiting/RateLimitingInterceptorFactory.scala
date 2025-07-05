// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.ratelimiting.LimitResult.LimitResultCheck
import com.digitalasset.canton.networking.grpc.ratelimiting.RateLimitingInterceptor
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig

import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryPoolMXBean}
import scala.jdk.CollectionConverters.ListHasAsScala

object RateLimitingInterceptorFactory {

  def create(
      loggerFactory: NamedLoggerFactory,
      metrics: LedgerApiServerMetrics,
      config: RateLimitingConfig,
      additionalChecks: List[LimitResultCheck] = List.empty,
  ): RateLimitingInterceptor =
    createWithMXBeans(
      loggerFactory = loggerFactory,
      metrics = metrics,
      config = config,
      tenuredMemoryPools = ManagementFactory.getMemoryPoolMXBeans.asScala.toList,
      memoryMxBean = ManagementFactory.getMemoryMXBean,
      additionalChecks = additionalChecks,
    )

  def createWithMXBeans(
      loggerFactory: NamedLoggerFactory,
      metrics: LedgerApiServerMetrics,
      config: RateLimitingConfig,
      tenuredMemoryPools: List[MemoryPoolMXBean],
      memoryMxBean: MemoryMXBean,
      additionalChecks: List[LimitResultCheck],
  ): RateLimitingInterceptor = {

    val activeStreamsName = metrics.lapi.streams.activeName
    val activeStreamsCounter = metrics.lapi.streams.active

    new RateLimitingInterceptor(
      checks = List[LimitResultCheck](
        MemoryCheck(tenuredMemoryPools, memoryMxBean, config, loggerFactory),
        StreamCheck(activeStreamsCounter, activeStreamsName, config.maxStreams, loggerFactory),
      ) ::: additionalChecks
    )
  }

}
