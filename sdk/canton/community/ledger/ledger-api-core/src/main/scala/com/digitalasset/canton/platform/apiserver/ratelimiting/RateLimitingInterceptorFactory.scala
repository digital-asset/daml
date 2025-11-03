// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.ratelimiting.LimitResult.LimitResultCheck
import com.digitalasset.canton.networking.grpc.ratelimiting.RateLimitingInterceptor
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig

import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryPoolMXBean}
import scala.jdk.CollectionConverters.ListHasAsScala

object RateLimitingInterceptorFactory {

  def create(
      loggerFactory: NamedLoggerFactory,
      config: RateLimitingConfig,
      additionalChecks: List[LimitResultCheck] = List.empty,
  ): RateLimitingInterceptor =
    createWithMXBeans(
      loggerFactory = loggerFactory,
      config = config,
      tenuredMemoryPools = ManagementFactory.getMemoryPoolMXBeans.asScala.toList,
      memoryMxBean = ManagementFactory.getMemoryMXBean,
      additionalChecks = additionalChecks,
    )

  def createWithMXBeans(
      loggerFactory: NamedLoggerFactory,
      config: RateLimitingConfig,
      tenuredMemoryPools: List[MemoryPoolMXBean],
      memoryMxBean: MemoryMXBean,
      additionalChecks: List[LimitResultCheck],
  ): RateLimitingInterceptor =
    new RateLimitingInterceptor(
      checks = List[LimitResultCheck](
        MemoryCheck(tenuredMemoryPools, memoryMxBean, config, loggerFactory)
      ) ::: additionalChecks
    )

}
