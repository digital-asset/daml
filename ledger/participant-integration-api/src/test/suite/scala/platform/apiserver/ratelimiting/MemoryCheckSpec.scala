// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.apiserver.ratelimiting.MemoryCheck._
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.management.{MemoryMXBean, MemoryPoolMXBean, MemoryType, MemoryUsage}
import scala.concurrent.duration.DurationInt

/** Note that most of the check functionality is tested via [[RateLimitingInterceptorSpec]] */
class MemoryCheckSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val config = RateLimitingConfig(100, 10, 75, 100 * RateLimitingConfig.Megabyte, 100)

  // For tests that do not involve memory
  private def underLimitMemoryPoolMXBean(): MemoryPoolMXBean = {
    val memoryPoolBean = mock[MemoryPoolMXBean]
    when(memoryPoolBean.getType).thenReturn(MemoryType.HEAP)
    when(memoryPoolBean.getName).thenReturn("UnderLimitPool")
    when(memoryPoolBean.getCollectionUsage).thenReturn(new MemoryUsage(0, 0, 0, 0))
    when(memoryPoolBean.isCollectionUsageThresholdSupported).thenReturn(true)
    when(memoryPoolBean.isCollectionUsageThresholdExceeded).thenReturn(false)
  }

  behavior of "MemoryCheck"

  it should "throttle calls to GC" in {
    val delegate = mock[MemoryMXBean]
    val delayBetweenCalls = 100.milliseconds
    val underTest = new GcThrottledMemoryBean(delegate, delayBetweenCalls)
    underTest.gc()
    underTest.gc()
    verify(delegate, times(1)).gc()
    Thread.sleep(2 * delayBetweenCalls.toMillis)
    underTest.gc()
    verify(delegate, times(2)).gc()
    succeed
  }

  it should "use largest tenured pool as rate limiting pool" in {
    val expected = underLimitMemoryPoolMXBean()
    when(expected.getCollectionUsage).thenReturn(new MemoryUsage(0, 0, 0, 100))
    findTenuredMemoryPool(config, Nil) shouldBe None
    findTenuredMemoryPool(
      config,
      List(
        underLimitMemoryPoolMXBean(),
        expected,
        underLimitMemoryPoolMXBean(),
      ),
    ) shouldBe Some(expected)
  }

}
