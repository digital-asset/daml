// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.apiserver.ratelimiting.MemoryCheck.*
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.event.Level

import java.lang.management.{MemoryMXBean, MemoryPoolMXBean, MemoryType, MemoryUsage}
import scala.concurrent.duration.DurationInt

/** Note that most of the check functionality is tested via [[RateLimitingInterceptorSpec]] */
class MemoryCheckSpec extends AnyFlatSpec with BaseTest {

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
    Threading.sleep(2 * delayBetweenCalls.toMillis)
    underTest.gc()
    verify(delegate, times(2)).gc()
    succeed
  }

  it should "use largest tenured pool as rate limiting pool" in {
    val expected = underLimitMemoryPoolMXBean()
    when(expected.getCollectionUsage).thenReturn(new MemoryUsage(0, 0, 0, 100))
    loggerFactory.assertLogs(MemoryCheckSpecSuppressionRule)(
      within = {
        findTenuredMemoryPool(config, Nil, errorLoggingContext) shouldBe None
      },
      assertions = _.errorMessage should include("Could not find tenured memory pool"),
    )
    findTenuredMemoryPool(
      config,
      List(
        underLimitMemoryPoolMXBean(),
        expected,
        underLimitMemoryPoolMXBean(),
      ),
      errorLoggingContext,
    ) shouldBe Some(expected)
  }

  val MemoryCheckSpecSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[MemoryCheckSpec] && SuppressionRule.Level(Level.ERROR)

}
