// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

/** The memory based rate limiting parameters ([[maxUsedHeapSpacePercentage]] and [[minFreeHeapSpaceBytes]] are highly
  * sensitive to the operating environment and should only be configured where memory profiling has highlighted spikes
  * in memory usage that need to be flattened.
  *
  * @param maxApiServicesQueueSize
  *  The maximum number of non-running items in the ApiServices execution service
  * @param maxApiServicesIndexDbQueueSize
  *  The maximum number of non-running items in the IndexDb execution service
  * @param maxUsedHeapSpacePercentage
  *   If, following a garbage collection of the 'tenured' memory pool, the percentage of used pool memory is
  *   above this percentage the system will be rate limited until additional space is freed up.
  * @param minFreeHeapSpaceBytes
  *   If, following a garbage collection of the 'tenured' memory pool, the amount of free space is below
  *   this value the system will be rate limited until additional space is freed up.
  */
final case class RateLimitingConfig(
    maxApiServicesQueueSize: Int = 10000,
    maxApiServicesIndexDbQueueSize: Int = 1000,
    maxUsedHeapSpacePercentage: Int = 100,
    minFreeHeapSpaceBytes: Long = 0,
    maxStreams: Int = 1000,
) {
  def calculateCollectionUsageThreshold(maxPoolBytes: Long): Long = {
    val thresholdBasedOnUsedPercentage = (maxUsedHeapSpacePercentage * maxPoolBytes) / 100
    val thresholdBasedOnMinFreeSpace = maxPoolBytes - minFreeHeapSpaceBytes
    Math.max(thresholdBasedOnUsedPercentage, thresholdBasedOnMinFreeSpace)
  }
}

case object RateLimitingConfig {

  val Megabyte: Long = 1024L * 1024L

  val Default: RateLimitingConfig = RateLimitingConfig()
}
