// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

/** @param maxApiServicesQueueSize
  *  The maximum number of non-running items in the ApiServices execution service
  *
  * @param maxApiServicesIndexDbQueueSize
  *  The maximum number of non-running items in the IndexDb execution service
  *
  * @param maxUsedHeapSpacePercentage
  *   If, following a garbage collection of the 'tenured' memory pool, the percentage of used pool memory is
  *   above this percentage the system will be rate limited until additional space is freed up.
  *
  * @param minFreeHeapSpaceBytes
  *   If, following a garbage collection of the 'tenured' memory pool, the amount of free space is below
  *   this value the system will be rate limited until additional space is freed up.
  */
final case class RateLimitingConfig(
    maxApiServicesQueueSize: Int = 10000,
    maxApiServicesIndexDbQueueSize: Int = 1000,
    maxUsedHeapSpacePercentage: Int = 85,
    minFreeHeapSpaceBytes: Long = 300 * RateLimitingConfig.Megabyte,
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
