// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

final case class RateLimitingConfig(
    maxApiServicesQueueSize: Int,
    maxApiServicesIndexDbQueueSize: Int,
    maxHeapSpacePercentage: Int,
    maxOverThresholdZoneSize: Long,
) {
  def calculateCollectionUsageThreshold(maxPoolBytes: Long): Long = {
    val percentageBasedThreshold = (maxHeapSpacePercentage * maxPoolBytes) / 100
    val zoneBasedThreshold = maxPoolBytes - maxOverThresholdZoneSize
    Math.max(percentageBasedThreshold, zoneBasedThreshold)
  }
}

case object RateLimitingConfig {

  val Megabyte: Long = 1024L * 1024L

  val Default: RateLimitingConfig = RateLimitingConfig(
    maxApiServicesQueueSize = 10000,
    maxApiServicesIndexDbQueueSize = 1000,
    maxHeapSpacePercentage = 85,
    maxOverThresholdZoneSize = 300 * Megabyte,
  )
}
