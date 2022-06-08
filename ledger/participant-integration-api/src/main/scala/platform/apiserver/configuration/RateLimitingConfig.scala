// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

final case class RateLimitingConfig(
    maxApiServicesQueueSize: Int,
    maxApiServicesIndexDbQueueSize: Int,
    maxHeapSpacePercentage: Int,
) {
  def collectionUsageThreshold(maxPoolBytes: Long): Long =
    (maxHeapSpacePercentage * maxPoolBytes) / 100
}

case object RateLimitingConfig {

  val Default: RateLimitingConfig = RateLimitingConfig(
    maxApiServicesQueueSize = 10000,
    maxApiServicesIndexDbQueueSize = 1000,
    maxHeapSpacePercentage = 85,
  )
}
