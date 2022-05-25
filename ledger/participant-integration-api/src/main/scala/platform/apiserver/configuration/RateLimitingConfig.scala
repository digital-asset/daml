// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

final case class RateLimitingConfig(
    maxApiServicesQueueSize: Int
)

case object RateLimitingConfig {
  val default: RateLimitingConfig = RateLimitingConfig(maxApiServicesQueueSize = 1000)
}
