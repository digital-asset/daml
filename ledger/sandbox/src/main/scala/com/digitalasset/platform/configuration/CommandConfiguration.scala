// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.configuration

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class CommandConfiguration(
    inputBufferSize: Int,
    maxParallelSubmissions: Int,
    maxCommandsInFlight: Int,
    limitMaxCommandsInFlight: Boolean,
    historySize: Int,
    retentionPeriod: FiniteDuration
)

object CommandConfiguration {
  lazy val default =
    CommandConfiguration(
      inputBufferSize = 512,
      maxParallelSubmissions = 128,
      maxCommandsInFlight = 256,
      limitMaxCommandsInFlight = true,
      historySize = 5000,
      retentionPeriod = 24.hours
    )
}
