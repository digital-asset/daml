// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.NonNegativeFiniteDuration

import java.time.Duration

/** Configuration for the Ledger API Command Service.
  *
  * @param defaultTrackingTimeout
  *        The duration that the command service will keep tracking an active command by default. This value
  *        will be used if a timeout is not specified on a gRPC request.
  * @param maxCommandsInFlight
  *        Maximum number of submitted commands waiting to be completed in parallel.
  *        Commands submitted after this limit is reached will be rejected.
  */
final case class CommandServiceConfig(
    defaultTrackingTimeout: NonNegativeFiniteDuration =
      CommandServiceConfig.DefaultDefaultTrackingTimeout,
    maxCommandsInFlight: Int = CommandServiceConfig.DefaultMaxCommandsInFlight,
)

object CommandServiceConfig {
  val DefaultDefaultTrackingTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    Duration.ofMinutes(5)
  )
  val DefaultMaxCommandsInFlight: Int = 256
  lazy val Default: CommandServiceConfig = CommandServiceConfig()
}
