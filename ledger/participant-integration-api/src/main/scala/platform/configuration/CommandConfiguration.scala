// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

/** Configuration for the Ledger API Command Service.
  *
  * @param maximumTrackingTimeout
  *        The duration that the command service will keep tracking an active command. If timeout is not
  *        specified on the gRPC layer, this will be used. If there is specified one, and this is a longer,
  *        then this will be used.
  */
final case class CommandConfiguration(
    maximumTrackingTimeout: Duration = CommandConfiguration.DefaultMaximumTrackingTimeout
)

object CommandConfiguration {
  val DefaultMaximumTrackingTimeout: Duration = Duration.ofMinutes(5)
  lazy val Default: CommandConfiguration = CommandConfiguration()
}
