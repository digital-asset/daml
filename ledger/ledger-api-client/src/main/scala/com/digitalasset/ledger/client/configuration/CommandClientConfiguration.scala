// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.configuration

import java.time.Duration

/**
  * @param maxCommandsInFlight The maximum number of unconfirmed commands the client may track.
  *                            The client will backpressure when this number is reached.
  * @param maxParallelSubmissions The maximum number of parallel command submissions at a given time.
  *                               The client will backpressure when this number is reached.
  * @param overrideTtl If true, the ttls of incoming commands will be set to [[ttl]]
  * @param ttl The TTL to set on commands
  */
final case class CommandClientConfiguration(
    maxCommandsInFlight: Int,
    maxParallelSubmissions: Int,
    overrideTtl: Boolean,
    ttl: Duration)

object CommandClientConfiguration {
  def default = CommandClientConfiguration(1, 1, true, Duration.ofSeconds(30L))
}
