// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

/** Reaching either [[inputBufferSize]] or [[maxCommandsInFlight]] will trigger
  * back-pressure by [[com.daml.ledger.client.services.commands.CommandClient]].
  *
  * @param inputBufferSize
  *        Maximum number of commands waiting to be submitted for each party.
  * @param maxCommandsInFlight
  *        Maximum number of submitted commands waiting to be completed for each party.
  * @param trackerRetentionPeriod
  *        The duration that the command service will keep an active command tracker for a given set
  *        of parties. A longer period cuts down on the tracker instantiation cost for a party that
  *        seldom acts. A shorter period causes a quick removal of unused trackers.
  */
final case class CommandConfiguration(
    inputBufferSize: Int,
    maxCommandsInFlight: Int,
    trackerRetentionPeriod: Duration,
)

object CommandConfiguration {
  val DefaultTrackerRetentionPeriod: Duration = Duration.ofMinutes(5)

  lazy val default: CommandConfiguration =
    CommandConfiguration(
      inputBufferSize = 512,
      maxCommandsInFlight = 256,
      trackerRetentionPeriod = DefaultTrackerRetentionPeriod,
    )
}
