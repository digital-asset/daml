// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

/** Configuration for the Ledger API Command Service.
  *
  * @param inputBufferSize
  *        Maximum number of commands waiting to be submitted for each distinct set of parties,
  *        as specified by the `act_as` property of the command. Reaching this limit will cause the
  *        server to signal backpressure using the ``RESOURCE_EXHAUSTED`` gRPC status code.
  * @param maxCommandsInFlight
  *        Maximum number of submitted commands waiting to be completed in parallel, for each
  *        distinct set of parties, as specified by the `act_as` property of the command. Reaching
  *        this limit will cause new submissions to wait in the queue before being submitted.
  * @param trackerRetentionPeriod
  *        The duration that the command service will keep an active command tracker for a given set
  *        of parties. A longer period cuts down on the tracker instantiation cost for a party that
  *        seldom acts. A shorter period causes a quick removal of unused trackers.
  */
final case class CommandConfiguration(
    inputBufferSize: Int = 512,
    maxCommandsInFlight: Int = 256,
    trackerRetentionPeriod: Duration = CommandConfiguration.DefaultTrackerRetentionPeriod,
)

object CommandConfiguration {
  val DefaultTrackerRetentionPeriod: Duration = Duration.ofMinutes(5)
  lazy val Default: CommandConfiguration = CommandConfiguration()
}
