// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Reaching either [[inputBufferSize]] or [[maxCommandsInFlight]] will trigger
  * back-pressure by [[com.daml.ledger.client.services.commands.CommandClient]].
  *
  * Reaching [[maxParallelSubmissions]] will trigger back-pressure
  * by [[com.daml.platform.sandbox.stores.ledger.sql.SqlLedger]].
  *
  * @param inputBufferSize
  *        Maximum number of commands waiting to be submitted for each party.
  * @param maxParallelSubmissions
  *        Maximum number of commands waiting to be sequenced after being evaluated by the engine.
  *        This does _not_ apply to on-X ledgers, where sequencing happens after the evaluated
  *        transaction has been shipped via the WriteService.
  * @param maxCommandsInFlight
  *        Maximum number of submitted commands waiting to be completed for each party.
  * @param limitMaxCommandsInFlight
  *        Whether [[maxCommandsInFlight]] should be honored or not.
  * @param retentionPeriod
  *        For how long the command service will keep an active command tracker for a given party.
  *        A longer retention period allows to not instantiate a new tracker for a party that seldom acts.
  *        A shorter retention period allows to quickly remove unused trackers.
  */
final case class CommandConfiguration(
    inputBufferSize: Int,
    maxParallelSubmissions: Int,
    maxCommandsInFlight: Int,
    limitMaxCommandsInFlight: Boolean,
    retentionPeriod: FiniteDuration,
)

object CommandConfiguration {
  val DefaultTrackerRetentionPeriod: FiniteDuration = 5.minutes

  lazy val default: CommandConfiguration =
    CommandConfiguration(
      inputBufferSize = 512,
      maxParallelSubmissions = 512,
      maxCommandsInFlight = 256,
      limitMaxCommandsInFlight = true,
      retentionPeriod = DefaultTrackerRetentionPeriod,
    )
}
