// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import java.time.Duration

import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.commands.Commands
import scalaz.syntax.tag._

case class TrackerCommandInput(commandId: String, submissionId: String, timeout: Option[Duration])

object TrackerCommandInput {

  def apply(commands: domain.Commands, timeout: Option[Duration]): TrackerCommandInput =
    TrackerCommandInput(
      commands.commandId.unwrap,
      commands.submissionId.map(_.unwrap).getOrElse(""),
      timeout,
    )

  def apply(commands: Commands, timeout: Option[Duration]): TrackerCommandInput =
    TrackerCommandInput(
      commands.commandId,
      commands.submissionId,
      timeout,
    )
}
trait Trackable[T] {

  def trackingCommandInput(value: T): TrackerCommandInput
}

object Trackable {
  implicit class TrackableOps[T](value: T)(implicit trackable: Trackable[T]) {

    def trackingInput: TrackerCommandInput = trackable.trackingCommandInput(value)

  }
}

