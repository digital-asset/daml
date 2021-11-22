// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import java.time.Duration

import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.client.services.commands.tracker.{Trackable, TrackerCommandInput}

case class CommandSubmission(commands: Commands, timeout: Option[Duration] = None)

object CommandSubmission {
  implicit val commandSubmissionTrackable: Trackable[CommandSubmission] =
    (value: CommandSubmission) => TrackerCommandInput(value.commands, value.timeout)
}
