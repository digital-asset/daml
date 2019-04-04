// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.refinements

import java.time.Duration

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId, LedgerId}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import scalaz.syntax.tag._

class CompositeCommandAdapter(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
    ttl: Duration,
    timeProvider: TimeProvider) {
  def transform(c: CompositeCommand): SubmitRequest = {
    val now = timeProvider.getCurrentTime

    val commands = Commands(
      ledgerId.unwrap,
      c.workflowId.unwrap,
      applicationId.unwrap,
      c.commandId.unwrap,
      c.party.unwrap,
      Some(fromInstant(now)),
      Some(fromInstant(now plus ttl)),
      c.commands
    )

    SubmitRequest(Some(commands), c.traceContext)
  }

}

object CompositeCommandAdapter {
  def apply(
      ledgerId: LedgerId,
      applicationId: ApplicationId,
      ttl: Duration,
      timeProvider: TimeProvider) =
    new CompositeCommandAdapter(ledgerId, applicationId, ttl, timeProvider)
}
