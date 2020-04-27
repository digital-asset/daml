// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.refinements

import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, LedgerId}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import scalaz.syntax.tag._

class CompositeCommandAdapter(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
) {
  def transform(c: CompositeCommand): SubmitRequest = {

    val commands = Commands(
      ledgerId.unwrap,
      c.workflowId.unwrap,
      applicationId.unwrap,
      c.commandId.unwrap,
      c.party.unwrap,
      c.commands
    )

    SubmitRequest(Some(commands), c.traceContext)
  }

}

object CompositeCommandAdapter {
  def apply(
      ledgerId: LedgerId,
      applicationId: ApplicationId,
  ) = new CompositeCommandAdapter(ledgerId, applicationId)
}
