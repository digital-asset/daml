// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.refinements

import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, LedgerId}
import com.daml.ledger.api.v1.commands.Commands
import scalaz.syntax.tag._

class CompositeCommandAdapter(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
) {
  def transform(c: CompositeCommand): Commands =
    Commands(
      ledgerId = ledgerId.unwrap,
      workflowId = c.workflowId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = c.commandId.unwrap,
      party = c.party.unwrap,
      commands = c.commands,
    )
}

object CompositeCommandAdapter {
  def apply(
      ledgerId: LedgerId,
      applicationId: ApplicationId,
  ) = new CompositeCommandAdapter(ledgerId, applicationId)
}
