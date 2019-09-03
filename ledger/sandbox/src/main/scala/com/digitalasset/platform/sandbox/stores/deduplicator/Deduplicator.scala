// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.deduplicator

import com.daml.ledger.participant.state.v1.Party
import com.digitalasset.ledger.api.domain.{ApplicationId, CommandId}

case class Deduplicator(transactions: Set[(Party, ApplicationId, CommandId)]) {
  def checkAndAdd(
      submitter: Party,
      applicationId: ApplicationId,
      commandId: CommandId): (Deduplicator, Boolean) =
    if (isDuplicate(submitter, applicationId, commandId)) {
      (this, true)
    } else {
      (Deduplicator(transactions + ((submitter, applicationId, commandId))), false)
    }

  def isDuplicate(submitter: Party, applicationId: ApplicationId, commandId: CommandId): Boolean =
    transactions.contains((submitter, applicationId, commandId))
}

object Deduplicator {

  def apply(): Deduplicator = Deduplicator(Set())
}
