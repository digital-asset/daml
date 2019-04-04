// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.deduplicator

import com.digitalasset.ledger.api.domain.{ApplicationId, CommandId}

case class Deduplicator(transactions: Set[(ApplicationId, CommandId)]) {
  def checkAndAdd(applicationId: ApplicationId, commandId: CommandId): (Deduplicator, Boolean) =
    if (isDuplicate(applicationId, commandId)) {
      (this, true)
    } else {
      (Deduplicator(transactions + ((applicationId, commandId))), false)
    }

  def isDuplicate(applicationId: ApplicationId, commandId: CommandId): Boolean =
    transactions.contains((applicationId, commandId))
}

object Deduplicator {

  def apply(): Deduplicator = Deduplicator(Set())
}
