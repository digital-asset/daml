// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck.normalization

import com.daml.ledger.participant.state.v1.RejectionReason.{Inconsistent, InvalidLedgerTime}
import com.daml.ledger.participant.state.v1.Update

trait UpdateNormalizer {
  def normalize(update: Update): Update
}

object InvalidLedgerTimeRejectionNormalizer extends UpdateNormalizer {
  override def normalize(update: Update): Update = update match {
    case commandRejected @ Update.CommandRejected(_, _, reason)
        if reason.isInstanceOf[InvalidLedgerTime] =>
      commandRejected.copy(reason = Inconsistent(reason.description))
    case _ => update
  }
}
