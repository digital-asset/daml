// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.Offset

class LedgerRecord(val offset: Offset, val entryId: DamlLogEntryId, val envelope: Array[Byte]) {}

object LedgerRecord {
  def apply(offset: Offset, entryId: DamlLogEntryId, envelope: Array[Byte]): LedgerRecord =
    new LedgerRecord(offset, entryId, envelope)
}
