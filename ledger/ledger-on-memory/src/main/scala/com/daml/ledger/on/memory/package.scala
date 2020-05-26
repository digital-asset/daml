package com.daml.ledger.on

import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerWriter}

package object memory {
  type Index = Int

  type KeyValueLedger = LedgerReader with LedgerWriter
}
