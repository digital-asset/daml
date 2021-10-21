// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.platform.PruneBuffers
import com.daml.platform.store.appendonlydao.LedgerReadDao
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BaseLedgerSpec extends AnyWordSpec with MockitoSugar with Matchers {
  private val ledgerDao = mock[LedgerReadDao]
  private val pruneBuffers = mock[PruneBuffers]
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val baseLedger = new BaseLedger(
    ledgerDao = ledgerDao,
    pruneBuffers = pruneBuffers,
    ledgerId = LedgerId("some ledger id"),
    transactionsReader = null,
    contractStore = null,
    dispatcher = null,
  ) {}

  classOf[BaseLedger].getSimpleName when {
    "prune" should {
      "trigger prune on the ledger read dao and the buffers" in {
        val someOffset = Offset.fromByteArray(BigInt(1337L).toByteArray)
        baseLedger.prune(someOffset, pruneAllDivulgedContracts = true)

        verify(ledgerDao).prune(someOffset, pruneAllDivulgedContracts = true)
        verify(pruneBuffers).apply(someOffset)
      }
    }
  }
}
