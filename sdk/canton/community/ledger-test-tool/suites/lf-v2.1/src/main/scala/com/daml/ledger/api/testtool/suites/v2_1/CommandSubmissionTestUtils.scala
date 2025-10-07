// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.test.java.model.test.Dummy

trait CommandSubmissionTestUtils { this: LedgerTestSuite =>
  protected def assertOnTransactionResponse(
      transaction: Transaction
  ): Unit = {
    assert(
      transaction.updateId.nonEmpty,
      "The transaction identifier was empty but shouldn't.",
    )
    val event = transaction.events.head
    assert(
      event.event.isCreated,
      s"The returned transaction should contain a created-event, but was ${event.event}",
    )
    assert(
      event.getCreated.getTemplateId == Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      s"The template ID of the created-event should by ${Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1}, but was ${event.getCreated.getTemplateId}",
    )
  }
}
