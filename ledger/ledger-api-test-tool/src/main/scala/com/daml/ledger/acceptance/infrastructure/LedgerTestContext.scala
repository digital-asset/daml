// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value.Identifier

import scala.concurrent.{ExecutionContext, Future}

final class LedgerTestContext(executionContext: ExecutionContext, session: LedgerSession)
    extends ExecutionContext {
  override def execute(runnable: Runnable): Unit = executionContext.execute(runnable)
  override def reportFailure(cause: Throwable): Unit = executionContext.reportFailure(cause)

  implicit private[this] val ec: ExecutionContext = this

  val offsetAtStart: Future[LedgerOffset] = session.ledgerEnd()

  def transactionsSinceStart(party: String, templateIds: Identifier*): Future[Vector[Transaction]] =
    for {
      begin <- offsetAtStart
      transactions <- session.transactionsUntilNow(begin, party, templateIds: _*)
    } yield transactions

}
