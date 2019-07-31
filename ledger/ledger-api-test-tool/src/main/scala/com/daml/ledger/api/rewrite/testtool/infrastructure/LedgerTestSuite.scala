// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import com.daml.ledger.api.rewrite.testtool.infrastructure.LedgerTestSuite.SkipTestException
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value.Identifier

import scala.concurrent.Future

private[testtool] object LedgerTestSuite {

  final case class SkipTestException(override val getMessage: String) extends RuntimeException

}

private[testtool] abstract class LedgerTestSuite(val session: LedgerSession) {

  val name: String = getClass.getSimpleName

  val tests: Vector[LedgerTest] = Vector.empty

  final def skip(reason: String): Unit = throw new SkipTestException(reason)

  final def skipIf(reason: String)(p: LedgerSessionConfiguration => Boolean): Unit =
    if (p(session.config)) skip(reason)

  final def ledgerId()(implicit context: LedgerTestContext): Future[String] =
    context.ledgerId

  final def allocateParty()(implicit context: LedgerTestContext): Future[String] =
    context.allocateParty()

  final def allocateParties(n: Int)(implicit context: LedgerTestContext): Future[Vector[String]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  final def transactionsSinceTestStarted(party: String, templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[Transaction]] =
    context.transactionsSinceTestStarted(party, templateIds)

}
