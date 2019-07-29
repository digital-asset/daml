// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import com.daml.ledger.acceptance.infrastructure.LedgerTestSuite.SkipTestException
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.apitesting.TestTemplateIds

import scala.concurrent.{ExecutionContext, Future}

private[acceptance] object LedgerTestSuite {

  final case class SkipTestException(override val getMessage: String) extends RuntimeException

}

private[acceptance] abstract class LedgerTestSuite(val session: LedgerSession) {

  val name: String = getClass.getSimpleName

  val tests: Vector[LedgerTest] = Vector.empty

  final val templateIds = new TestTemplateIds(PlatformApplications.Config.default).templateIds

  final def skip(reason: String) = throw new SkipTestException(reason)

  final def skipIf(reason: String)(p: PartialFunction[LedgerSessionConfiguration, Boolean]) =
    if (p.lift(session.configuration).getOrElse(false)) skip(reason)

  final def offsetAtStart(implicit context: LedgerTestContext): Future[LedgerOffset] =
    context.offsetAtStart

  final def transactionsSinceStart(party: String, templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[Transaction]] =
    context.transactionsSinceStart(party, templateIds: _*)

  final def ledgerId()(implicit ec: ExecutionContext): Future[String] =
    session.ledgerId()

  final def allocateParty()(implicit ec: ExecutionContext): Future[String] =
    session.allocateParty()

  final def allocateParties(n: Int)(implicit ec: ExecutionContext): Future[Vector[String]] =
    session.allocateParties(n)

  final def create(party: String, templateId: Identifier, args: Map[String, Value.Sum])(
      implicit ec: ExecutionContext): Future[String] =
    session.create(party, templateId, args)

  final def exercise(
      party: String,
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum])(implicit ec: ExecutionContext): Future[Unit] =
    session.exercise(party, templateId, contractId, choice, args)

}
