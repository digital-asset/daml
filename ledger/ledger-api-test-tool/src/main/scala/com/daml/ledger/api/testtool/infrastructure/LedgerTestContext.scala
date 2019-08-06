// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

final class LedgerTestContext(
    val applicationId: String,
    val offsetAtStart: LedgerOffset,
    bindings: LedgerBindings)(implicit val ec: ExecutionContext)
    extends ExecutionContext {

  override def execute(runnable: Runnable): Unit = ec.execute(runnable)
  override def reportFailure(cause: Throwable): Unit = ec.reportFailure(cause)

  val ledgerId: Future[String] = bindings.ledgerId

  def allocateParty(): Future[String] =
    bindings.allocateParty()

  def time: Future[Instant] = bindings.time

  def passTime(t: Duration): Future[Unit] = bindings.passTime(t)

  def create(party: String, templateId: Identifier, args: Map[String, Value.Sum]): Future[String] =
    bindings.create(party, applicationId, templateId, args)

  def exercise(
      party: String,
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum]
  ): Future[Unit] =
    bindings.exercise(party, applicationId, templateId, contractId, choice, args)

  def transactionsSinceTestStarted(
      party: String,
      templateIds: Seq[Identifier]): Future[Vector[Transaction]] =
    bindings.transactions(offsetAtStart, party, templateIds)

  def semanticTesterLedger(parties: Set[Ref.Party], packages: Map[Ref.PackageId, Ast.Package]) =
    new SemanticTesterLedger(bindings)(parties, packages)(this)

}
