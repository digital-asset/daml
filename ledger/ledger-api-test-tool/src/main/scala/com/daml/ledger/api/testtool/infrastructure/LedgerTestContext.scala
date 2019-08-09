// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.ledger.client.binding.{Contract, Primitive, Template, ValueDecoder}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

final class LedgerTestContext(
    val applicationId: String,
    val offsetAtStart: LedgerOffset,
    bindings: LedgerBindings)(implicit val ec: ExecutionContext)
    extends ExecutionContext {

  override def execute(runnable: Runnable): Unit = ec.execute(runnable)
  override def reportFailure(cause: Throwable): Unit = ec.reportFailure(cause)

  private[this] val nextPartyHintId: () => String = {
    val it = Iterator.from(0).map(n => s"$applicationId-party-$n")
    () =>
      it.synchronized(it.next())
  }
  private[this] val nextCommandId: () => String = {
    val it = Iterator.from(0).map(n => s"$applicationId-command-$n")
    () =>
      it.synchronized(it.next())
  }

  val ledgerId: Future[String] = bindings.ledgerId

  def allocateParty(): Future[String] =
    bindings.allocateParty(nextPartyHintId())

  def time: Future[Instant] = bindings.time

  def passTime(t: Duration): Future[Unit] = bindings.passTime(t)

  def activeContracts(
      parties: Seq[String],
      templateIds: Seq[Identifier]): Future[Vector[CreatedEvent]] =
    bindings.activeContracts(parties, templateIds)

  def create[T <: Template[T]: ValueDecoder](
      party: String,
      template: Template[T]): Future[Contract[T]] =
    bindings.create(party, applicationId, nextCommandId(), template)

  def create(party: String, templateId: Identifier, args: Map[String, Value.Sum]): Future[String] =
    bindings.create(party, applicationId, nextCommandId(), templateId, args)

  def exercise(
      party: String,
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum]
  ): Future[Unit] =
    bindings.exercise(party, applicationId, nextCommandId(), templateId, contractId, choice, args)

  def exercise[T](
      party: String,
      exercise: Primitive.Update[T]
  ): Future[Unit] = bindings.exercise(party, applicationId, nextCommandId(), exercise)

  def flatTransactions(
      parties: Seq[String],
      templateIds: Seq[Identifier]): Future[Vector[Transaction]] =
    bindings.flatTransactions(offsetAtStart, parties, templateIds)

  def transactionTrees(
      parties: Seq[String],
      templateIds: Seq[Identifier]): Future[Vector[TransactionTree]] =
    bindings.transactionTrees(offsetAtStart, parties, templateIds)

  def semanticTesterLedger(parties: Set[Ref.Party], packages: Map[Ref.PackageId, Ast.Package]) =
    new SemanticTesterLedger(bindings)(parties, packages)(this)

}
