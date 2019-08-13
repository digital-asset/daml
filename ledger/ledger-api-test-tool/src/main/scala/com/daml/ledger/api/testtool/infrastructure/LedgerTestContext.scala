// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.binding.Primitive.Party
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
  val nextCommandId: () => String = {
    val it = Iterator.from(0).map(n => s"$applicationId-command-$n")
    () =>
      it.synchronized(it.next())
  }

  val ledgerId: Future[String] = bindings.ledgerId

  def allocateParty(): Future[Party] =
    bindings.allocateParty(nextPartyHintId())

  def time: Future[Instant] = bindings.time

  def passTime(t: Duration): Future[Unit] = bindings.passTime(t)

  def activeContracts(
      parties: Seq[Party],
      templateIds: Seq[Identifier]): Future[Vector[CreatedEvent]] =
    bindings.activeContracts(parties, templateIds)

  def create[T <: Template[T]: ValueDecoder](
      party: Party,
      template: Template[T]): Future[Contract[T]] =
    bindings.create(party, applicationId, nextCommandId(), template)

  def createAndGetTransactionId[T <: Template[T]: ValueDecoder](
      party: Party,
      template: Template[T]): Future[(String, Contract[T])] =
    bindings.createAndGetTransactionId(party, applicationId, nextCommandId(), template)

  def exercise[T](
      party: Party,
      exercise: Party => Primitive.Update[T]
  ): Future[TransactionTree] = bindings.exercise(party, applicationId, nextCommandId(), exercise)

  def flatTransactions(
      parties: Seq[Party],
      templateIds: Seq[Identifier]): Future[Vector[Transaction]] =
    bindings.flatTransactions(offsetAtStart, parties, templateIds)

  def transactionTrees(
      parties: Seq[Party],
      templateIds: Seq[Identifier]): Future[Vector[TransactionTree]] =
    bindings.transactionTrees(offsetAtStart, parties, templateIds)

  def transactionTreeById(transactionId: String, parties: Seq[Party]): Future[TransactionTree] =
    bindings.getTransactionById(transactionId, parties)

  def semanticTesterLedger(parties: Set[Ref.Party], packages: Map[Ref.PackageId, Ast.Package]) =
    new SemanticTesterLedger(bindings)(parties, packages)(this)

}
