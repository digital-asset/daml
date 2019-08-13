// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.http.util

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.ledger.api.refinements.ApiTypes.{
  ApplicationId,
  CommandId,
  Party,
  WorkflowId
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.LedgerClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class ClientUtil(
    client: LedgerClient,
    applicationId: ApplicationId,
    ttl: Duration,
    timeProvider: TimeProvider) {

  import ClientUtil._

  private val ledgerId = client.ledgerId
  private val packageClient = client.packageClient
  private val transactionClient = client.transactionClient

  def listPackages(implicit ec: ExecutionContext): Future[Set[String]] =
    packageClient.listPackages().map(_.packageIds.toSet)

  def ledgerEnd(implicit ec: ExecutionContext): Future[LedgerOffset] =
    transactionClient.getLedgerEnd.flatMap(response => toFuture(response.offset))

  def nextTransaction(party: Party, offset: LedgerOffset)(
      implicit mat: Materializer): Future[Transaction] =
    transactionClient
      .getTransactions(offset, None, transactionFilter(party))
      .take(1L)
      .runWith(Sink.head)

  def subscribe(party: Party, offset: LedgerOffset, max: Option[Long])(f: Transaction => Unit)(
      implicit mat: Materializer): Future[Done] = {
    val source: Source[Transaction, NotUsed] =
      transactionClient.getTransactions(offset, None, transactionFilter(party))
    max.fold(source)(n => source.take(n)) runForeach f
  }

  override lazy val toString: String = s"ClientUtil{ledgerId=$ledgerId}"
}

object ClientUtil {
  def transactionFilter(ps: Party*): TransactionFilter =
    TransactionFilter(Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap)

  def uniqueId(): String = UUID.randomUUID.toString

  def workflowIdFromParty(p: Party): WorkflowId =
    WorkflowId(s"${Party.unwrap(p)} Workflow")

  def uniqueCommandId(): CommandId = CommandId(uniqueId())
}
