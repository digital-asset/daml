// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.http.util

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId, Party, WorkflowId}
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitRequest
}
import com.digitalasset.ledger.api.v1.commands.{Command, Commands}
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
  private val commandServiceClient = client.commandServiceClient
  private val transactionClient = client.transactionClient

  def listPackages(implicit ec: ExecutionContext): Future[Set[String]] =
    packageClient.listPackages().map(_.packageIds.toSet)

  def ledgerEnd(implicit ec: ExecutionContext): Future[LedgerOffset] =
    transactionClient.getLedgerEnd.flatMap(response => toFuture(response.offset))

  def submitCommandAndWait(
      party: Party,
      workflowId: WorkflowId,
      cmd: Command.Command): Future[SubmitAndWaitForTransactionResponse] = {
    val now = timeProvider.getCurrentTime
    val commands = Commands(
      ledgerId = LedgerId.unwrap(ledgerId),
      workflowId = WorkflowId.unwrap(workflowId),
      applicationId = ApplicationId.unwrap(applicationId),
      commandId = uniqueId,
      party = Party.unwrap(party),
      ledgerEffectiveTime = Some(fromInstant(now)),
      maximumRecordTime = Some(fromInstant(now.plusNanos(ttl.toNanos))),
      commands = Seq(Command(cmd))
    )

    commandServiceClient.submitAndWaitForTransaction(SubmitAndWaitRequest(Some(commands), None))
  }

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

  def uniqueId: String = UUID.randomUUID.toString

  def workflowIdFromParty(p: String): WorkflowId =
    WorkflowId(s"$p Workflow")
}
