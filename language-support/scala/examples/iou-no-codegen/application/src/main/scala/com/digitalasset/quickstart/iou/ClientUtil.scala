// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.quickstart.iou

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, WorkflowId}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.quickstart.iou.FutureUtil.toFuture
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class ClientUtil(
    client: LedgerClient,
    applicationId: ApplicationId,
) {

  import ClientUtil._

  private val ledgerId = client.ledgerId
  private val packageClient = client.packageClient
  private val commandClient = client.commandClient
  private val transactionClient = client.transactionClient

  def listPackages(implicit ec: ExecutionContext): Future[Set[String]] =
    packageClient.listPackages().map(_.packageIds.toSet)

  def ledgerEnd(implicit ec: ExecutionContext): Future[LedgerOffset] =
    transactionClient.getLedgerEnd().flatMap(response => toFuture(response.offset))

  def submitCommand(party: String, workflowId: WorkflowId, cmd: Command.Command): Future[Empty] = {
    val commands = Commands(
      ledgerId = LedgerId.unwrap(ledgerId),
      workflowId = WorkflowId.unwrap(workflowId),
      applicationId = ApplicationId.unwrap(applicationId),
      commandId = uniqueId,
      party = party,
      commands = Seq(Command(cmd)),
    )

    commandClient.submitSingleCommand(SubmitRequest(Some(commands)))
  }

  def nextTransaction(party: String, offset: LedgerOffset)(implicit
      mat: Materializer
  ): Future[Transaction] =
    transactionClient
      .getTransactions(offset, None, transactionFilter(party))
      .take(1L)
      .runWith(Sink.head)

  def subscribe(party: String, offset: LedgerOffset, max: Option[Long])(
      f: Transaction => Unit
  )(implicit mat: Materializer): Future[Done] = {
    val source: Source[Transaction, NotUsed] =
      transactionClient.getTransactions(offset, None, transactionFilter(party))
    max.fold(source)(n => source.take(n)) runForeach f
  }

  override lazy val toString: String = s"ClientUtil{ledgerId=$ledgerId}"
}

object ClientUtil {
  def transactionFilter(parties: String*): TransactionFilter =
    TransactionFilter(parties.map((_, Filters.defaultInstance)).toMap)

  def uniqueId: String = UUID.randomUUID.toString

  def workflowIdFromParty(p: String): WorkflowId =
    WorkflowId(s"$p Workflow")
}
