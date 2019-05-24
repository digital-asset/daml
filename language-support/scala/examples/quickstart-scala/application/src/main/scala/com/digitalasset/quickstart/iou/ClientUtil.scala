// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.quickstart.iou

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId, WorkflowId}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.binding.{Primitive => P}
import com.digitalasset.quickstart.iou.FutureUtil.toFuture
import com.google.protobuf.empty.Empty

import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class ClientUtil(
    client: LedgerClient,
    applicationId: ApplicationId,
    ttl: Duration,
    timeProvider: TimeProvider) {

  import ClientUtil._

  private val ledgerId = client.ledgerId
  private val commandClient = client.commandClient
  private val transactionClient = client.transactionClient

  def ledgerEnd(implicit ec: ExecutionContext): Future[LedgerOffset] =
    transactionClient.getLedgerEnd.flatMap(response => toFuture(response.offset))

  def submitCommand[T](
      sender: P.Party,
      workflowId: WorkflowId,
      command: P.Update[P.ContractId[T]]): Future[Empty] = {
    commandClient.submitSingleCommand(submitRequest(sender, workflowId, command))
  }

  def submitRequest[T](
      party: P.Party,
      workflowId: WorkflowId,
      seq: P.Update[P.ContractId[T]]*): SubmitRequest = {
    val now = timeProvider.getCurrentTime
    val commands = Commands(
      ledgerId = ledgerId.unwrap,
      workflowId = WorkflowId.unwrap(workflowId),
      applicationId = ApplicationId.unwrap(applicationId),
      commandId = uniqueId,
      party = P.Party.unwrap(party),
      ledgerEffectiveTime = Some(fromInstant(now)),
      maximumRecordTime = Some(fromInstant(now.plusNanos(ttl.toNanos))),
      commands = seq.map(_.command)
    )
    SubmitRequest(Some(commands), None)
  }

  def nextTransaction(party: P.Party, offset: LedgerOffset)(
      implicit mat: Materializer): Future[Transaction] =
    transactionClient
      .getTransactions(offset, None, transactionFilter(party))
      .take(1L)
      .runWith(Sink.head)

  def subscribe(party: P.Party, offset: LedgerOffset, max: Option[Long])(f: Transaction => Unit)(
      implicit mat: Materializer): Future[Done] = {
    val source: Source[Transaction, NotUsed] =
      transactionClient.getTransactions(offset, None, transactionFilter(party))
    max.fold(source)(n => source.take(n)) runForeach f
  }

  override lazy val toString: String = s"ClientUtil{ledgerId=$ledgerId}"
}

object ClientUtil {
  def transactionFilter(ps: P.Party*): TransactionFilter =
    TransactionFilter(P.Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap)

  def uniqueId: String = UUID.randomUUID.toString

  def workflowIdFromParty(p: P.Party): WorkflowId =
    WorkflowId(s"${P.Party.unwrap(p): String} Workflow")
}
