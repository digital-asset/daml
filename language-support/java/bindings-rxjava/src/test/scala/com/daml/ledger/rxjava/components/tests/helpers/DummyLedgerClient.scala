// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.tests.helpers

import java.time.{Duration, Instant}
import java.util
import java.util.Optional

import com.daml.ledger.javaapi.data._
import com.daml.ledger.rxjava._
import com.google.protobuf.Empty
import io.reactivex.{Flowable, Single}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class DummyLedgerClient(
    val ledgerId: String,
    activeContractSet: Flowable[GetActiveContractsResponse],
    boundedTransactions: Flowable[Transaction],
    transactions: Flowable[Transaction],
    commandCompletions: Flowable[CompletionStreamResponse],
    ledgerEnd: LedgerOffset)
    extends LedgerClient {

  private val logger = LoggerFactory.getLogger(getClass)

  val submitted: mutable.Buffer[SubmitCommandsRequest] = mutable.Buffer.empty[SubmitCommandsRequest]

  override def getLedgerId: String = ledgerId

  override def getActiveContractSetClient: ActiveContractsClient = new ActiveContractsClient {

    override def getActiveContracts(
        filter: TransactionFilter,
        verbose: Boolean): Flowable[GetActiveContractsResponse] =
      activeContractSet

    override def getActiveContracts(
        filter: TransactionFilter,
        verbose: Boolean,
        accessToken: String): Flowable[GetActiveContractsResponse] =
      untestedEndpoint
  }

  override def getTransactionsClient: TransactionsClient = new TransactionsClient {

    override def getTransactions(
        begin: LedgerOffset,
        end: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean): Flowable[Transaction] =
      (if (end == LedgerOffset.LedgerEnd.getInstance()) transactions else boundedTransactions)
        .filter(t =>
          DummyLedgerOffsetOrdering.compareWithAbsoluteValue(begin, t.getOffset) <= 0 &&
            DummyLedgerOffsetOrdering.compareWithAbsoluteValue(end, t.getOffset) >= 0)
        .map(t => {
          logger.debug(s"DummyLedgerClient.getTransactions emit $t")
          t
        })

    override def getTransactions(
        begin: LedgerOffset,
        end: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean,
        accessToken: String): Flowable[Transaction] = untestedEndpoint

    override def getTransactions(
        begin: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean): Flowable[Transaction] =
      getTransactions(begin, LedgerOffset.LedgerEnd.getInstance(), filter, verbose)

    override def getTransactions(
        begin: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean,
        accessToken: String): Flowable[Transaction] = untestedEndpoint

    override def getTransactionsTrees(
        begin: LedgerOffset,
        end: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean): Flowable[TransactionTree] = untestedEndpoint

    override def getTransactionsTrees(
        begin: LedgerOffset,
        end: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean,
        accessToken: String): Flowable[TransactionTree] = untestedEndpoint

    override def getTransactionsTrees(
        begin: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean): Flowable[TransactionTree] = untestedEndpoint

    override def getTransactionsTrees(
        begin: LedgerOffset,
        filter: TransactionFilter,
        verbose: Boolean,
        accessToken: String): Flowable[TransactionTree] = untestedEndpoint

    override def getTransactionByEventId(
        eventId: String,
        requestingParties: util.Set[String]): Single[TransactionTree] = untestedEndpoint

    override def getTransactionByEventId(
        eventId: String,
        requestingParties: util.Set[String],
        accessToken: String): Single[TransactionTree] = untestedEndpoint

    override def getTransactionById(
        transactionId: String,
        requestingParties: util.Set[String]): Single[TransactionTree] =
      untestedEndpoint

    override def getTransactionById(
        transactionId: String,
        requestingParties: util.Set[String],
        accessToken: String): Single[TransactionTree] = untestedEndpoint

    override def getFlatTransactionByEventId(
        eventId: String,
        requestingParties: util.Set[String]): Single[Transaction] = untestedEndpoint

    override def getFlatTransactionByEventId(
        eventId: String,
        requestingParties: util.Set[String],
        accessToken: String): Single[Transaction] = untestedEndpoint

    override def getFlatTransactionById(
        transactionId: String,
        requestingParties: util.Set[String]): Single[Transaction] = untestedEndpoint

    override def getFlatTransactionById(
        transactionId: String,
        requestingParties: util.Set[String],
        accessToken: String): Single[Transaction] = untestedEndpoint

    override def getLedgerEnd: Single[LedgerOffset] = Single.just(ledgerEnd)

    override def getLedgerEnd(accessToken: String): Single[LedgerOffset] = untestedEndpoint
  }

  override def getCommandCompletionClient: CommandCompletionClient = new CommandCompletionClient {
    override def completionStream(
        applicationId: String,
        offset: LedgerOffset,
        parties: util.Set[String]): Flowable[CompletionStreamResponse] =
      commandCompletions

    override def completionStream(
        applicationId: String,
        offset: LedgerOffset,
        parties: util.Set[String],
        accessToken: String): Flowable[CompletionStreamResponse] =
      untestedEndpoint

    override def completionStream(
        applicationId: String,
        parties: util.Set[String]): Flowable[CompletionStreamResponse] =
      commandCompletions

    override def completionStream(
        applicationId: String,
        parties: util.Set[String],
        accessToken: String): Flowable[CompletionStreamResponse] =
      untestedEndpoint

    override def completionEnd(): Single[CompletionEndResponse] = untestedEndpoint

    override def completionEnd(accessToken: String): Single[CompletionEndResponse] =
      untestedEndpoint
  }

  override def getCommandSubmissionClient: CommandSubmissionClient = new CommandSubmissionClient {
    override def submit(
        workflowId: String,
        applicationId: String,
        commandId: String,
        party: String,
        minLedgerTimeAbs: Optional[Instant],
        minLedgerTimeRel: Optional[Duration],
        deduplicationTime: Optional[Duration],
        commands: util.List[Command]): Single[Empty] = {
      submitted.append(
        new SubmitCommandsRequest(
          workflowId,
          applicationId,
          commandId,
          party,
          minLedgerTimeAbs,
          minLedgerTimeRel,
          deduplicationTime,
          commands))
      Single.just(Empty.getDefaultInstance)
    }

    override def submit(
        workflowId: String,
        applicationId: String,
        commandId: String,
        party: String,
        minLedgerTimeAbs: Optional[Instant],
        minLedgerTimeRel: Optional[Duration],
        deduplicationTime: Optional[Duration],
        commands: util.List[Command],
        accessToken: String): Single[Empty] = untestedEndpoint

    override def submit(
        workflowId: String,
        applicationId: String,
        commandId: String,
        party: String,
        commands: util.List[Command]): Single[Empty] = untestedEndpoint

    override def submit(
        workflowId: String,
        applicationId: String,
        commandId: String,
        party: String,
        commands: util.List[Command],
        accessToken: String): Single[Empty] = untestedEndpoint
  }

  override def getLedgerIdentityClient: LedgerIdentityClient = new LedgerIdentityClient {
    override def getLedgerIdentity: Single[String] = Single.just(ledgerId)

    override def getLedgerIdentity(accessToken: String): Single[String] = untestedEndpoint
  }

  override def getPackageClient: PackageClient = untestedEndpoint

  override def getLedgerConfigurationClient: LedgerConfigurationClient = untestedEndpoint

  override def getCommandClient: CommandClient = untestedEndpoint

  override def getTimeClient: TimeClient = untestedEndpoint
}

object DummyLedgerOffsetOrdering extends Ordering[LedgerOffset] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def compare(x: LedgerOffset, y: LedgerOffset): Int = {
    val r =
      if (x == y) 0
      else
        (x, y) match {
          case (_: LedgerOffset.LedgerBegin, _) => -1
          case (_: LedgerOffset.LedgerEnd, _) => 1
          case (_, _: LedgerOffset.LedgerBegin) => 1
          case (_, _: LedgerOffset.LedgerEnd) => -1
          case (a1: LedgerOffset.Absolute, a2: LedgerOffset.Absolute) =>
            a1.asInstanceOf[LedgerOffset.Absolute]
              .getOffset
              .compareTo(a2.asInstanceOf[LedgerOffset.Absolute].getOffset)
          case (_, _) =>
            throw new IllegalStateException(
              s"DummyLedgerOffsetOrdering failed to compare $x and $y")
        }
    logger.debug(s"compare($x, $y): $r")
    r
  }

  def compareWithAbsoluteValue(offset: LedgerOffset, absoluteOffsetValue: String): Int =
    compare(offset, new LedgerOffset.Absolute(absoluteOffsetValue))

}
