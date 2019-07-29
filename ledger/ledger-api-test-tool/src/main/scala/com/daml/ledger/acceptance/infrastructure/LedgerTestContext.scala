// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import java.time.Clock
import java.util.UUID

import com.digitalasset.ledger.api.v1.admin.party_management_service.AllocatePartyRequest
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionsRequest,
  GetTransactionsResponse
}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.{ExecutionContext, Future}

final class LedgerTestContext(executionContext: ExecutionContext, session: LedgerSession)
    extends ExecutionContext {
  override def execute(runnable: Runnable): Unit = executionContext.execute(runnable)
  override def reportFailure(cause: Throwable): Unit = executionContext.reportFailure(cause)

  implicit private[this] val ec: ExecutionContext = executionContext

  val applicationId: String = UUID.randomUUID.toString

  val offsetAtStart: Future[LedgerOffset] =
    for {
      id <- ledgerId
      response <- session.services.tx.getLedgerEnd(new GetLedgerEndRequest(id))
    } yield response.offset.get

  private def transactionFilter(party: String, templateIds: Seq[Identifier]) =
    new TransactionFilter(Map(party -> filter(templateIds)))

  private def filter(templateIds: Seq[Identifier]): Filters =
    new Filters(if (templateIds.isEmpty) None else Some(new InclusiveFilters(templateIds)))

  private def clock(): Future[Clock] =
    for (id <- ledgerId()) yield new LedgerClock(id, session.services.time)

  private def submitAndWaitCommand[A](service: SubmitAndWaitRequest => Future[A])(
      party: String,
      applicationId: String,
      command: Command.Command): Future[A] =
    for {
      id <- ledgerId()
      clock <- clock()
      now = clock.instant()
      inFiveSeconds = now.plusSeconds(5)
      a <- service(
        new SubmitAndWaitRequest(
          Some(new Commands(
            ledgerId = id,
            applicationId = applicationId,
            commandId = UUID.randomUUID().toString,
            party = party,
            ledgerEffectiveTime = Some(new Timestamp(now.getEpochSecond, now.getNano)),
            maximumRecordTime =
              Some(new Timestamp(inFiveSeconds.getEpochSecond, inFiveSeconds.getNano)),
            commands = Seq(new Command(command))
          ))))
    } yield a

  private def submitAndWait(
      party: String,
      applicationId: String,
      command: Command.Command): Future[Unit] =
    submitAndWaitCommand(session.services.cmd.submitAndWait)(party, applicationId, command).map(_ =>
      ())

  private def submitAndWaitForTransaction[A](
      party: String,
      applicationId: String,
      command: Command.Command)(f: Transaction => A): Future[A] =
    submitAndWaitCommand(session.services.cmd.submitAndWaitForTransaction)(
      party,
      applicationId,
      command)
      .map(r => f(r.transaction.get))

  private def createCommand(templateId: Identifier, args: Map[String, Value.Sum]): Command.Command =
    Create(
      new CreateCommand(
        Some(templateId),
        Some(
          new Record(
            fields = args.map {
              case (label, value) =>
                new RecordField(label, Some(new Value(value)))
            }(collection.breakOut)
          ))
      )
    )

  private def exerciseCommand(
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum]): Command.Command =
    Exercise(
      new ExerciseCommand(
        templateId = Some(templateId),
        contractId = contractId,
        choice = choice,
        choiceArgument = Some(
          new Value(
            Value.Sum.Record(new Record(
              fields = args.map {
                case (label, value) =>
                  new RecordField(label, Some(new Value(value)))
              }(collection.breakOut)
            ))))
      )
    )

  def ledgerId(): Future[String] =
    for {
      response <- session.services.id.getLedgerIdentity(new GetLedgerIdentityRequest)
    } yield response.ledgerId

  def allocateParty(): Future[String] =
    session.services.party.allocateParty(new AllocatePartyRequest()).map(_.partyDetails.get.party)

  def allocateParties(n: Int): Future[Vector[String]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  def transactionsUntilNow(
      begin: LedgerOffset,
      party: String,
      templateIds: Identifier*): Future[Vector[Transaction]] =
    for {
      id <- ledgerId()
      responses <- FiniteStreamObserver[GetTransactionsResponse](
        session.services.tx
          .getTransactions(
            new GetTransactionsRequest(
              ledgerId = id,
              begin = Some(begin),
              end = Some(ledgerEnd),
              filter = Some(transactionFilter(party, templateIds)),
              verbose = true
            ),
            _
          ))
    } yield responses.flatMap(_.transactions)

  def create(party: String, templateId: Identifier, args: Map[String, Value.Sum]): Future[String] =
    submitAndWaitForTransaction(party, applicationId, createCommand(templateId, args)) {
      _.events.collect { case Event(Created(e)) => e.contractId }.head
    }

  def exercise(
      party: String,
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum]
  ): Future[Unit] =
    submitAndWait(party, applicationId, exerciseCommand(templateId, contractId, choice, args))

  def transactionsSinceStart(party: String, templateIds: Identifier*): Future[Vector[Transaction]] =
    for {
      begin <- offsetAtStart
      transactions <- transactionsUntilNow(begin, party, templateIds: _*)
    } yield transactions

}
