// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import java.time.Instant
import java.util.UUID

import com.digitalasset.ledger.api.v1.admin.party_management_service.AllocatePartyRequest
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
  GetTransactionsResponse
}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Channel

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

object LedgerBindings {

  private def filter(templateIds: Seq[Identifier]): Filters =
    new Filters(if (templateIds.isEmpty) None else Some(new InclusiveFilters(templateIds)))

  private def transactionFilter(party: String, templateIds: Seq[Identifier]) =
    new TransactionFilter(Map(party -> filter(templateIds)))

  private val end = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

}

final class LedgerBindings(channel: Channel)(implicit ec: ExecutionContext) {

  private[this] val services = new LedgerServices(channel)

  val ledgerId: Future[String] =
    for {
      response <- services.id.getLedgerIdentity(new GetLedgerIdentityRequest)
    } yield response.ledgerId

  private[this] val clock: Future[LedgerClock] =
    for (id <- ledgerId; clock <- LedgerClock(id, services.time)) yield clock

  def time: Future[Instant] = clock.map(_.instant)

  def passTime(t: Duration): Future[Unit] = clock.flatMap(_.passTime(t))

  def allocateParty(): Future[String] =
    services.party.allocateParty(new AllocatePartyRequest()).map(_.partyDetails.get.party)

  def ledgerEnd: Future[LedgerOffset] =
    for {
      id <- ledgerId
      response <- services.tx.getLedgerEnd(new GetLedgerEndRequest(id))
    } yield response.offset.get

  def transactions(
      begin: LedgerOffset,
      party: String,
      templateIds: Seq[Identifier]): Future[Vector[Transaction]] =
    for {
      id <- ledgerId
      txs <- FiniteStreamObserver[GetTransactionsResponse](
        services.tx
          .getTransactions(
            new GetTransactionsRequest(
              ledgerId = id,
              begin = Some(begin),
              end = Some(LedgerBindings.end),
              filter = Some(LedgerBindings.transactionFilter(party, templateIds)),
              verbose = true
            ),
            _
          ))
    } yield txs.flatMap(_.transactions)

  def getTransactionById(transactionId: String, parties: Seq[String]): Future[TransactionTree] =
    for {
      id <- ledgerId
      transaction <- services.tx.getTransactionById(
        new GetTransactionByIdRequest(id, transactionId, parties))
    } yield transaction.transaction.get

  def create(
      party: String,
      applicationId: String,
      templateId: Identifier,
      args: Map[String, Value.Sum]): Future[String] =
    submitAndWaitForTransaction(party, applicationId, createCommand(templateId, args)) {
      _.events.collect { case Event(Created(e)) => e.contractId }.head
    }

  def exercise(
      party: String,
      applicationId: String,
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum]
  ): Future[Unit] =
    submitAndWait(party, applicationId, exerciseCommand(templateId, contractId, choice, args))

  private def submitAndWaitCommand[A](service: SubmitAndWaitRequest => Future[A])(
      party: String,
      applicationId: String,
      command: Command.Command,
      commands: Command.Command*): Future[A] =
    for {
      id <- ledgerId
      let <- time
      mrt = let.plusSeconds(30)
      a <- service(
        new SubmitAndWaitRequest(
          Some(new Commands(
            ledgerId = id,
            applicationId = applicationId,
            commandId = UUID.randomUUID().toString,
            party = party,
            ledgerEffectiveTime = Some(new Timestamp(let.getEpochSecond, let.getNano)),
            maximumRecordTime = Some(new Timestamp(mrt.getEpochSecond, mrt.getNano)),
            commands = new Command(command) +: commands.map(new Command(_))
          ))))
    } yield a

  def submitAndWait(
      party: String,
      applicationId: String,
      command: Command.Command,
      commands: Command.Command*): Future[Unit] =
    submitAndWaitCommand(services.cmd.submitAndWait)(party, applicationId, command, commands: _*)
      .map(_ => ())

  def submitAndWaitForTransactionId(
      party: String,
      applicationId: String,
      command: Command.Command,
      commands: Command.Command*
  ): Future[String] =
    submitAndWaitCommand(services.cmd.submitAndWaitForTransactionId)(
      party,
      applicationId,
      command,
      commands: _*).map(_.transactionId)

  def submitAndWaitForTransaction[A](
      party: String,
      applicationId: String,
      command: Command.Command,
      commands: Command.Command*)(f: Transaction => A): Future[A] =
    submitAndWaitCommand(services.cmd.submitAndWaitForTransaction)(
      party,
      applicationId,
      command,
      commands: _*)
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

}
