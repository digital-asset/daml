// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.{Clock, Instant}
import java.util.UUID

import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.AllocatePartyRequest
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest
}
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest
}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.client.binding.{Contract, Primitive, Template, ValueDecoder}
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Channel
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object LedgerBindings {

  private def filter(templateIds: Seq[Identifier]): Filters =
    new Filters(if (templateIds.isEmpty) None else Some(new InclusiveFilters(templateIds)))

  private def transactionFilter(
      parties: Seq[String],
      templateIds: Seq[Identifier]): TransactionFilter = {
    val templateIdFilter = filter(templateIds)
    new TransactionFilter(Map(parties.map(_ -> templateIdFilter): _*))
  }

  private val end = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

}

final class LedgerBindings(channel: Channel, commandTtlFactor: Double)(
    implicit ec: ExecutionContext) {

  private[this] val services = new LedgerServices(channel)

  val ledgerId: Future[String] =
    for {
      response <- services.identity.getLedgerIdentity(new GetLedgerIdentityRequest)
    } yield response.ledgerId

  private def timestampToInstant(t: Timestamp): Instant =
    Instant.EPOCH.plusSeconds(t.seconds).plusNanos(t.nanos.toLong)

  private def instantToTimestamp(t: Instant): Timestamp =
    new Timestamp(t.getEpochSecond, t.getNano)

  def time: Future[Instant] =
    for {
      id <- ledgerId
      t <- HeadObserver[GetTimeResponse](services.time.getTime(new GetTimeRequest(id), _))
        .map(_.map(r => timestampToInstant(r.currentTime.get)))
        .recover {
          case NonFatal(_) => Some(Clock.systemUTC().instant())
        }
    } yield t.get

  def passTime(t: Duration): Future[Unit] =
    for {
      id <- ledgerId
      currentInstant <- time
      currentTime = Some(instantToTimestamp(currentInstant))
      newTime = Some(instantToTimestamp(currentInstant.plusNanos(t.toNanos)))
      result <- services.time.setTime(new SetTimeRequest(id, currentTime, newTime)).map(_ => ())
    } yield result

  def allocateParty(): Future[String] =
    services.partyManagement.allocateParty(new AllocatePartyRequest()).map(_.partyDetails.get.party)

  def ledgerEnd: Future[LedgerOffset] =
    for {
      id <- ledgerId
      response <- services.transaction.getLedgerEnd(new GetLedgerEndRequest(id))
    } yield response.offset.get

  def activeContracts(
      parties: Seq[String],
      templateIds: Seq[Identifier]): Future[Vector[CreatedEvent]] =
    for {
      id <- ledgerId
      contracts <- FiniteStreamObserver[GetActiveContractsResponse](
        services.activeContracts.getActiveContracts(
          new GetActiveContractsRequest(
            ledgerId = id,
            filter = Some(LedgerBindings.transactionFilter(parties, templateIds)),
            verbose = true
          ),
          _
        )
      )
    } yield contracts.flatMap(_.activeContracts)

  private def transactions[Res, Tx](service: (GetTransactionsRequest, StreamObserver[Res]) => Unit)(
      extract: Res => Seq[Tx])(
      begin: LedgerOffset,
      parties: Seq[String],
      templateIds: Seq[Identifier]): Future[Vector[Tx]] =
    for {
      id <- ledgerId
      txs <- FiniteStreamObserver[Res](
        service(
          new GetTransactionsRequest(
            ledgerId = id,
            begin = Some(begin),
            end = Some(LedgerBindings.end),
            filter = Some(LedgerBindings.transactionFilter(parties, templateIds)),
            verbose = true
          ),
          _
        ))
    } yield txs.flatMap(extract)

  def flatTransactions(
      begin: LedgerOffset,
      parties: Seq[String],
      templateIds: Seq[Identifier]): Future[Vector[Transaction]] =
    transactions(services.transaction.getTransactions)(_.transactions)(begin, parties, templateIds)

  def transactionTrees(
      begin: LedgerOffset,
      parties: Seq[String],
      templateIds: Seq[Identifier]): Future[Vector[TransactionTree]] =
    transactions(services.transaction.getTransactionTrees)(_.transactions)(
      begin,
      parties,
      templateIds)

  def getTransactionById(transactionId: String, parties: Seq[String]): Future[TransactionTree] =
    for {
      id <- ledgerId
      transaction <- services.transaction.getTransactionById(
        new GetTransactionByIdRequest(id, transactionId, parties))
    } yield transaction.transaction.get

  private def decodeCreated[T <: Template[T]](event: CreatedEvent)(
      implicit decoder: ValueDecoder[T]): Option[Contract[T]] =
    for {
      record <- event.createArguments
      a <- decoder.read(Value.Sum.Record(record))
    } yield
      Contract(
        Primitive.ContractId(event.contractId),
        a,
        event.agreementText,
        event.signatories,
        event.observers,
        event.contractKey)

  def create[T <: Template[T]: ValueDecoder](
      party: String,
      applicationId: String,
      template: Template[T]
  ): Future[Contract[T]] = {
    submitAndWaitForTransaction(party, applicationId, template.create.command.command) {
      _.events.collect {
        case Event(Created(e)) => decodeCreated(e).get
      }.head
    }
  }

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

  def exercise[T](
      party: String,
      applicationId: String,
      exercise: Primitive.Update[T]
  ): Future[Unit] =
    submitAndWait(party, applicationId, exercise.command.command)

  private def submitAndWaitCommand[A](service: SubmitAndWaitRequest => Future[A])(
      party: String,
      applicationId: String,
      command: Command.Command,
      commands: Command.Command*): Future[A] =
    for {
      id <- ledgerId
      let <- time
      mrt = let.plusSeconds(math.floor(30 * commandTtlFactor).toLong)
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
    submitAndWaitCommand(services.command.submitAndWait)(
      party,
      applicationId,
      command,
      commands: _*)
      .map(_ => ())

  def submitAndWaitForTransactionId(
      party: String,
      applicationId: String,
      command: Command.Command,
      commands: Command.Command*
  ): Future[String] =
    submitAndWaitCommand(services.command.submitAndWaitForTransactionId)(
      party,
      applicationId,
      command,
      commands: _*).map(_.transactionId)

  def submitAndWaitForTransaction[A](
      party: String,
      applicationId: String,
      command: Command.Command,
      commands: Command.Command*)(f: Transaction => A): Future[A] =
    submitAndWaitCommand(services.command.submitAndWaitForTransaction)(
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
