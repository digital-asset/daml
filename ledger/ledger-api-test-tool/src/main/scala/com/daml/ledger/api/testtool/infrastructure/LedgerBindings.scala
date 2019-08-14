// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.{Clock, Instant}

import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.AllocatePartyRequest
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.{Command, Commands}
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
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
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.ledger.client.binding.Primitive.Party
import com.digitalasset.ledger.client.binding.{Contract, Primitive, Template, ValueDecoder}
import com.digitalasset.platform.testing.{FiniteStreamObserver, SingleItemObserver}
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import scalaz.Tag
import scalaz.syntax.tag._

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

private[infrastructure] final class LedgerBindings(
    services: LedgerServices,
    commandTtlFactor: Double)(implicit ec: ExecutionContext) {

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
      t <- SingleItemObserver
        .first[GetTimeResponse](services.time.getTime(new GetTimeRequest(id), _))
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

  def allocateParty(partyIdHint: String): Future[Party] =
    services.partyManagement
      .allocateParty(new AllocatePartyRequest(partyIdHint = partyIdHint))
      .map(r => Party(r.partyDetails.get.party))

  def ledgerEnd: Future[LedgerOffset] =
    for {
      id <- ledgerId
      response <- services.transaction.getLedgerEnd(new GetLedgerEndRequest(id))
    } yield response.offset.get

  def activeContracts(
      parties: Seq[Party],
      templateIds: Seq[Identifier]): Future[Vector[CreatedEvent]] =
    for {
      id <- ledgerId
      contracts <- FiniteStreamObserver[GetActiveContractsResponse](
        services.activeContracts.getActiveContracts(
          new GetActiveContractsRequest(
            ledgerId = id,
            filter = Some(LedgerBindings.transactionFilter(Tag.unsubst(parties), templateIds)),
            verbose = true
          ),
          _
        )
      )
    } yield contracts.flatMap(_.activeContracts)

  private def transactions[Res, Tx](service: (GetTransactionsRequest, StreamObserver[Res]) => Unit)(
      extract: Res => Seq[Tx])(
      begin: LedgerOffset,
      parties: Seq[Party],
      templateIds: Seq[Identifier]): Future[Vector[Tx]] =
    for {
      id <- ledgerId
      txs <- FiniteStreamObserver[Res](
        service(
          new GetTransactionsRequest(
            ledgerId = id,
            begin = Some(begin),
            end = Some(LedgerBindings.end),
            filter = Some(LedgerBindings.transactionFilter(Tag.unsubst(parties), templateIds)),
            verbose = true
          ),
          _
        ))
    } yield txs.flatMap(extract)

  def flatTransactions(
      begin: LedgerOffset,
      parties: Seq[Party],
      templateIds: Seq[Identifier]): Future[Vector[Transaction]] =
    transactions(services.transaction.getTransactions)(_.transactions)(begin, parties, templateIds)

  def transactionTrees(
      begin: LedgerOffset,
      parties: Seq[Party],
      templateIds: Seq[Identifier]): Future[Vector[TransactionTree]] =
    transactions(services.transaction.getTransactionTrees)(_.transactions)(
      begin,
      parties,
      templateIds)

  def getTransactionById(transactionId: String, parties: Seq[Party]): Future[TransactionTree] =
    for {
      id <- ledgerId
      transaction <- services.transaction.getTransactionById(
        new GetTransactionByIdRequest(id, transactionId, Tag.unsubst(parties)))
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
      party: Party,
      applicationId: String,
      commandId: String,
      template: Template[T]
  ): Future[Contract[T]] =
    for {
      request <- prepareSubmission(party, applicationId, commandId, Seq(template.create.command))
      response <- submitAndWaitForTransaction(request).map(_.events.collect {
        case Event(Created(e)) => decodeCreated(e).get
      }.head)
    } yield response

  def createAndGetTransactionId[T <: Template[T]: ValueDecoder](
      party: Party,
      applicationId: String,
      commandId: String,
      template: Template[T]
  ): Future[(String, Contract[T])] =
    for {
      request <- prepareSubmission(party, applicationId, commandId, Seq(template.create.command))
      response <- submitAndWaitForTransaction(request).map(tx =>
        tx.transactionId -> tx.events.collect {
          case Event(Created(e)) => decodeCreated(e).get
        }.head)
    } yield response

  def exercise[T](
      party: Party,
      applicationId: String,
      commandId: String,
      exercise: Party => Primitive.Update[T]
  ): Future[TransactionTree] =
    for {
      request <- prepareSubmission(party, applicationId, commandId, Seq(exercise(party).command))
      response <- submitAndWaitForTransactionTree(request)
    } yield response

  def prepareSubmission(
      party: Party,
      applicationId: String,
      commandId: String,
      commands: Seq[Command]): Future[SubmitAndWaitRequest] =
    for {
      id <- ledgerId
      let <- time
      mrt = let.plusSeconds(math.floor(30 * commandTtlFactor).toLong)
    } yield
      new SubmitAndWaitRequest(
        Some(
          new Commands(
            ledgerId = id,
            applicationId = applicationId,
            commandId = commandId,
            party = party.unwrap,
            ledgerEffectiveTime = Some(new Timestamp(let.getEpochSecond, let.getNano)),
            maximumRecordTime = Some(new Timestamp(mrt.getEpochSecond, mrt.getNano)),
            commands = commands
          )))

  def submitAndWait(request: SubmitAndWaitRequest): Future[Unit] =
    services.command.submitAndWait(request).map(_ => ())

  def submitAndWaitForTransactionId(request: SubmitAndWaitRequest): Future[String] =
    services.command.submitAndWaitForTransactionId(request).map(_.transactionId)

  def submitAndWaitForTransaction(request: SubmitAndWaitRequest): Future[Transaction] =
    services.command.submitAndWaitForTransaction(request).map(_.transaction.get)

  def submitAndWaitForTransactionTree(request: SubmitAndWaitRequest): Future[TransactionTree] =
    services.command.submitAndWaitForTransactionTree(request).map(_.transaction.get)

}
