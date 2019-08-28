// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.{Clock, Instant}

import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UploadDarFileRequest
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  GetParticipantIdRequest
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.{Command, Commands}
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
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
  GetTransactionByIdRequest,
  GetTransactionsRequest
}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.binding.Primitive.Party
import com.digitalasset.ledger.client.binding.{Primitive, Template}
import com.digitalasset.platform.testing.{FiniteStreamObserver, SingleItemObserver}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import scalaz.Tag
import scalaz.syntax.tag._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object LedgerTestContext {

  private[this] def filter(templateIds: Seq[Identifier]): Filters =
    new Filters(if (templateIds.isEmpty) None else Some(new InclusiveFilters(templateIds)))

  private def transactionFilter(
      parties: Seq[String],
      templateIds: Seq[Identifier]): Some[TransactionFilter] =
    Some(new TransactionFilter(Map(parties.map(_ -> filter(templateIds)): _*)))

  private def timestamp(i: Instant): Some[Timestamp] =
    Some(new Timestamp(i.getEpochSecond, i.getNano))

  private def timestampToInstant(t: Timestamp): Instant =
    Instant.EPOCH.plusSeconds(t.seconds).plusNanos(t.nanos.toLong)

  private def instantToTimestamp(t: Instant): Timestamp =
    new Timestamp(t.getEpochSecond, t.getNano)

  private val end = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  private val defaultTtlSeconds = 30

}

private[testtool] final class LedgerTestContext private[infrastructure] (
    val ledgerId: String,
    val applicationId: String,
    referenceOffset: LedgerOffset,
    services: LedgerServices,
    commandTtlFactor: Double)(implicit ec: ExecutionContext) {

  import LedgerTestContext._

  private[this] def timestampWithTtl(i: Instant): Some[Timestamp] =
    timestamp(i.plusSeconds(math.floor(defaultTtlSeconds * commandTtlFactor).toLong))

  private[this] val nextPartyHintId: () => String = {
    val it = Iterator.from(0).map(n => s"$applicationId-party-$n")
    () =>
      it.synchronized(it.next())
  }
  private[this] val nextCommandId: () => String = {
    val it = Iterator.from(0).map(n => s"$applicationId-command-$n")
    () =>
      it.synchronized(it.next())
  }

  def time(): Future[Instant] =
    SingleItemObserver
      .first[GetTimeResponse](services.time.getTime(new GetTimeRequest(ledgerId), _))
      .map(_.map(r => timestampToInstant(r.getCurrentTime)).get)
      .recover {
        case NonFatal(_) => Clock.systemUTC().instant()
      }

  def passTime(t: Duration): Future[Unit] =
    for {
      currentInstant <- time()
      currentTime = Some(instantToTimestamp(currentInstant))
      newTime = Some(instantToTimestamp(currentInstant.plusNanos(t.toNanos)))
      result <- services.time
        .setTime(new SetTimeRequest(ledgerId, currentTime, newTime))
        .map(_ => ())
    } yield result

  def listKnownPackages(): Future[Seq[PackageDetails]] =
    services.packageManagement.listKnownPackages(new ListKnownPackagesRequest).map(_.packageDetails)

  def uploadDarFile(bytes: ByteString): Future[Unit] =
    services.packageManagement
      .uploadDarFile(new UploadDarFileRequest(bytes))
      .map(_ => ())

  def participantId(): Future[String] =
    services.partyManagement.getParticipantId(new GetParticipantIdRequest).map(_.participantId)

  /**
    * Managed version of party allocation, should be used anywhere a party has
    * to be allocated unless the party management service itself is under test
    */
  def allocateParty(): Future[Party] =
    services.partyManagement
      .allocateParty(new AllocatePartyRequest(partyIdHint = nextPartyHintId()))
      .map(r => Party(r.partyDetails.get.party))

  /**
    * Non managed version of party allocation. Use exclusively when testing the party management service.
    */
  def allocateParty(partyHintId: Option[String], displayName: Option[String]): Future[Party] =
    services.partyManagement
      .allocateParty(
        new AllocatePartyRequest(
          partyIdHint = partyHintId.getOrElse(""),
          displayName = displayName.getOrElse("")))
      .map(r => Party(r.partyDetails.get.party))

  def allocateParties(n: Int): Future[Vector[Party]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  def activeContracts(
      request: GetActiveContractsRequest): Future[(Option[LedgerOffset], Vector[CreatedEvent])] =
    for {
      contracts <- FiniteStreamObserver[GetActiveContractsResponse](
        services.activeContracts.getActiveContracts(request, _)
      )
    } yield
      contracts.lastOption.map(c => LedgerOffset(LedgerOffset.Value.Absolute(c.offset))) -> contracts
        .flatMap(_.activeContracts)

  def activeContractsRequest(
      parties: Seq[Party],
      templateIds: Seq[Identifier] = Seq.empty,
  ): GetActiveContractsRequest =
    new GetActiveContractsRequest(
      ledgerId = ledgerId,
      filter = transactionFilter(Tag.unsubst(parties), templateIds),
      verbose = true
    )

  def activeContracts(parties: Party*): Future[Vector[CreatedEvent]] =
    activeContractsByTemplateId(Seq.empty, parties: _*)

  def activeContractsByTemplateId(
      templateIds: Seq[Identifier],
      parties: Party*): Future[Vector[CreatedEvent]] =
    activeContracts(activeContractsRequest(parties, templateIds)).map(_._2)

  private def getTransactionsRequest(
      parties: Seq[Party],
      beginOffset: Option[LedgerOffset] = None): GetTransactionsRequest =
    new GetTransactionsRequest(
      ledgerId = ledgerId,
      begin = beginOffset.orElse(Some(referenceOffset)),
      end = Some(end),
      filter = transactionFilter(Tag.unsubst(parties), Seq.empty),
      verbose = true
    )

  private def transactions[Res](
      parties: Seq[Party],
      service: (GetTransactionsRequest, StreamObserver[Res]) => Unit,
      beginOffset: Option[LedgerOffset] = None): Future[Vector[Res]] =
    FiniteStreamObserver[Res](service(getTransactionsRequest(parties, beginOffset), _))

  def flatTransactions(parties: Party*): Future[Vector[Transaction]] =
    transactions(parties, services.transaction.getTransactions).map(_.flatMap(_.transactions))

  def flatTransactionsFromOffset(
      offset: LedgerOffset,
      parties: Seq[Party]): Future[Vector[Transaction]] =
    transactions(parties, services.transaction.getTransactions, Some(offset))
      .map(_.flatMap(_.transactions))

  def transactionTrees(parties: Party*): Future[Vector[TransactionTree]] =
    transactions(parties, services.transaction.getTransactionTrees).map(_.flatMap(_.transactions))

  def transactionTreeById(transactionId: String, parties: Party*): Future[TransactionTree] =
    services.transaction
      .getTransactionById(
        new GetTransactionByIdRequest(ledgerId, transactionId, Tag.unsubst(parties)))
      .map(_.getTransaction)

  def create[T](
      party: Party,
      template: Template[T]
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitRequest(party, template.create.command)
      .flatMap(submitAndWaitForTransaction)
      .map(_.events.collect {
        case Event(Created(e)) => Primitive.ContractId(e.contractId)
      }.head)

  def createAndGetTransactionId[T](
      party: Party,
      template: Template[T]
  ): Future[(String, Primitive.ContractId[T])] =
    submitAndWaitRequest(party, template.create.command)
      .flatMap(submitAndWaitForTransaction)
      .map(tx =>
        tx.transactionId -> tx.events.collect {
          case Event(Created(e)) => Primitive.ContractId(e.contractId)
        }.head)

  def exercise[T](
      party: Party,
      exercise: Party => Primitive.Update[T]
  ): Future[TransactionTree] =
    submitAndWaitRequest(party, exercise(party).command).flatMap(submitAndWaitForTransactionTree)

  def submitAndWaitRequest(party: Party, commands: Command*): Future[SubmitAndWaitRequest] =
    time().map(
      let =>
        new SubmitAndWaitRequest(
          Some(new Commands(
            ledgerId = ledgerId,
            applicationId = applicationId,
            commandId = nextCommandId(),
            party = party.unwrap,
            ledgerEffectiveTime = timestamp(let),
            maximumRecordTime = timestampWithTtl(let),
            commands = commands
          ))))

  def submitAndWait(request: SubmitAndWaitRequest): Future[Unit] =
    services.command.submitAndWait(request).map(_ => ())

  def submitAndWaitForTransactionId(request: SubmitAndWaitRequest): Future[String] =
    services.command.submitAndWaitForTransactionId(request).map(_.transactionId)

  def submitAndWaitForTransaction(request: SubmitAndWaitRequest): Future[Transaction] =
    services.command.submitAndWaitForTransaction(request).map(_.getTransaction)

  def submitAndWaitForTransactionTree(request: SubmitAndWaitRequest): Future[TransactionTree] =
    services.command.submitAndWaitForTransactionTree(request).map(_.getTransaction)

}
