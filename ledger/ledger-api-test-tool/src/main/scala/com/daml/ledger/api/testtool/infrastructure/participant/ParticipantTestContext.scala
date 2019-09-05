// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.time.{Clock, Instant}

import com.daml.ledger.api.testtool.infrastructure.{
  Identification,
  LedgerServices,
  instantToTimestamp,
  timestampToInstant
}
import com.digitalasset.ledger.api.refinements.ApiTypes.TemplateId
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
  GetLedgerEndRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest
}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.binding.Primitive.Party
import com.digitalasset.ledger.client.binding.{Primitive, Template}
import com.digitalasset.platform.testing.{
  FiniteStreamObserver,
  SingleItemObserver,
  SizeBoundObserver
}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver
import scalaz.Tag
import scalaz.syntax.tag._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[testtool] object ParticipantTestContext {

  private[this] def filter(templateIds: Seq[Identifier]): Filters =
    new Filters(if (templateIds.isEmpty) None else Some(new InclusiveFilters(templateIds)))

  private def transactionFilter(
      parties: Seq[String],
      templateIds: Seq[Identifier]): Some[TransactionFilter] =
    Some(new TransactionFilter(Map(parties.map(_ -> filter(templateIds)): _*)))

  private def timestamp(i: Instant): Some[Timestamp] =
    Some(new Timestamp(i.getEpochSecond, i.getNano))

  private val defaultTtlSeconds = 30

}

private[testtool] final class ParticipantTestContext private[participant] (
    val ledgerId: String,
    val endpointId: String,
    val applicationId: String,
    val identifierSuffix: String,
    referenceOffset: LedgerOffset,
    services: LedgerServices,
    commandTtlFactor: Double)(implicit ec: ExecutionContext) {

  import ParticipantTestContext._

  val begin = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  /**
    * A reference to the moving ledger end. If you want a fixed reference to the offset at
    * a given point in time, use [[currentEnd]]
    */
  val end = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  private[this] def timestampWithTtl(i: Instant): Some[Timestamp] =
    timestamp(i.plusSeconds(math.floor(defaultTtlSeconds * commandTtlFactor).toLong))

  private[this] val identifierPrefix = s"$applicationId-$endpointId-$identifierSuffix"

  private[this] val nextPartyHintId: () => String =
    Identification.indexSuffix(s"$identifierPrefix-party")
  private[this] val nextCommandId: () => String =
    Identification.indexSuffix(s"$identifierPrefix-command")

  /**
    * Gets the absolute offset of the ledger end at a point in time. Use [[end]] if you need
    * a reference to the moving end of the ledger.
    */
  def currentEnd(): Future[LedgerOffset] =
    services.transaction.getLedgerEnd(new GetLedgerEndRequest(ledgerId)).map(_.getOffset)

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

  /**
    * Create a [[GetTransactionsRequest]] with a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[flatTransactions]]
    * or [[transactionTrees]], otherwise use the shortcut override that allows you to
    * directly pass a set of [[Party]]
    */
  def getTransactionsRequest(
      parties: Seq[Party],
      templateIds: Seq[TemplateId] = Seq.empty): GetTransactionsRequest =
    new GetTransactionsRequest(
      ledgerId = ledgerId,
      begin = Some(referenceOffset),
      end = Some(end),
      filter = transactionFilter(Tag.unsubst(parties), Tag.unsubst(templateIds)),
      verbose = true
    )

  private def transactions[Res](
      take: Int,
      request: GetTransactionsRequest,
      service: (GetTransactionsRequest, StreamObserver[Res]) => Unit): Future[Vector[Res]] =
    SizeBoundObserver[Res](take)(service(request, _))

  private def transactions[Res](
      request: GetTransactionsRequest,
      service: (GetTransactionsRequest, StreamObserver[Res]) => Unit): Future[Vector[Res]] =
    FiniteStreamObserver[Res](service(request, _))

  def flatTransactionsByTemplateId(
      templateId: TemplateId,
      parties: Party*): Future[Vector[Transaction]] =
    flatTransactions(getTransactionsRequest(parties, Seq(templateId)))

  /**
    * Non-managed version of [[flatTransactions]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(request: GetTransactionsRequest): Future[Vector[Transaction]] =
    transactions(request, services.transaction.getTransactions).map(_.flatMap(_.transactions))

  /**
    * Managed version of [[flatTransactions]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(parties: Party*): Future[Vector[Transaction]] =
    flatTransactions(getTransactionsRequest(parties))

  /**
    * Non-managed version of [[flatTransactions]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(take: Int, request: GetTransactionsRequest): Future[Vector[Transaction]] =
    transactions(take, request, services.transaction.getTransactions).map(_.flatMap(_.transactions))

  /**
    * Managed version of [[flatTransactions]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(take: Int, parties: Party*): Future[Vector[Transaction]] =
    flatTransactions(take, getTransactionsRequest(parties))

  /**
    * Non-managed version of [[transactionTrees]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(request: GetTransactionsRequest): Future[Vector[TransactionTree]] =
    transactions(request, services.transaction.getTransactionTrees).map(_.flatMap(_.transactions))

  /**
    * Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(parties: Party*): Future[Vector[TransactionTree]] =
    transactionTrees(getTransactionsRequest(parties))

  /**
    * Non-managed version of [[transactionTrees]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(
      take: Int,
      request: GetTransactionsRequest): Future[Vector[TransactionTree]] =
    transactions(take, request, services.transaction.getTransactionTrees)
      .map(_.flatMap(_.transactions))

  /**
    * Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(take: Int, parties: Party*): Future[Vector[TransactionTree]] =
    transactionTrees(take, getTransactionsRequest(parties))

  def transactionTreeById(transactionId: String, parties: Party*): Future[TransactionTree] =
    services.transaction
      .getTransactionById(
        new GetTransactionByIdRequest(ledgerId, transactionId, Tag.unsubst(parties)))
      .map(_.getTransaction)

  private def extractContracts[T](transaction: Transaction): Seq[Primitive.ContractId[T]] =
    transaction.events.collect {
      case Event(Created(e)) => Primitive.ContractId(e.contractId)
    }

  def create[T](
      party: Party,
      template: Template[T],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitRequest(party, template.create.command)
      .flatMap(submitAndWaitForTransaction)
      .map(extractContracts)
      .map(_.head)

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

  def exerciseAndGetContract[T](
      party: Party,
      exercise: Party => Primitive.Update[_]
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitRequest(party, exercise(party).command)
      .flatMap(submitAndWaitForTransaction)
      .map(extractContracts)
      .map(_.head.asInstanceOf[Primitive.ContractId[T]])

  def exerciseAndGetContracts[T](
      party: Party,
      exercise: Party => Primitive.Update[T]
  ): Future[Seq[Primitive.ContractId[_]]] =
    submitAndWaitRequest(party, exercise(party).command)
      .flatMap(submitAndWaitForTransaction)
      .map(extractContracts)

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
