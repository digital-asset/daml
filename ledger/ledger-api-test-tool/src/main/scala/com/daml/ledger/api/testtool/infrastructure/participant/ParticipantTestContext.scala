// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.time.{Clock, Instant}

import com.daml.ledger.api.refinements.ApiTypes.TemplateId
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.{
  Endpoint,
  Identification,
  LedgerServices,
  PartyAllocationConfiguration,
}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import com.daml.ledger.api.v1.admin.config_management_service.{
  GetTimeModelRequest,
  GetTimeModelResponse,
  SetTimeModelRequest,
  SetTimeModelResponse,
  TimeModel,
}
import com.daml.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UploadDarFileRequest,
}
import com.daml.ledger.api.v1.admin.participant_pruning_service.{PruneRequest, PruneResponse}
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  GetParticipantIdRequest,
  GetPartiesRequest,
  ListKnownPartiesRequest,
  PartyDetails,
}
import com.daml.ledger.api.v1.command_completion_service.{
  Checkpoint,
  CompletionEndRequest,
  CompletionEndResponse,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, ExerciseByKeyCommand}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.event.{CreatedEvent, Event}
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfiguration,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.package_service._
import com.daml.ledger.api.v1.testing.time_service.{GetTimeRequest, GetTimeResponse, SetTimeRequest}
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
  GetTransactionsResponse,
}
import com.daml.ledger.api.v1.value.{Identifier, Value}
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.client.binding.{Primitive, Template}
import com.daml.platform.testing.StreamConsumer
import com.google.protobuf.ByteString
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse}
import io.grpc.stub.StreamObserver
import scalaz.Tag
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[testtool] object ParticipantTestContext {

  private[this] def filter(templateIds: Seq[Identifier]): Filters =
    new Filters(
      if (templateIds.isEmpty) None
      else Some(new InclusiveFilters(templateIds))
    )

  private def transactionFilter(
      parties: Seq[String],
      templateIds: Seq[Identifier],
  ): Some[TransactionFilter] =
    Some(new TransactionFilter(Map(parties.map(_ -> filter(templateIds)): _*)))

}

private[testtool] final class ParticipantTestContext private[participant] (
    val ledgerId: String,
    val endpointId: String,
    val applicationId: String,
    val identifierSuffix: String,
    referenceOffset: LedgerOffset,
    services: LedgerServices,
    partyAllocation: PartyAllocationConfiguration,
    val ledgerEndpoint: Endpoint,
    val clientTlsConfiguration: Option[TlsConfiguration],
    val features: Features,
)(implicit ec: ExecutionContext) {

  import ParticipantTestContext._

  val begin: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  /** A reference to the moving ledger end. If you want a fixed reference to the offset at
    * a given point in time, use [[currentEnd]]
    */
  val end: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  private[this] val identifierPrefix =
    s"$applicationId-$endpointId-$identifierSuffix"
  private[this] def nextId(idType: String): () => String =
    Identification.indexSuffix(s"$identifierPrefix-$idType")

  private[this] val nextPartyHintId: () => String = nextId("party")
  private[this] val nextCommandId: () => String = nextId("command")
  private[this] val nextSubmissionId: () => String = nextId("submission")
  private[this] val workflowId: String = s"$applicationId-$identifierSuffix"
  val nextKeyId: () => String = nextId("key")

  override def toString: String = s"participant $endpointId"

  /** Gets the absolute offset of the ledger end at a point in time. Use [[end]] if you need
    * a reference to the moving end of the ledger.
    */
  def currentEnd(): Future[LedgerOffset] =
    services.transaction
      .getLedgerEnd(new GetLedgerEndRequest(ledgerId))
      .map(_.getOffset)

  /** Works just like [[currentEnd]] but allows to override the ledger identifier.
    *
    * Used only for low-level testing. Please use the other method unless you want to test the
    * behavior of the ledger end endpoint with a wrong ledger identifier.
    */
  def currentEnd(overrideLedgerId: String): Future[LedgerOffset] =
    services.transaction
      .getLedgerEnd(new GetLedgerEndRequest(overrideLedgerId))
      .map(_.getOffset)

  /** Returns an absolute offset that is beyond the current ledger end.
    *
    * Note: offsets are opaque byte strings, but they are lexicographically sortable.
    * Prepending the current absolute ledger end with non-zero bytes creates an offset that
    * is be beyond the current ledger end for the ledger API server.
    * The offset might however not be valid for the underlying ledger.
    * This method can therefore only be used for offsets that are only interpreted by the
    * ledger API server and not sent to the ledger.
    */
  def offsetBeyondLedgerEnd(): Future[LedgerOffset] =
    currentEnd().map(end => LedgerOffset(LedgerOffset.Value.Absolute("FFFF" + end.getAbsolute)))

  def time(): Future[Instant] =
    new StreamConsumer[GetTimeResponse](services.time.getTime(new GetTimeRequest(ledgerId), _))
      .first()
      .map(_.map(r => r.getCurrentTime.asJava).get)
      .recover { case NonFatal(_) =>
        Clock.systemUTC().instant()
      }

  def setTime(currentTime: Instant, newTime: Instant): Future[Unit] =
    services.time
      .setTime(
        SetTimeRequest(
          ledgerId = ledgerId,
          currentTime = Some(currentTime.asProtobuf),
          newTime = Some(newTime.asProtobuf),
        )
      )
      .map(_ => ())

  def listKnownPackages(): Future[Seq[PackageDetails]] =
    services.packageManagement
      .listKnownPackages(new ListKnownPackagesRequest)
      .map(_.packageDetails)

  def uploadDarFile(bytes: ByteString): Future[Unit] =
    uploadDarFile(new UploadDarFileRequest(bytes))

  def uploadDarRequest(bytes: ByteString): UploadDarFileRequest =
    new UploadDarFileRequest(bytes, nextSubmissionId())

  def uploadDarFile(request: UploadDarFileRequest): Future[Unit] =
    services.packageManagement
      .uploadDarFile(request)
      .map(_ => ())

  def participantId(): Future[String] =
    services.partyManagement
      .getParticipantId(new GetParticipantIdRequest)
      .map(_.participantId)

  def listPackages(): Future[Seq[String]] =
    services.packages
      .listPackages(new ListPackagesRequest(ledgerId))
      .map(_.packageIds)

  def getPackage(packageId: String): Future[GetPackageResponse] =
    services.packages.getPackage(new GetPackageRequest(ledgerId, packageId))

  def getPackageStatus(packageId: String): Future[PackageStatus] =
    services.packages
      .getPackageStatus(new GetPackageStatusRequest(ledgerId, packageId))
      .map(_.packageStatus)

  /** Managed version of party allocation, should be used anywhere a party has
    * to be allocated unless the party management service itself is under test
    */
  def allocateParty(): Future[Party] =
    services.partyManagement
      .allocateParty(new AllocatePartyRequest(partyIdHint = nextPartyHintId()))
      .map(r => Party(r.partyDetails.get.party))

  /** Non managed version of party allocation. Use exclusively when testing the party management service.
    */
  def allocateParty(partyIdHint: Option[String], displayName: Option[String]): Future[Party] =
    services.partyManagement
      .allocateParty(
        new AllocatePartyRequest(
          partyIdHint = partyIdHint.getOrElse(""),
          displayName = displayName.getOrElse(""),
        )
      )
      .map(r => Party(r.partyDetails.get.party))

  def allocateParties(n: Int): Future[Vector[Party]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  def getParties(parties: Seq[Party]): Future[Seq[PartyDetails]] =
    services.partyManagement
      .getParties(GetPartiesRequest(parties.map(_.unwrap)))
      .map(_.partyDetails)

  def listKnownParties(): Future[Set[Party]] =
    services.partyManagement
      .listKnownParties(new ListKnownPartiesRequest())
      .map(_.partyDetails.map(partyDetails => Party(partyDetails.party)).toSet)

  def waitForParties(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
  ): Future[Unit] =
    if (partyAllocation.waitForAllParticipants) {
      eventually {
        val participants = otherParticipants.toSet + this
        Future
          .sequence(participants.map(otherParticipant => {
            otherParticipant
              .listKnownParties()
              .map { actualParties =>
                assert(
                  expectedParties.subsetOf(actualParties),
                  s"Parties from $this never appeared on $otherParticipant.",
                )
              }
          }))
          .map(_ => ())
      }
    } else {
      Future.unit
    }

  def activeContracts(
      request: GetActiveContractsRequest
  ): Future[(Option[LedgerOffset], Vector[CreatedEvent])] =
    for {
      contracts <- new StreamConsumer[GetActiveContractsResponse](
        services.activeContracts.getActiveContracts(request, _)
      ).all()
    } yield contracts.lastOption
      .map(c => LedgerOffset(LedgerOffset.Value.Absolute(c.offset))) -> contracts
      .flatMap(_.activeContracts)

  def activeContractsRequest(
      parties: Seq[Party],
      templateIds: Seq[Identifier] = Seq.empty,
  ): GetActiveContractsRequest =
    new GetActiveContractsRequest(
      ledgerId = ledgerId,
      filter = transactionFilter(Tag.unsubst(parties), templateIds),
      verbose = true,
    )

  def activeContracts(parties: Party*): Future[Vector[CreatedEvent]] =
    activeContractsByTemplateId(Seq.empty, parties: _*)

  def activeContractsByTemplateId(
      templateIds: Seq[Identifier],
      parties: Party*
  ): Future[Vector[CreatedEvent]] =
    activeContracts(activeContractsRequest(parties, templateIds)).map(_._2)

  /** Create a [[GetTransactionsRequest]] with a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[flatTransactions]]
    * or [[transactionTrees]], otherwise use the shortcut override that allows you to
    * directly pass a set of [[Party]]
    */
  def getTransactionsRequest(
      parties: Seq[Party],
      templateIds: Seq[TemplateId] = Seq.empty,
      begin: LedgerOffset = referenceOffset,
  ): GetTransactionsRequest =
    new GetTransactionsRequest(
      ledgerId = ledgerId,
      begin = Some(begin),
      end = Some(end),
      filter = transactionFilter(Tag.unsubst(parties), Tag.unsubst(templateIds)),
      verbose = true,
    )

  private def transactions[Res](
      n: Int,
      request: GetTransactionsRequest,
      service: (GetTransactionsRequest, StreamObserver[Res]) => Unit,
  ): Future[Vector[Res]] =
    new StreamConsumer[Res](service(request, _)).take(n)

  private def transactions[Res](
      request: GetTransactionsRequest,
      service: (GetTransactionsRequest, StreamObserver[Res]) => Unit,
  ): Future[Vector[Res]] =
    new StreamConsumer[Res](service(request, _)).all()

  def transactionStream(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit =
    services.transaction.getTransactions(request, responseObserver)

  def flatTransactionsByTemplateId(
      templateId: TemplateId,
      parties: Party*
  ): Future[Vector[Transaction]] =
    flatTransactions(getTransactionsRequest(parties, Seq(templateId)))

  /** Non-managed version of [[flatTransactions]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(request: GetTransactionsRequest): Future[Vector[Transaction]] =
    transactions(request, services.transaction.getTransactions)
      .map(_.flatMap(_.transactions))

  /** Managed version of [[flatTransactions]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(parties: Party*): Future[Vector[Transaction]] =
    flatTransactions(getTransactionsRequest(parties))

  /** Non-managed version of [[flatTransactions]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(take: Int, request: GetTransactionsRequest): Future[Vector[Transaction]] =
    transactions(take, request, services.transaction.getTransactions)
      .map(_.flatMap(_.transactions))

  /** Managed version of [[flatTransactions]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(take: Int, parties: Party*): Future[Vector[Transaction]] =
    flatTransactions(take, getTransactionsRequest(parties))

  def transactionTreesByTemplateId(
      templateId: TemplateId,
      parties: Party*
  ): Future[Vector[TransactionTree]] =
    transactionTrees(getTransactionsRequest(parties, Seq(templateId)))

  /** Non-managed version of [[transactionTrees]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(request: GetTransactionsRequest): Future[Vector[TransactionTree]] =
    transactions(request, services.transaction.getTransactionTrees)
      .map(_.flatMap(_.transactions))

  /** Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(parties: Party*): Future[Vector[TransactionTree]] =
    transactionTrees(getTransactionsRequest(parties))

  /** Non-managed version of [[transactionTrees]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(
      take: Int,
      request: GetTransactionsRequest,
  ): Future[Vector[TransactionTree]] =
    transactions(take, request, services.transaction.getTransactionTrees)
      .map(_.flatMap(_.transactions))

  /** Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(take: Int, parties: Party*): Future[Vector[TransactionTree]] =
    transactionTrees(take, getTransactionsRequest(parties))

  /** Create a [[GetTransactionByIdRequest]] with an identifier and a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[transactionTreeById]] or
    * [[flatTransactionById]], otherwise use the shortcut override that allows you to directly
    * pass the identifier and parties.
    */
  def getTransactionByIdRequest(
      transactionId: String,
      parties: Seq[Party],
  ): GetTransactionByIdRequest =
    new GetTransactionByIdRequest(ledgerId, transactionId, Tag.unsubst(parties))

  /** Non-managed version of [[transactionTreeById]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeById(request: GetTransactionByIdRequest): Future[TransactionTree] =
    services.transaction.getTransactionById(request).map(_.getTransaction)

  /** Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeById(transactionId: String, parties: Party*): Future[TransactionTree] =
    transactionTreeById(getTransactionByIdRequest(transactionId, parties))

  /** Non-managed version of [[flatTransactionById]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionById(request: GetTransactionByIdRequest): Future[Transaction] =
    services.transaction.getFlatTransactionById(request).map(_.getTransaction)

  /** Managed version of [[flatTransactionById]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionById(transactionId: String, parties: Party*): Future[Transaction] =
    flatTransactionById(getTransactionByIdRequest(transactionId, parties))

  /** Create a [[GetTransactionByEventIdRequest]] with an identifier and a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[transactionTreeByEventId]] or
    * [[flatTransactionByEventId]], otherwise use the shortcut override that allows you to directly
    * pass the identifier and parties.
    */
  def getTransactionByEventIdRequest(
      eventId: String,
      parties: Seq[Party],
  ): GetTransactionByEventIdRequest =
    new GetTransactionByEventIdRequest(ledgerId, eventId, Tag.unsubst(parties))

  /** Non-managed version of [[transactionTreeByEventId]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeByEventId(request: GetTransactionByEventIdRequest): Future[TransactionTree] =
    services.transaction.getTransactionByEventId(request).map(_.getTransaction)

  /** Managed version of [[transactionTreeByEventId]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeByEventId(eventId: String, parties: Party*): Future[TransactionTree] =
    transactionTreeByEventId(getTransactionByEventIdRequest(eventId, parties))

  /** Non-managed version of [[flatTransactionByEventId]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionByEventId(request: GetTransactionByEventIdRequest): Future[Transaction] =
    services.transaction
      .getFlatTransactionByEventId(request)
      .map(_.getTransaction)

  /** Managed version of [[flatTransactionByEventId]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionByEventId(eventId: String, parties: Party*): Future[Transaction] =
    flatTransactionByEventId(getTransactionByEventIdRequest(eventId, parties))

  private def extractContracts[T](transaction: Transaction): Seq[Primitive.ContractId[T]] =
    transaction.events.collect { case Event(Created(e)) =>
      Primitive.ContractId(e.contractId)
    }

  def create[T](
      party: Party,
      template: Template[T],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(party, template.create.command)
    )
      .map(response => extractContracts(response.getTransaction).head)

  def create[T](
      actAs: List[Party],
      readAs: List[Party],
      template: Template[T],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(actAs, readAs, template.create.command)
    ).map(response => extractContracts(response.getTransaction).head)

  def createAndGetTransactionId[T](
      party: Party,
      template: Template[T],
  ): Future[(String, Primitive.ContractId[T])] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(party, template.create.command)
    )
      .map(_.getTransaction)
      .map(tx =>
        tx.transactionId -> tx.events.collect { case Event(Created(e)) =>
          Primitive.ContractId(e.contractId)
        }.head
      )

  def exercise[T](
      party: Party,
      exercise: Party => Primitive.Update[T],
  ): Future[TransactionTree] =
    submitAndWaitForTransactionTree(
      submitAndWaitRequest(party, exercise(party).command)
    ).map(_.getTransaction)

  def exercise[T](
      actAs: List[Party],
      readAs: List[Party],
      exercise: => Primitive.Update[T],
  ): Future[TransactionTree] =
    submitAndWaitForTransactionTree(
      submitAndWaitRequest(actAs, readAs, exercise.command)
    ).map(_.getTransaction)

  def exerciseForFlatTransaction[T](
      party: Party,
      exercise: Party => Primitive.Update[T],
  ): Future[Transaction] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(party, exercise(party).command)
    ).map(_.getTransaction)

  def exerciseAndGetContract[T](
      party: Party,
      exercise: Party => Primitive.Update[Any],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(party, exercise(party).command)
    )
      .map(_.getTransaction)
      .map(extractContracts)
      .map(_.head.asInstanceOf[Primitive.ContractId[T]])

  def exerciseByKey[T](
      party: Party,
      template: Primitive.TemplateId[T],
      key: Value,
      choice: String,
      argument: Value,
  ): Future[TransactionTree] =
    submitAndWaitForTransactionTree(
      submitAndWaitRequest(
        party,
        Command.of(
          Command.Command.ExerciseByKey(
            ExerciseByKeyCommand(
              Some(template.unwrap),
              Option(key),
              choice,
              Option(argument),
            )
          )
        ),
      )
    ).map(_.getTransaction)

  def submitRequest(actAs: List[Party], readAs: List[Party], commands: Command*): SubmitRequest =
    new SubmitRequest(
      Some(
        new Commands(
          ledgerId = ledgerId,
          applicationId = applicationId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = Party.unsubst(actAs),
          readAs = Party.unsubst(readAs),
          commands = commands,
          workflowId = workflowId,
        )
      )
    )

  def submitRequest(party: Party, commands: Command*): SubmitRequest =
    new SubmitRequest(
      Some(
        new Commands(
          ledgerId = ledgerId,
          applicationId = applicationId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          party = party.unwrap,
          commands = commands,
          workflowId = workflowId,
        )
      )
    )

  def submitAndWaitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: Command*
  ): SubmitAndWaitRequest =
    new SubmitAndWaitRequest(
      Some(
        new Commands(
          ledgerId = ledgerId,
          applicationId = applicationId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = Party.unsubst(actAs),
          readAs = Party.unsubst(readAs),
          commands = commands,
          workflowId = workflowId,
        )
      )
    )

  def submitAndWaitRequest(party: Party, commands: Command*): SubmitAndWaitRequest =
    new SubmitAndWaitRequest(
      Some(
        new Commands(
          ledgerId = ledgerId,
          applicationId = applicationId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          party = party.unwrap,
          commands = commands,
          workflowId = workflowId,
        )
      )
    )

  def submit(request: SubmitRequest): Future[Unit] =
    services.commandSubmission.submit(request).map(_ => ())

  def submitAndWait(request: SubmitAndWaitRequest): Future[Unit] =
    services.command.submitAndWait(request).map(_ => ())

  def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    services.command.submitAndWaitForTransactionId(request)

  def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    services.command.submitAndWaitForTransaction(request)

  def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    services.command
      .submitAndWaitForTransactionTree(request)

  def completionStreamRequest(from: LedgerOffset = referenceOffset)(parties: Party*) =
    new CompletionStreamRequest(ledgerId, applicationId, parties.map(_.unwrap), Some(from))

  def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    services.commandCompletion.completionEnd(request)

  def completionStream(
      request: CompletionStreamRequest,
      streamObserver: StreamObserver[CompletionStreamResponse],
  ): Unit =
    services.commandCompletion.completionStream(request, streamObserver)

  def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).find(_.completions.nonEmpty)
      .map(_.completions.toVector)

  def firstCompletions(parties: Party*): Future[Vector[Completion]] =
    firstCompletions(completionStreamRequest()(parties: _*))

  def findCompletion(
      request: CompletionStreamRequest
  )(p: Completion => Boolean): Future[Option[Completion]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).find(_.completions.exists(p))
      .map(_.completions.find(p))

  def findCompletion(parties: Party*)(p: Completion => Boolean): Future[Option[Completion]] =
    findCompletion(completionStreamRequest()(parties: _*))(p)

  def checkpoints(n: Int, request: CompletionStreamRequest): Future[Vector[Checkpoint]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).filterTake(_.checkpoint.isDefined)(n)
      .map(_.map(_.getCheckpoint))

  def checkpoints(n: Int, from: LedgerOffset = referenceOffset)(
      parties: Party*
  ): Future[Vector[Checkpoint]] =
    checkpoints(n, completionStreamRequest(from)(parties: _*))

  def firstCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] =
    checkpoints(1, request).map(_.head)

  def firstCheckpoint(parties: Party*): Future[Checkpoint] =
    firstCheckpoint(completionStreamRequest()(parties: _*))

  def nextCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] =
    checkpoints(1, request).map(_.head)

  def nextCheckpoint(from: LedgerOffset, parties: Party*): Future[Checkpoint] =
    nextCheckpoint(completionStreamRequest(from)(parties: _*))

  def configuration(overrideLedgerId: Option[String] = None): Future[LedgerConfiguration] =
    new StreamConsumer[GetLedgerConfigurationResponse](
      services.configuration
        .getLedgerConfiguration(
          new GetLedgerConfigurationRequest(overrideLedgerId.getOrElse(ledgerId)),
          _,
        )
    ).first()
      .map(_.fold(sys.error("No ledger configuration available."))(_.getLedgerConfiguration))

  def checkHealth(): Future[HealthCheckResponse] =
    services.health.check(HealthCheckRequest())

  def watchHealth(): Future[Seq[HealthCheckResponse]] =
    new StreamConsumer[HealthCheckResponse](services.health.watch(HealthCheckRequest(), _))
      .within(1.second)

  def getTimeModel(): Future[GetTimeModelResponse] =
    services.configManagement.getTimeModel(GetTimeModelRequest())

  def setTimeModel(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): Future[SetTimeModelResponse] =
    setTimeModel(setTimeModelRequest(mrt, generation, newTimeModel))

  def setTimeModelRequest(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): SetTimeModelRequest =
    SetTimeModelRequest(nextSubmissionId(), Some(mrt.asProtobuf), generation, Some(newTimeModel))

  def setTimeModel(
      request: SetTimeModelRequest
  ): Future[SetTimeModelResponse] =
    services.configManagement.setTimeModel(request)

  def prune(
      pruneUpTo: String,
      attempts: Int,
      pruneAllDivulgedContracts: Boolean,
  ): Future[PruneResponse] =
    // Distributed ledger participants need to reach global consensus prior to pruning. Hence the "eventually" here:
    eventually(
      attempts = attempts,
      runAssertion = {
        services.participantPruning.prune(
          PruneRequest(pruneUpTo, nextSubmissionId(), pruneAllDivulgedContracts)
        )
      },
    )

  def prune(
      pruneUpTo: LedgerOffset,
      attempts: Int = 10,
      pruneAllDivulgedContracts: Boolean = false,
  ): Future[PruneResponse] =
    prune(pruneUpTo.getAbsolute, attempts, pruneAllDivulgedContracts)

  private[infrastructure] def preallocateParties(
      n: Int,
      participantsUnderTest: Iterable[ParticipantTestContext],
  ): Future[Vector[Party]] =
    for {
      parties <-
        if (partyAllocation.allocateParties) {
          allocateParties(n)
        } else {
          reservePartyNames(n)
        }
      _ <- waitForParties(participantsUnderTest, parties.toSet)
    } yield parties

  private def reservePartyNames(n: Int): Future[Vector[Party]] =
    Future.successful(Vector.fill(n)(Party(nextPartyHintId())))
}
