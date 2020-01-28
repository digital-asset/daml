// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.time.{Clock, Duration, Instant}

import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.{Identification, LedgerServices}
import com.digitalasset.ledger.api.refinements.ApiTypes.TemplateId
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import com.digitalasset.ledger.api.v1.admin.config_management_service.{
  GetTimeModelRequest,
  GetTimeModelResponse,
  SetTimeModelRequest,
  SetTimeModelResponse,
  TimeModel,
}
import com.digitalasset.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UploadDarFileRequest,
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  GetParticipantIdRequest,
  ListKnownPartiesRequest,
}
import com.digitalasset.ledger.api.v1.command_completion_service.{
  Checkpoint,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, ExerciseByKeyCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfiguration,
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.package_service._
import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest,
}
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter,
}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
}
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.ledger.client.binding.Primitive.Party
import com.digitalasset.ledger.client.binding.{Primitive, Template}
import com.digitalasset.platform.testing.StreamConsumer
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
    new Filters(if (templateIds.isEmpty) None else Some(new InclusiveFilters(templateIds)))

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
    ttl: Duration,
)(implicit ec: ExecutionContext) {

  import ParticipantTestContext._

  val begin = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  /**
    * A reference to the moving ledger end. If you want a fixed reference to the offset at
    * a given point in time, use [[currentEnd]]
    */
  val end = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  private[this] val identifierPrefix = s"$applicationId-$endpointId-$identifierSuffix"

  private[this] val nextPartyHintId: () => String =
    Identification.indexSuffix(s"$identifierPrefix-party")
  private[this] val nextCommandId: () => String =
    Identification.indexSuffix(s"$identifierPrefix-command")
  private[this] val nextSubmissionId: () => String =
    Identification.indexSuffix(s"$identifierPrefix-submission")

  /**
    * Gets the absolute offset of the ledger end at a point in time. Use [[end]] if you need
    * a reference to the moving end of the ledger.
    */
  def currentEnd(): Future[LedgerOffset] =
    services.transaction.getLedgerEnd(new GetLedgerEndRequest(ledgerId)).map(_.getOffset)

  /**
    * Works just like [[currentEnd]] but allows to override the ledger identifier.
    *
    * Used only for low-level testing. Please use the other method unless you want to test the
    * behavior of the ledger end endpoint with a wrong ledger identifier.
    */
  def currentEnd(overrideLedgerId: String): Future[LedgerOffset] =
    services.transaction.getLedgerEnd(new GetLedgerEndRequest(overrideLedgerId)).map(_.getOffset)

  def time(): Future[Instant] =
    new StreamConsumer[GetTimeResponse](services.time.getTime(new GetTimeRequest(ledgerId), _))
      .first()
      .map(_.map(r => r.getCurrentTime.asJava).get)
      .recover {
        case NonFatal(_) => Clock.systemUTC().instant()
      }

  def passTime(t: Duration): Future[Unit] =
    for {
      currentInstant <- time()
      currentTime = Some(currentInstant.asProtobuf)
      newTime = Some(currentInstant.plus(t).asProtobuf)
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

  def listPackages(): Future[Seq[String]] =
    services.packages.listPackages(new ListPackagesRequest(ledgerId)).map(_.packageIds)

  def getPackage(packageId: String): Future[GetPackageResponse] =
    services.packages.getPackage(new GetPackageRequest(ledgerId, packageId))

  def getPackageStatus(packageId: String): Future[PackageStatus] =
    services.packages
      .getPackageStatus(new GetPackageStatusRequest(ledgerId, packageId))
      .map(_.packageStatus)

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
          displayName = displayName.getOrElse(""),
        ),
      )
      .map(r => Party(r.partyDetails.get.party))

  def allocateParties(n: Int): Future[Vector[Party]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  def listParties(): Future[Set[Party]] =
    services.partyManagement
      .listKnownParties(new ListKnownPartiesRequest())
      .map(_.partyDetails.map(partyDetails => Party(partyDetails.party)).toSet)

  def waitForParties(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
  ): Future[Unit] = eventually {
    val participants = otherParticipants.toSet + this
    Future
      .sequence(participants.map(otherParticipant => {
        otherParticipant
          .listParties()
          .map(actualParties => {
            assert(
              expectedParties.subsetOf(actualParties),
              s"Parties from $this never appeared on $otherParticipant.",
            )
          })
      }))
      .map(_ => ())
  }

  def activeContracts(
      request: GetActiveContractsRequest,
  ): Future[(Option[LedgerOffset], Vector[CreatedEvent])] =
    for {
      contracts <- new StreamConsumer[GetActiveContractsResponse](
        services.activeContracts.getActiveContracts(request, _),
      ).all()
    } yield contracts.lastOption.map(c => LedgerOffset(LedgerOffset.Value.Absolute(c.offset))) -> contracts
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
      parties: Party*,
  ): Future[Vector[CreatedEvent]] =
    activeContracts(activeContractsRequest(parties, templateIds)).map(_._2)

  /**
    * Create a [[GetTransactionsRequest]] with a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[flatTransactions]]
    * or [[transactionTrees]], otherwise use the shortcut override that allows you to
    * directly pass a set of [[Party]]
    */
  def getTransactionsRequest(
      parties: Seq[Party],
      templateIds: Seq[TemplateId] = Seq.empty,
  ): GetTransactionsRequest =
    new GetTransactionsRequest(
      ledgerId = ledgerId,
      begin = Some(referenceOffset),
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

  def flatTransactionsByTemplateId(
      templateId: TemplateId,
      parties: Party*,
  ): Future[Vector[Transaction]] =
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

  def transactionTreesByTemplateId(
      templateId: TemplateId,
      parties: Party*,
  ): Future[Vector[TransactionTree]] =
    transactionTrees(getTransactionsRequest(parties, Seq(templateId)))

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
      request: GetTransactionsRequest,
  ): Future[Vector[TransactionTree]] =
    transactions(take, request, services.transaction.getTransactionTrees)
      .map(_.flatMap(_.transactions))

  /**
    * Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(take: Int, parties: Party*): Future[Vector[TransactionTree]] =
    transactionTrees(take, getTransactionsRequest(parties))

  /**
    * Create a [[GetTransactionByIdRequest]] with an identifier and a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[transactionTreeById]] or
    * [[flatTransactionById]], otherwise use the shortcut override that allows you to directly
    * pass the identifier and parties.
    */
  def getTransactionByIdRequest(
      transactionId: String,
      parties: Seq[Party],
  ): GetTransactionByIdRequest =
    new GetTransactionByIdRequest(ledgerId, transactionId, Tag.unsubst(parties))

  /**
    * Non-managed version of [[transactionTreeById]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeById(request: GetTransactionByIdRequest): Future[TransactionTree] =
    services.transaction.getTransactionById(request).map(_.getTransaction)

  /**
    * Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeById(transactionId: String, parties: Party*): Future[TransactionTree] =
    transactionTreeById(getTransactionByIdRequest(transactionId, parties))

  /**
    * Non-managed version of [[flatTransactionById]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionById(request: GetTransactionByIdRequest): Future[Transaction] =
    services.transaction.getFlatTransactionById(request).map(_.getTransaction)

  /**
    * Managed version of [[flatTransactionById]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionById(transactionId: String, parties: Party*): Future[Transaction] =
    flatTransactionById(getTransactionByIdRequest(transactionId, parties))

  /**
    * Create a [[GetTransactionByEventIdRequest]] with an identifier and a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[transactionTreeByEventId]] or
    * [[flatTransactionByEventId]], otherwise use the shortcut override that allows you to directly
    * pass the identifier and parties.
    */
  def getTransactionByEventIdRequest(
      eventId: String,
      parties: Seq[Party],
  ): GetTransactionByEventIdRequest =
    new GetTransactionByEventIdRequest(ledgerId, eventId, Tag.unsubst(parties))

  /**
    * Non-managed version of [[transactionTreeByEventId]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeByEventId(request: GetTransactionByEventIdRequest): Future[TransactionTree] =
    services.transaction.getTransactionByEventId(request).map(_.getTransaction)

  /**
    * Managed version of [[transactionTreeByEventId]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeByEventId(eventId: String, parties: Party*): Future[TransactionTree] =
    transactionTreeByEventId(getTransactionByEventIdRequest(eventId, parties))

  /**
    * Non-managed version of [[flatTransactionByEventId]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionByEventId(request: GetTransactionByEventIdRequest): Future[Transaction] =
    services.transaction.getFlatTransactionByEventId(request).map(_.getTransaction)

  /**
    * Managed version of [[flatTransactionByEventId]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionByEventId(eventId: String, parties: Party*): Future[Transaction] =
    flatTransactionByEventId(getTransactionByEventIdRequest(eventId, parties))

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
      template: Template[T],
  ): Future[(String, Primitive.ContractId[T])] =
    submitAndWaitRequest(party, template.create.command)
      .flatMap(submitAndWaitForTransaction)
      .map(tx =>
        tx.transactionId -> tx.events.collect {
          case Event(Created(e)) => Primitive.ContractId(e.contractId)
        }.head,
      )

  def exercise[T](
      party: Party,
      exercise: Party => Primitive.Update[T],
  ): Future[TransactionTree] =
    submitAndWaitRequest(party, exercise(party).command).flatMap(submitAndWaitForTransactionTree)

  def exerciseForFlatTransaction[T](
      party: Party,
      exercise: Party => Primitive.Update[T],
  ): Future[Transaction] =
    submitAndWaitRequest(party, exercise(party).command).flatMap(submitAndWaitForTransaction)

  def exerciseAndGetContract[T](
      party: Party,
      exercise: Party => Primitive.Update[_],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitRequest(party, exercise(party).command)
      .flatMap(submitAndWaitForTransaction)
      .map(extractContracts)
      .map(_.head.asInstanceOf[Primitive.ContractId[T]])

  def exerciseAndGetContracts[T](
      party: Party,
      exercise: Party => Primitive.Update[T],
  ): Future[Seq[Primitive.ContractId[_]]] =
    submitAndWaitRequest(party, exercise(party).command)
      .flatMap(submitAndWaitForTransaction)
      .map(extractContracts)

  def exerciseByKey[T](
      party: Party,
      template: Primitive.TemplateId[T],
      key: Value,
      choice: String,
      argument: Value,
  ): Future[TransactionTree] =
    submitAndWaitRequest(
      party,
      Command(
        Command.Command.ExerciseByKey(
          ExerciseByKeyCommand(
            Some(template.unwrap),
            Option(key),
            choice,
            Option(argument),
          ),
        ),
      ),
    ).flatMap(submitAndWaitForTransactionTree)

  def submitRequest(party: Party, commands: Command*): Future[SubmitRequest] =
    time().map(let =>
      new SubmitRequest(
        Some(
          new Commands(
            ledgerId = ledgerId,
            applicationId = applicationId,
            commandId = nextCommandId(),
            party = party.unwrap,
            ledgerEffectiveTime = Some(let.asProtobuf),
            maximumRecordTime = Some(let.plus(ttl).asProtobuf),
            commands = commands,
          ),
        ),
      ),
    )

  def submitAndWaitRequest(party: Party, commands: Command*): Future[SubmitAndWaitRequest] =
    time().map(let =>
      new SubmitAndWaitRequest(
        Some(
          new Commands(
            ledgerId = ledgerId,
            applicationId = applicationId,
            commandId = nextCommandId(),
            party = party.unwrap,
            ledgerEffectiveTime = Some(let.asProtobuf),
            maximumRecordTime = Some(let.plus(ttl).asProtobuf),
            commands = commands,
          ),
        ),
      ),
    )

  def submit(request: SubmitRequest): Future[Unit] =
    services.commandSubmission.submit(request).map(_ => ())

  def submitAndWait(request: SubmitAndWaitRequest): Future[Unit] =
    services.command.submitAndWait(request).map(_ => ())

  def submitAndWaitForTransactionId(request: SubmitAndWaitRequest): Future[String] =
    services.command.submitAndWaitForTransactionId(request).map(_.transactionId)

  def submitAndWaitForTransaction(request: SubmitAndWaitRequest): Future[Transaction] =
    services.command.submitAndWaitForTransaction(request).map(_.getTransaction)

  def submitAndWaitForTransactionTree(request: SubmitAndWaitRequest): Future[TransactionTree] =
    services.command.submitAndWaitForTransactionTree(request).map(_.getTransaction)

  def completionStreamRequest(from: LedgerOffset = referenceOffset)(parties: Party*) =
    new CompletionStreamRequest(ledgerId, applicationId, parties.map(_.unwrap), Some(from))

  def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _),
    ).find(_.completions.nonEmpty)
      .map(_.fold(Seq.empty[Completion])(_.completions).toVector)

  def firstCompletions(parties: Party*): Future[Vector[Completion]] =
    firstCompletions(completionStreamRequest()(parties: _*))

  def findCompletion(
      request: CompletionStreamRequest,
  )(p: Completion => Boolean): Future[Option[Completion]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _),
    ).find(_.completions.exists(p))
      .map(_.flatMap(_.completions.find(p)))

  def findCompletion(parties: Party*)(p: Completion => Boolean): Future[Option[Completion]] =
    findCompletion(completionStreamRequest()(parties: _*))(p)

  def checkpoints(n: Int, request: CompletionStreamRequest): Future[Vector[Checkpoint]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _),
    ).filterTake(_.checkpoint.isDefined)(n)
      .map(_.map(_.getCheckpoint))

  def checkpoints(n: Int, from: LedgerOffset = referenceOffset)(
      parties: Party*,
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
        ),
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
    services.configManagement.setTimeModel(
      SetTimeModelRequest(nextSubmissionId(), Some(mrt.asProtobuf), generation, Some(newTimeModel)),
    )
}
