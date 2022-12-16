// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.error.ErrorCode

import java.time.{Clock, Instant}
import com.daml.ledger.api.refinements.ApiTypes.TemplateId
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.{
  CompletionResponse,
  IncludeInterfaceView,
}
import com.daml.ledger.api.testtool.infrastructure.time.{
  DelayMechanism,
  StaticTimeDelayMechanism,
  TimeDelayMechanism,
}
import com.daml.ledger.api.testtool.infrastructure.{
  Endpoint,
  Identification,
  LedgerServices,
  PartyAllocationConfiguration,
}
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
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UploadDarFileRequest,
}
import com.daml.ledger.api.v1.admin.participant_pruning_service.{PruneRequest, PruneResponse}
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  AllocatePartyResponse,
  GetParticipantIdRequest,
  GetPartiesRequest,
  GetPartiesResponse,
  ListKnownPartiesRequest,
  ListKnownPartiesResponse,
  PartyDetails,
  UpdatePartyDetailsRequest,
  UpdatePartyDetailsResponse,
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
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
  GetEventsByContractKeyRequest,
  GetEventsByContractKeyResponse,
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
  GetTransactionsResponse,
}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.client.binding.{Primitive, Template}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.participant.util.HexOffset
import com.daml.platform.testing.StreamConsumer
import com.daml.timer.Delayed
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse}
import io.grpc.protobuf.StatusProto
import io.grpc.stub.StreamObserver
import scalaz.Tag
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** Exposes services running on some participant server in a test case.
  *
  * Each time a test case is run it receives a fresh instance of [[SingleParticipantTestContext]]
  * (one for every used participant server).
  */
final class SingleParticipantTestContext private[participant] (
    val ledgerId: String,
    val endpointId: String,
    val applicationId: String,
    identifierSuffix: String,
    val referenceOffset: LedgerOffset,
    protected[participant] val services: LedgerServices,
    partyAllocationConfig: PartyAllocationConfiguration,
    val ledgerEndpoint: Endpoint,
    val features: Features,
)(protected[participant] implicit val ec: ExecutionContext)
    extends ParticipantTestContext {
  private val logger = ContextualizedLogger.get(getClass)

  private[this] val identifierPrefix =
    s"$applicationId-$endpointId-$identifierSuffix"

  private[this] def nextIdGenerator(name: String, lowerCase: Boolean = false): () => String = {
    val f = Identification.indexSuffix(s"$identifierPrefix-$name")
    if (lowerCase)
      () => f().toLowerCase
    else
      f
  }

  private[this] val nextPartyHintId: () => String = nextIdGenerator("party")
  private[this] val nextCommandId: () => String = nextIdGenerator("command")
  private[this] val nextSubmissionId: () => String = nextIdGenerator("submission")
  private[this] val workflowId: String = s"$applicationId-$identifierSuffix"
  override val nextKeyId: () => String = nextIdGenerator("key")
  override val nextUserId: () => String = nextIdGenerator("user", lowerCase = true)
  override val nextPartyId: () => String = nextIdGenerator("party", lowerCase = true)

  override lazy val delayMechanism: DelayMechanism = if (features.staticTime) {
    new StaticTimeDelayMechanism(this)
  } else
    new TimeDelayMechanism()

  override def toString: String = s"participant $endpointId"

  override def currentEnd(): Future[LedgerOffset] =
    services.transaction
      .getLedgerEnd(new GetLedgerEndRequest(ledgerId))
      .map(_.getOffset)

  override def currentEnd(overrideLedgerId: String): Future[LedgerOffset] =
    services.transaction
      .getLedgerEnd(new GetLedgerEndRequest(overrideLedgerId))
      .map(_.getOffset)

  override def offsetBeyondLedgerEnd(): Future[LedgerOffset] =
    currentEnd().map(end => LedgerOffset(LedgerOffset.Value.Absolute("ffff" + end.getAbsolute)))

  override def time(): Future[Instant] =
    new StreamConsumer[GetTimeResponse](services.time.getTime(new GetTimeRequest(ledgerId), _))
      .first()
      .map(_.map(r => r.getCurrentTime.asJava).get)
      .recover { case NonFatal(_) =>
        Clock.systemUTC().instant()
      }

  override def setTime(currentTime: Instant, newTime: Instant): Future[Unit] =
    services.time
      .setTime(
        SetTimeRequest(
          ledgerId = ledgerId,
          currentTime = Some(currentTime.asProtobuf),
          newTime = Some(newTime.asProtobuf),
        )
      )
      .map(_ => ())

  override def listKnownPackages(): Future[Seq[PackageDetails]] =
    services.packageManagement
      .listKnownPackages(new ListKnownPackagesRequest)
      .map(_.packageDetails)

  override def uploadDarRequest(bytes: ByteString): UploadDarFileRequest =
    new UploadDarFileRequest(bytes, nextSubmissionId())

  override def uploadDarFile(request: UploadDarFileRequest): Future[Unit] =
    services.packageManagement
      .uploadDarFile(request)
      .map(_ => ())

  override def participantId(): Future[String] =
    services.partyManagement
      .getParticipantId(new GetParticipantIdRequest)
      .map(_.participantId)

  override def listPackages(): Future[Seq[String]] =
    services.packages
      .listPackages(new ListPackagesRequest(ledgerId))
      .map(_.packageIds)

  override def getPackage(packageId: String): Future[GetPackageResponse] =
    services.packages.getPackage(new GetPackageRequest(ledgerId, packageId))

  override def getPackageStatus(packageId: String): Future[PackageStatus] =
    services.packages
      .getPackageStatus(new GetPackageStatusRequest(ledgerId, packageId))
      .map(_.packageStatus)

  override def allocateParty(): Future[Party] =
    services.partyManagement
      .allocateParty(new AllocatePartyRequest(partyIdHint = nextPartyHintId()))
      .map(r => Party(r.partyDetails.get.party))

  override def allocateParty(
      partyIdHint: Option[String] = None,
      displayName: Option[String] = None,
      localMetadata: Option[ObjectMeta] = None,
      identityProviderId: Option[String] = None,
  ): Future[Party] =
    services.partyManagement
      .allocateParty(
        new AllocatePartyRequest(
          partyIdHint = partyIdHint.getOrElse(""),
          displayName = displayName.getOrElse(""),
          localMetadata = localMetadata,
          identityProviderId = identityProviderId.getOrElse(""),
        )
      )
      .map(r => Party(r.partyDetails.get.party))

  override def allocateParty(req: AllocatePartyRequest): Future[AllocatePartyResponse] =
    services.partyManagement
      .allocateParty(req)

  override def updatePartyDetails(
      req: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] = {
    services.partyManagement.updatePartyDetails(req)
  }

  override def allocateParties(n: Int): Future[Vector[Party]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  override def getParties(req: GetPartiesRequest): Future[GetPartiesResponse] =
    services.partyManagement.getParties(req)

  override def getParties(parties: Seq[Party]): Future[Seq[PartyDetails]] =
    services.partyManagement
      .getParties(GetPartiesRequest(parties.map(_.unwrap)))
      .map(_.partyDetails)

  override def listKnownParties(): Future[Set[Party]] =
    services.partyManagement
      .listKnownParties(new ListKnownPartiesRequest())
      .map(_.partyDetails.map(partyDetails => Party(partyDetails.party)).toSet)

  override def listKnownPartiesResp(): Future[ListKnownPartiesResponse] =
    services.partyManagement
      .listKnownParties(new ListKnownPartiesRequest())

  override def waitForParties(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
  ): Future[Unit] =
    if (partyAllocationConfig.waitForAllParticipants) {
      eventually("Wait for parties") {
        val participants = otherParticipants.toSet + this
        Future
          .sequence(participants.map(participant => {
            participant
              .listKnownParties()
              .map { actualParties =>
                assert(
                  expectedParties.subsetOf(actualParties),
                  s"Parties from $this never appeared on $participant.",
                )
              }
          }))
          .map(_ => ())
      }
    } else {
      Future.unit
    }

  override def activeContracts(
      request: GetActiveContractsRequest
  ): Future[(Option[LedgerOffset], Vector[CreatedEvent])] =
    for {
      contracts <- new StreamConsumer[GetActiveContractsResponse](
        services.activeContracts.getActiveContracts(request, _)
      ).all()
    } yield contracts.lastOption
      .map(c => LedgerOffset(LedgerOffset.Value.Absolute(c.offset))) -> contracts
      .flatMap(_.activeContracts)

  override def activeContractsRequest(
      parties: Seq[Party],
      templateIds: Seq[TemplateId] = Seq.empty,
      interfaceFilters: Seq[(TemplateId, IncludeInterfaceView)] = Seq.empty,
      activeAtOffset: String = "",
  ): GetActiveContractsRequest =
    new GetActiveContractsRequest(
      ledgerId = ledgerId,
      filter = Some(transactionFilter(parties, templateIds, interfaceFilters)),
      verbose = true,
      activeAtOffset,
    )

  override def activeContracts(parties: Party*): Future[Vector[CreatedEvent]] =
    activeContractsByTemplateId(Seq.empty, parties: _*)

  override def activeContractsByTemplateId(
      templateIds: Seq[TemplateId],
      parties: Party*
  ): Future[Vector[CreatedEvent]] =
    activeContracts(activeContractsRequest(parties, templateIds)).map(_._2)

  def transactionFilter(
      parties: Seq[Party],
      templateIds: Seq[TemplateId] = Seq.empty,
      interfaceFilters: Seq[(TemplateId, IncludeInterfaceView)] = Seq.empty,
  ): TransactionFilter =
    new TransactionFilter(
      parties.map(party => party.unwrap -> filters(templateIds, interfaceFilters)).toMap
    )

  def filters(
      templateIds: Seq[TemplateId] = Seq.empty,
      interfaceFilters: Seq[(TemplateId, IncludeInterfaceView)] = Seq.empty,
  ): Filters = new Filters(
    if (templateIds.isEmpty && interfaceFilters.isEmpty) None
    else
      Some(
        new InclusiveFilters(
          templateIds = templateIds.map(Tag.unwrap).toSeq,
          interfaceFilters = interfaceFilters.map { case (id, includeInterfaceView) =>
            new InterfaceFilter(
              Some(Tag.unwrap(id)),
              includeInterfaceView = includeInterfaceView,
            )
          }.toSeq,
        )
      )
  )

  def getTransactionsRequest(
      transactionFilter: TransactionFilter,
      begin: LedgerOffset = referenceOffset,
  ): GetTransactionsRequest = new GetTransactionsRequest(
    ledgerId = ledgerId,
    begin = Some(begin),
    end = Some(end),
    filter = Some(transactionFilter),
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

  override def transactionStream(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit =
    services.transaction.getTransactions(request, responseObserver)

  override def flatTransactionsByTemplateId(
      templateId: TemplateId,
      parties: Party*
  ): Future[Vector[Transaction]] =
    flatTransactions(getTransactionsRequest(transactionFilter(parties, Seq(templateId))))

  override def flatTransactions(request: GetTransactionsRequest): Future[Vector[Transaction]] =
    transactions(request, services.transaction.getTransactions)
      .map(_.flatMap(_.transactions))

  override def flatTransactions(parties: Party*): Future[Vector[Transaction]] =
    flatTransactions(getTransactionsRequest(transactionFilter(parties)))

  override def flatTransactions(
      take: Int,
      request: GetTransactionsRequest,
  ): Future[Vector[Transaction]] =
    transactions(take, request, services.transaction.getTransactions)
      .map(_.flatMap(_.transactions))

  override def flatTransactions(take: Int, parties: Party*): Future[Vector[Transaction]] =
    flatTransactions(take, getTransactionsRequest(transactionFilter(parties)))

  override def transactionTreesByTemplateId(
      templateId: TemplateId,
      parties: Party*
  ): Future[Vector[TransactionTree]] =
    transactionTrees(getTransactionsRequest(transactionFilter(parties, Seq(templateId))))

  override def transactionTrees(request: GetTransactionsRequest): Future[Vector[TransactionTree]] =
    transactions(request, services.transaction.getTransactionTrees)
      .map(_.flatMap(_.transactions))

  override def transactionTrees(parties: Party*): Future[Vector[TransactionTree]] =
    transactionTrees(getTransactionsRequest(transactionFilter(parties)))

  override def transactionTrees(
      take: Int,
      request: GetTransactionsRequest,
  ): Future[Vector[TransactionTree]] =
    transactions(take, request, services.transaction.getTransactionTrees)
      .map(_.flatMap(_.transactions))

  override def transactionTrees(take: Int, parties: Party*): Future[Vector[TransactionTree]] =
    transactionTrees(take, getTransactionsRequest(transactionFilter(parties)))

  override def getTransactionByIdRequest(
      transactionId: String,
      parties: Seq[Party],
  ): GetTransactionByIdRequest =
    new GetTransactionByIdRequest(ledgerId, transactionId, Tag.unsubst(parties))

  override def transactionTreeById(request: GetTransactionByIdRequest): Future[TransactionTree] =
    services.transaction.getTransactionById(request).map(_.getTransaction)

  override def transactionTreeById(
      transactionId: String,
      parties: Party*
  ): Future[TransactionTree] =
    transactionTreeById(getTransactionByIdRequest(transactionId, parties))

  override def flatTransactionById(request: GetTransactionByIdRequest): Future[Transaction] =
    services.transaction.getFlatTransactionById(request).map(_.getTransaction)

  override def flatTransactionById(transactionId: String, parties: Party*): Future[Transaction] =
    flatTransactionById(getTransactionByIdRequest(transactionId, parties))

  override def getTransactionByEventIdRequest(
      eventId: String,
      parties: Seq[Party],
  ): GetTransactionByEventIdRequest =
    new GetTransactionByEventIdRequest(ledgerId, eventId, Tag.unsubst(parties))

  override def transactionTreeByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[TransactionTree] =
    services.transaction.getTransactionByEventId(request).map(_.getTransaction)

  override def transactionTreeByEventId(eventId: String, parties: Party*): Future[TransactionTree] =
    transactionTreeByEventId(getTransactionByEventIdRequest(eventId, parties))

  override def flatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[Transaction] =
    services.transaction
      .getFlatTransactionByEventId(request)
      .map(_.getTransaction)

  override def flatTransactionByEventId(eventId: String, parties: Party*): Future[Transaction] =
    flatTransactionByEventId(getTransactionByEventIdRequest(eventId, parties))

  private def extractContracts[T](transaction: Transaction): Seq[Primitive.ContractId[T]] =
    transaction.events.collect { case Event(Created(e)) =>
      Primitive.ContractId(e.contractId)
    }

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] =
    services.transaction.getEventsByContractId(request)

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  ): Future[GetEventsByContractKeyResponse] =
    services.transaction.getEventsByContractKey(request)

  override def create[T](
      party: Party,
      template: Template[T],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(party, template.create.command)
    )
      .map(response => extractContracts(response.getTransaction).head)

  override def create[T](
      actAs: List[Party],
      readAs: List[Party],
      template: Template[T],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(actAs, readAs, template.create.command)
    ).map(response => extractContracts(response.getTransaction).head)

  override def createAndGetTransactionId[T](
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

  override def exercise[T](
      party: Party,
      exercise: Primitive.Update[T],
  ): Future[TransactionTree] =
    submitAndWaitForTransactionTree(
      submitAndWaitRequest(party, exercise.command)
    ).map(_.getTransaction)

  override def exercise[T](
      actAs: List[Party],
      readAs: List[Party],
      exercise: Primitive.Update[T],
  ): Future[TransactionTree] =
    submitAndWaitForTransactionTree(
      submitAndWaitRequest(actAs, readAs, exercise.command)
    ).map(_.getTransaction)

  override def exerciseForFlatTransaction[T](
      party: Party,
      exercise: Primitive.Update[T],
  ): Future[Transaction] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(party, exercise.command)
    ).map(_.getTransaction)

  override def exerciseAndGetContract[T](
      party: Party,
      exercise: Primitive.Update[Any],
  ): Future[Primitive.ContractId[T]] =
    submitAndWaitForTransaction(
      submitAndWaitRequest(party, exercise.command)
    )
      .map(_.getTransaction)
      .map(extractContracts)
      .map(_.head.asInstanceOf[Primitive.ContractId[T]])

  override def exerciseByKey[T](
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

  override def submitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: Command*
  ): SubmitRequest =
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

  override def submitRequest(party: Party, commands: Command*): SubmitRequest =
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

  override def submitAndWaitRequest(
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

  override def submitAndWaitRequest(party: Party, commands: Command*): SubmitAndWaitRequest =
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

  override def submit(request: SubmitRequest): Future[Unit] =
    services.commandSubmission.submit(request).map(_ => ())

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Unit] =
    services.command.submitAndWait(request).map(_ => ())

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    services.command.submitAndWaitForTransactionId(request)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    services.command.submitAndWaitForTransaction(request)

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    services.command
      .submitAndWaitForTransactionTree(request)

  /** This addresses a narrow case in which we tolerate a
    * single occurrence of a specific and transient (and rare) error
    * by retrying only a single time.
    */
  override def submitRequestAndTolerateGrpcError[T](
      errorCodeToTolerateOnce: ErrorCode,
      submitAndWaitGeneric: ParticipantTestContext => Future[T],
  ): Future[T] =
    submitAndWaitGeneric(this)
      .transform {
        case Failure(e: StatusRuntimeException)
            if errorCodeToTolerateOnce.category.grpcCode
              .map(_.value())
              .contains(StatusProto.fromThrowable(e).getCode) =>
          Success(Left(e))
        case otherTry =>
          // Otherwise return a Right with a nested Either that
          // let's us create a failed or successful future in the
          // default case of the step below.
          Success(Right(otherTry.toEither))
      }
      .flatMap {
        case Left(_) => // If we are retrying a single time, back off first for one second.
          Delayed.Future.by(1.second)(submitAndWaitGeneric(this))
        case Right(firstCallResult) => firstCallResult.fold(Future.failed, Future.successful)
      }

  override def completionStreamRequest(from: LedgerOffset = referenceOffset)(
      parties: Party*
  ): CompletionStreamRequest =
    new CompletionStreamRequest(ledgerId, applicationId, parties.map(_.unwrap), Some(from))

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    services.commandCompletion.completionEnd(request)

  override def completionStream(
      request: CompletionStreamRequest,
      streamObserver: StreamObserver[CompletionStreamResponse],
  ): Unit =
    services.commandCompletion.completionStream(request, streamObserver)

  override def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).find(_.completions.nonEmpty)
      .map(_.completions.toVector)

  override def firstCompletions(parties: Party*): Future[Vector[Completion]] =
    firstCompletions(completionStreamRequest()(parties: _*))

  override def findCompletionAtOffset(
      offset: Ref.HexString,
      p: Completion => Boolean,
  )(parties: Party*): Future[Option[CompletionResponse]] = {
    // We have to request an offset before the reported offset, as offsets are exclusive in the completion service.
    val offsetPreviousToReportedOffset = HexOffset
      .previous(offset)
      .map(offset => LedgerOffset.of(LedgerOffset.Value.Absolute(offset)))
      .getOrElse(referenceOffset)
    val reportedOffsetCompletionStreamRequest =
      completionStreamRequest(offsetPreviousToReportedOffset)(parties: _*)
    findCompletion(reportedOffsetCompletionStreamRequest)(p)
  }

  override def findCompletion(
      request: CompletionStreamRequest
  )(p: Completion => Boolean): Future[Option[CompletionResponse]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).find(_.completions.exists(p))
      .map(response => {
        val checkpoint = response.getCheckpoint
        response.completions
          .find(p)
          .map(CompletionResponse(_, checkpoint.getOffset, checkpoint.getRecordTime.asJava))
      })

  override def findCompletion(parties: Party*)(
      p: Completion => Boolean
  ): Future[Option[CompletionResponse]] =
    findCompletion(completionStreamRequest()(parties: _*))(p)

  override def checkpoints(n: Int, request: CompletionStreamRequest): Future[Vector[Checkpoint]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).filterTake(_.checkpoint.isDefined)(n)
      .map(_.map(_.getCheckpoint))

  override def checkpoints(n: Int, from: LedgerOffset)(
      parties: Party*
  ): Future[Vector[Checkpoint]] =
    checkpoints(n, completionStreamRequest(from)(parties: _*))

  override def firstCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] =
    checkpoints(1, request).map(_.head)

  override def firstCheckpoint(parties: Party*): Future[Checkpoint] =
    firstCheckpoint(completionStreamRequest()(parties: _*))

  override def nextCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] =
    checkpoints(1, request).map(_.head)

  override def nextCheckpoint(from: LedgerOffset, parties: Party*): Future[Checkpoint] =
    nextCheckpoint(completionStreamRequest(from)(parties: _*))

  override def configuration(overrideLedgerId: Option[String] = None): Future[LedgerConfiguration] =
    new StreamConsumer[GetLedgerConfigurationResponse](
      services.configuration
        .getLedgerConfiguration(
          new GetLedgerConfigurationRequest(overrideLedgerId.getOrElse(ledgerId)),
          _,
        )
    ).first()
      .map(_.fold(sys.error("No ledger configuration available."))(_.getLedgerConfiguration))

  override def checkHealth(): Future[HealthCheckResponse] =
    services.health.check(HealthCheckRequest())

  override def watchHealth(): Future[Seq[HealthCheckResponse]] =
    new StreamConsumer[HealthCheckResponse](services.health.watch(HealthCheckRequest(), _))
      .within(1.second)

  override def getTimeModel(): Future[GetTimeModelResponse] =
    services.configManagement.getTimeModel(GetTimeModelRequest())

  override def setTimeModel(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): Future[SetTimeModelResponse] =
    setTimeModel(setTimeModelRequest(mrt, generation, newTimeModel))

  override def setTimeModelRequest(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): SetTimeModelRequest =
    SetTimeModelRequest(nextSubmissionId(), Some(mrt.asProtobuf), generation, Some(newTimeModel))

  override def setTimeModel(
      request: SetTimeModelRequest
  ): Future[SetTimeModelResponse] =
    services.configManagement.setTimeModel(request)

  override def prune(
      pruneUpTo: LedgerOffset,
      attempts: Int = 10,
      pruneAllDivulgedContracts: Boolean = false,
  ): Future[PruneResponse] =
    // Distributed ledger participants need to reach global consensus prior to pruning. Hence the "eventually" here:
    eventually(assertionName = "Prune", attempts = attempts) {
      services.participantPruning
        .prune(
          PruneRequest(pruneUpTo.getAbsolute, nextSubmissionId(), pruneAllDivulgedContracts)
        )
        .andThen { case Failure(exception) =>
          logger.warn("Failed to prune", exception)(LoggingContext.ForTesting)
        }
    }

  private[infrastructure] override def preallocateParties(
      n: Int,
      participants: Iterable[ParticipantTestContext],
  ): Future[Vector[Party]] =
    for {
      parties <-
        if (partyAllocationConfig.allocateParties) {
          allocateParties(n)
        } else {
          reservePartyNames(n)
        }
      _ <- waitForParties(participants, parties.toSet)
    } yield parties

  private def reservePartyNames(n: Int): Future[Vector[Party]] =
    Future.successful(Vector.fill(n)(Party(nextPartyHintId())))
}
