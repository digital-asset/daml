// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.time.Instant
import java.util.concurrent.TimeoutException

import com.daml.ledger.api.refinements.ApiTypes.TemplateId
import com.daml.ledger.api.testtool.infrastructure.Endpoint
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.IncludeInterfaceView
import com.daml.ledger.api.testtool.infrastructure.time.{DelayMechanism, Durations}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsRequest
import com.daml.ledger.api.v1.admin.config_management_service.{
  GetTimeModelResponse,
  SetTimeModelRequest,
  SetTimeModelResponse,
  TimeModel,
}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.package_management_service.{
  PackageDetails,
  UploadDarFileRequest,
}
import com.daml.ledger.api.v1.admin.participant_pruning_service.PruneResponse
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  AllocatePartyResponse,
  GetPartiesRequest,
  GetPartiesResponse,
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
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.package_service.{GetPackageResponse, PackageStatus}
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
  GetTransactionsResponse,
}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.client.binding.{Primitive, Template}
import com.daml.lf.data.Ref.HexString
import com.daml.timer.Delayed
import com.google.protobuf.ByteString
import io.grpc.health.v1.health.HealthCheckResponse
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class TimeoutParticipantTestContext(timeoutScaleFactor: Double, delegate: ParticipantTestContext)
    extends ParticipantTestContext {

  private val timeoutDuration = Durations.scaleDuration(15.seconds, timeoutScaleFactor)

  override val ledgerId: String = delegate.ledgerId
  override val applicationId: String = delegate.applicationId
  override val endpointId: String = delegate.endpointId
  override private[participant] def services = delegate.services
  override private[participant] implicit val ec: ExecutionContext = delegate.ec
  override def ledgerEndpoint: Endpoint = delegate.ledgerEndpoint
  override def features: Features = delegate.features
  override def referenceOffset: LedgerOffset = delegate.referenceOffset
  override def nextKeyId: () => String = delegate.nextKeyId
  override def nextUserId: () => String = delegate.nextUserId
  override def nextPartyId: () => String = delegate.nextUserId
  override def delayMechanism: DelayMechanism = delegate.delayMechanism

  override def currentEnd(): Future[LedgerOffset] =
    withTimeout("Get current end", delegate.currentEnd())
  override def currentEnd(overrideLedgerId: String): Future[LedgerOffset] = withTimeout(
    s"Get current end for ledger id $overrideLedgerId",
    delegate.currentEnd(overrideLedgerId),
  )
  override def offsetBeyondLedgerEnd(): Future[LedgerOffset] = withTimeout(
    "Offset beyond ledger end",
    delegate.offsetBeyondLedgerEnd(),
  )
  override def time(): Future[Instant] = withTimeout("Get time", delegate.time())
  override def setTime(currentTime: Instant, newTime: Instant): Future[Unit] = withTimeout(
    "Set time",
    delegate.setTime(currentTime, newTime),
  )
  override def listKnownPackages(): Future[Seq[PackageDetails]] = withTimeout(
    "List known packages",
    delegate.listKnownPackages(),
  )
  override def uploadDarRequest(bytes: ByteString): UploadDarFileRequest =
    delegate.uploadDarRequest(bytes)
  override def uploadDarFile(request: UploadDarFileRequest): Future[Unit] = withTimeout(
    s"Upload dar file ${request.submissionId}",
    delegate.uploadDarFile(request),
  )
  override def participantId(): Future[String] =
    withTimeout("Get participant id", delegate.participantId())
  override def listPackages(): Future[Seq[String]] =
    withTimeout("List packages", delegate.listPackages())
  override def getPackage(packageId: String): Future[GetPackageResponse] = withTimeout(
    s"Get package $packageId",
    delegate.getPackage(packageId),
  )
  override def getPackageStatus(packageId: String): Future[PackageStatus] = withTimeout(
    s"Get package status $packageId",
    delegate.getPackageStatus(packageId),
  )
  override def allocateParty(): Future[Primitive.Party] =
    withTimeout("Allocate party", delegate.allocateParty())

  def allocateParty(req: AllocatePartyRequest): Future[AllocatePartyResponse] =
    withTimeout("Allocate party", delegate.allocateParty(req))

  override def updatePartyDetails(
      req: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] =
    withTimeout("Update party details", delegate.updatePartyDetails(req))

  override def allocateParty(
      partyIdHint: Option[String] = None,
      displayName: Option[String] = None,
      localMetadata: Option[ObjectMeta] = None,
  ): Future[Primitive.Party] = withTimeout(
    s"Allocate party with hint $partyIdHint and display name $displayName",
    delegate.allocateParty(partyIdHint, displayName, localMetadata),
  )

  override def getParties(req: GetPartiesRequest): Future[GetPartiesResponse] = withTimeout(
    s"Get parties",
    delegate.getParties(req),
  )

  override def allocateParties(n: Int): Future[Vector[Primitive.Party]] = withTimeout(
    s"Allocate $n parties",
    delegate.allocateParties(n),
  )
  override def getParties(parties: Seq[Primitive.Party]): Future[Seq[PartyDetails]] = withTimeout(
    s"Get parties $parties",
    delegate.getParties(parties),
  )
  override def listKnownParties(): Future[Set[Primitive.Party]] = withTimeout(
    "List known parties",
    delegate.listKnownParties(),
  )
  override def listKnownPartiesResp(): Future[ListKnownPartiesResponse] = withTimeout(
    "List known parties",
    delegate.listKnownPartiesResp(),
  )

  override def waitForParties(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Primitive.Party],
  ): Future[Unit] = withTimeout(
    s"Wait for parties $expectedParties on participants ${otherParticipants.map(_.ledgerEndpoint)}",
    delegate.waitForParties(otherParticipants, expectedParties),
  )
  override def activeContracts(
      request: GetActiveContractsRequest
  ): Future[(Option[LedgerOffset], Vector[CreatedEvent])] = withTimeout(
    s"Active contracts for request $request",
    delegate.activeContracts(request),
  )
  override def activeContractsRequest(
      parties: Seq[Primitive.Party],
      templateIds: Seq[TemplateId],
      interfaceFilters: Seq[(TemplateId, IncludeInterfaceView)] = Seq.empty,
      activeAtOffset: String = "",
  ): GetActiveContractsRequest =
    delegate.activeContractsRequest(parties, templateIds, interfaceFilters, activeAtOffset)
  override def activeContracts(parties: Primitive.Party*): Future[Vector[CreatedEvent]] =
    withTimeout(s"Active contracts for parties $parties", delegate.activeContracts(parties: _*))
  override def activeContractsByTemplateId(
      templateIds: Seq[TemplateId],
      parties: Primitive.Party*
  ): Future[Vector[CreatedEvent]] = withTimeout(
    s"Active contracts by template ids $templateIds for parties $parties",
    delegate.activeContractsByTemplateId(templateIds, parties: _*),
  )

  def transactionFilter(
      parties: Seq[Primitive.Party],
      templateIds: Seq[TemplateId] = Seq.empty,
      interfaceFilters: Seq[(TemplateId, IncludeInterfaceView)] = Seq.empty,
  ) = delegate.transactionFilter(parties, templateIds, interfaceFilters)
  override def getTransactionsRequest(
      transactionFilter: TransactionFilter,
      begin: LedgerOffset,
  ): GetTransactionsRequest = delegate.getTransactionsRequest(transactionFilter, begin)
  override def transactionStream(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit = delegate.transactionStream(request, responseObserver)
  override def flatTransactionsByTemplateId(
      templateId: TemplateId,
      parties: Primitive.Party*
  ): Future[Vector[Transaction]] = withTimeout(
    s"Flat transaction by template id $templateId for parties $parties",
    delegate.flatTransactionsByTemplateId(templateId, parties: _*),
  )
  override def flatTransactions(request: GetTransactionsRequest): Future[Vector[Transaction]] =
    withTimeout(s"Flat transactions for request $request", delegate.flatTransactions(request))
  override def flatTransactions(parties: Primitive.Party*): Future[Vector[Transaction]] =
    withTimeout(s"Flat transactions for parties $parties", delegate.flatTransactions(parties: _*))
  override def flatTransactions(
      take: Int,
      request: GetTransactionsRequest,
  ): Future[Vector[Transaction]] = withTimeout(
    s"$take flat transactions for request $request",
    delegate.flatTransactions(take, request),
  )
  override def flatTransactions(take: Int, parties: Primitive.Party*): Future[Vector[Transaction]] =
    withTimeout(
      s"$take flat transactions for parties $parties",
      delegate.flatTransactions(take, parties: _*),
    )
  override def transactionTreesByTemplateId(
      templateId: TemplateId,
      parties: Primitive.Party*
  ): Future[Vector[TransactionTree]] = withTimeout(
    s"Transaction trees by template id $templateId for parties $parties",
    delegate.transactionTreesByTemplateId(templateId, parties: _*),
  )
  override def transactionTrees(request: GetTransactionsRequest): Future[Vector[TransactionTree]] =
    withTimeout(s"Transaction trees for request $request", delegate.transactionTrees(request))
  override def transactionTrees(parties: Primitive.Party*): Future[Vector[TransactionTree]] =
    withTimeout(s"Transaction trees for parties $parties", delegate.transactionTrees(parties: _*))
  override def transactionTrees(
      take: Int,
      request: GetTransactionsRequest,
  ): Future[Vector[TransactionTree]] = withTimeout(
    s"$take transaction trees for request $request",
    delegate.transactionTrees(take, request),
  )
  override def transactionTrees(
      take: Int,
      parties: Primitive.Party*
  ): Future[Vector[TransactionTree]] = withTimeout(
    s"$take transaction trees for parties $parties",
    delegate.transactionTrees(take, parties: _*),
  )
  override def getTransactionByIdRequest(
      transactionId: String,
      parties: Seq[Primitive.Party],
  ): GetTransactionByIdRequest =
    delegate.getTransactionByIdRequest(transactionId, parties)
  override def transactionTreeById(request: GetTransactionByIdRequest): Future[TransactionTree] =
    withTimeout(
      s"Get transaction tree by id for request $request",
      delegate.transactionTreeById(request),
    )
  override def transactionTreeById(
      transactionId: String,
      parties: Primitive.Party*
  ): Future[TransactionTree] = withTimeout(
    s"Get transaction tree by id for transaction id $transactionId and parties $parties",
    delegate.transactionTreeById(transactionId, parties: _*),
  )
  override def flatTransactionById(request: GetTransactionByIdRequest): Future[Transaction] =
    withTimeout(
      s"Flat transaction by id for request $request",
      delegate.flatTransactionById(request),
    )
  override def flatTransactionById(
      transactionId: String,
      parties: Primitive.Party*
  ): Future[Transaction] = withTimeout(
    s"Flat transaction by id for transaction id $transactionId and parties $parties",
    delegate.flatTransactionById(transactionId, parties: _*),
  )
  override def getTransactionByEventIdRequest(
      eventId: String,
      parties: Seq[Primitive.Party],
  ): GetTransactionByEventIdRequest =
    delegate.getTransactionByEventIdRequest(eventId, parties)
  override def transactionTreeByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[TransactionTree] = withTimeout(
    s"Transaction tree by event id for request $request",
    delegate.transactionTreeByEventId(request),
  )
  override def transactionTreeByEventId(
      eventId: String,
      parties: Primitive.Party*
  ): Future[TransactionTree] = withTimeout(
    s"Transaction tree by event id for event id $eventId and parties $parties",
    delegate.transactionTreeByEventId(eventId, parties: _*),
  )
  override def flatTransactionByEventId(
      request: GetTransactionByEventIdRequest
  ): Future[Transaction] = withTimeout(
    s"Flat transaction by event id for request $request",
    delegate.flatTransactionByEventId(request),
  )
  override def flatTransactionByEventId(
      eventId: String,
      parties: Primitive.Party*
  ): Future[Transaction] = withTimeout(
    s"Flat transaction by event id for event id $eventId and parties $parties",
    delegate.flatTransactionByEventId(eventId, parties: _*),
  )
  override def create[T](
      party: Primitive.Party,
      template: Template[T],
  ): Future[Primitive.ContractId[T]] =
    withTimeout(s"Create template for party $party", delegate.create(party, template))
  override def create[T](
      actAs: List[Primitive.Party],
      readAs: List[Primitive.Party],
      template: Template[T],
  ): Future[Primitive.ContractId[T]] = withTimeout(
    s"Create template for actAs $actAs and readAs $readAs",
    delegate.create(actAs, readAs, template),
  )
  override def createAndGetTransactionId[T](
      party: Primitive.Party,
      template: Template[T],
  ): Future[(String, Primitive.ContractId[T])] = withTimeout(
    s"Create and get transaction id for party $party",
    delegate.createAndGetTransactionId(party, template),
  )
  override def exercise[T](
      party: Primitive.Party,
      exercise: Primitive.Update[T],
  ): Future[TransactionTree] =
    withTimeout(s"Exercise for party $party", delegate.exercise(party, exercise))
  override def exercise[T](
      actAs: List[Primitive.Party],
      readAs: List[Primitive.Party],
      exercise: Primitive.Update[T],
  ): Future[TransactionTree] = withTimeout(
    s"Exercise for actAs $actAs and readAs $readAs",
    delegate.exercise(actAs, readAs, exercise),
  )
  override def exerciseForFlatTransaction[T](
      party: Primitive.Party,
      exercise: Primitive.Update[T],
  ): Future[Transaction] = withTimeout(
    s"Exercise for flat transaction for party $party",
    delegate.exerciseForFlatTransaction(party, exercise),
  )
  override def exerciseAndGetContract[T](
      party: Primitive.Party,
      exercise: Primitive.Update[Any],
  ): Future[Primitive.ContractId[T]] = withTimeout(
    s"Exercise and get contract for party $party",
    delegate.exerciseAndGetContract(party, exercise),
  )
  override def exerciseByKey[T](
      party: Primitive.Party,
      template: Primitive.TemplateId[T],
      key: Value,
      choice: String,
      argument: Value,
  ): Future[TransactionTree] = withTimeout(
    s"Exercise by key for party $party, template $template, key $key, choice $choice and argument $argument.",
    delegate.exerciseByKey(party, template, key, choice, argument),
  )
  override def submitRequest(
      actAs: List[Primitive.Party],
      readAs: List[Primitive.Party],
      commands: Command*
  ): SubmitRequest = delegate.submitRequest(actAs, readAs, commands: _*)
  override def submitRequest(party: Primitive.Party, commands: Command*): SubmitRequest =
    delegate.submitRequest(party, commands: _*)
  override def submitAndWaitRequest(
      actAs: List[Primitive.Party],
      readAs: List[Primitive.Party],
      commands: Command*
  ): SubmitAndWaitRequest = delegate.submitAndWaitRequest(actAs, readAs, commands: _*)
  override def submitAndWaitRequest(
      party: Primitive.Party,
      commands: Command*
  ): SubmitAndWaitRequest = delegate.submitAndWaitRequest(party, commands: _*)
  override def submit(request: SubmitRequest): Future[Unit] =
    withTimeout(s"Submit for request $request", delegate.submit(request))
  override def submitAndWait(request: SubmitAndWaitRequest): Future[Unit] = withTimeout(
    s"Submit and wait for request $request",
    delegate.submitAndWait(request),
  )
  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] = withTimeout(
    s"Submit and wait for transaction id for request $request",
    delegate.submitAndWaitForTransactionId(request),
  )
  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] = withTimeout(
    s"Submit and wait for transaction for request $request",
    delegate.submitAndWaitForTransaction(request),
  )
  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] = withTimeout(
    s"Submit and wait for transaction tree for request $request",
    delegate.submitAndWaitForTransactionTree(request),
  )
  override def completionStreamRequest(from: LedgerOffset)(
      parties: Primitive.Party*
  ): CompletionStreamRequest = delegate.completionStreamRequest(from)(parties: _*)
  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    withTimeout(
      s"Completion end for request $request",
      delegate.completionEnd(request),
    )
  override def completionStream(
      request: CompletionStreamRequest,
      streamObserver: StreamObserver[CompletionStreamResponse],
  ): Unit = delegate.completionStream(request, streamObserver)
  override def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]] =
    withTimeout(
      s"First completions for request $request",
      delegate.firstCompletions(request),
    )
  override def firstCompletions(parties: Primitive.Party*): Future[Vector[Completion]] =
    withTimeout(
      s"First completions for parties $parties",
      delegate.firstCompletions(parties: _*),
    )
  override def findCompletionAtOffset(offset: HexString, p: Completion => Boolean)(
      parties: Primitive.Party*
  ): Future[Option[ParticipantTestContext.CompletionResponse]] = withTimeout(
    s"Find completion at offset $offset for parties $parties",
    delegate.findCompletionAtOffset(offset, p)(parties: _*),
  )
  override def findCompletion(request: CompletionStreamRequest)(
      p: Completion => Boolean
  ): Future[Option[ParticipantTestContext.CompletionResponse]] = withTimeout(
    s"Find completion for request $request",
    delegate.findCompletion(request)(p),
  )
  override def findCompletion(parties: Primitive.Party*)(
      p: Completion => Boolean
  ): Future[Option[ParticipantTestContext.CompletionResponse]] =
    withTimeout(s"Find completion for parties $parties", delegate.findCompletion(parties: _*)(p))
  override def checkpoints(n: Int, request: CompletionStreamRequest): Future[Vector[Checkpoint]] =
    withTimeout(s"$n checkpoints for request $request", delegate.checkpoints(n, request))
  override def checkpoints(n: Int, from: LedgerOffset)(
      parties: Primitive.Party*
  ): Future[Vector[Checkpoint]] = withTimeout(
    s"$n checkpoints from offset $from for parties $parties",
    delegate.checkpoints(n, from)(parties: _*),
  )
  override def firstCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] = withTimeout(
    s"First checkpoint for request $request",
    delegate.firstCheckpoint(request),
  )
  override def firstCheckpoint(parties: Primitive.Party*): Future[Checkpoint] = withTimeout(
    s"First checkpoint for parties $parties",
    delegate.firstCheckpoint(parties: _*),
  )
  override def nextCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] = withTimeout(
    s"Next checkpoint for request $request",
    delegate.nextCheckpoint(request),
  )
  override def nextCheckpoint(from: LedgerOffset, parties: Primitive.Party*): Future[Checkpoint] =
    withTimeout(
      s"Next checkpoint from offset $from for parties $parties",
      delegate.nextCheckpoint(from, parties: _*),
    )
  override def configuration(overrideLedgerId: Option[String]): Future[LedgerConfiguration] =
    withTimeout(
      s"Configuration for ledger $overrideLedgerId",
      delegate.configuration(overrideLedgerId),
    )
  override def checkHealth(): Future[HealthCheckResponse] =
    withTimeout("Check health", delegate.checkHealth())
  override def watchHealth(): Future[Seq[HealthCheckResponse]] =
    withTimeout("Watch health", delegate.watchHealth())
  override def getTimeModel(): Future[GetTimeModelResponse] =
    withTimeout("Get time model", delegate.getTimeModel())
  override def setTimeModel(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): Future[SetTimeModelResponse] = withTimeout(
    s"Set time model with mrt $mrt, generation $generation and new model $newTimeModel",
    delegate.setTimeModel(mrt, generation, newTimeModel),
  )
  override def setTimeModelRequest(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): SetTimeModelRequest = delegate.setTimeModelRequest(mrt, generation, newTimeModel)
  override def setTimeModel(request: SetTimeModelRequest): Future[SetTimeModelResponse] =
    withTimeout(
      s"Set time model for request $request",
      delegate.setTimeModel(request),
    )
  override private[infrastructure] def preallocateParties(
      n: Int,
      participants: Iterable[ParticipantTestContext],
  ) = withTimeout(
    s"Preallocate $n parties on participants ${participants.map(_.ledgerEndpoint)}",
    delegate.preallocateParties(n, participants),
  )
  override def prune(
      pruneUpTo: LedgerOffset,
      attempts: Int,
      pruneAllDivulgedContracts: Boolean,
  ): Future[PruneResponse] = withTimeout(
    s"Prune up to $pruneUpTo, with $attempts attempts and divulged contracts [$pruneAllDivulgedContracts]",
    delegate.prune(pruneUpTo, attempts, pruneAllDivulgedContracts),
  )

  private def withTimeout[T](hint: String, future: => Future[T]): Future[T] = {
    Future.firstCompletedOf(
      Seq(
        Delayed.Future.by(timeoutDuration)(
          Future.failed(
            new TimeoutException(s"Operation [$hint] timed out after $timeoutDuration.")
          )
        ),
        future,
      )
    )
  }

  override def filters(
      templateIds: Seq[TemplateId],
      interfaceFilters: Seq[(TemplateId, IncludeInterfaceView)],
  ): Filters = delegate.filters(templateIds, interfaceFilters)
}
