// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.error.ErrorCode

import java.time.Instant
import java.util.concurrent.TimeoutException
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
  UpdatePartyIdentityProviderRequest,
  UpdatePartyIdentityProviderResponse,
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
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.package_service.{GetPackageResponse, PackageStatus}
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
  GetEventsByContractKeyRequest,
  GetEventsByContractKeyResponse,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
  GetTransactionsResponse,
}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.javaapi.data.{Command, Identifier, Party, Unit => UnitData}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, ContractId, Exercised, Update}
import com.daml.lf.data.Ref.HexString
import com.daml.timer.Delayed
import com.google.protobuf.ByteString
import io.grpc.health.v1.health.HealthCheckResponse
import io.grpc.stub.StreamObserver

import java.util.{List => JList}
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
  override def nextIdentityProviderId: () => String = delegate.nextIdentityProviderId

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
  override def allocateParty(): Future[Party] =
    withTimeout("Allocate party", delegate.allocateParty())

  def allocateParty(req: AllocatePartyRequest): Future[AllocatePartyResponse] =
    withTimeout("Allocate party", delegate.allocateParty(req))

  override def updatePartyDetails(
      req: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] =
    withTimeout("Update party details", delegate.updatePartyDetails(req))

  override def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderRequest
  ): Future[UpdatePartyIdentityProviderResponse] =
    withTimeout(
      "Update party identity provider id",
      delegate.updatePartyIdentityProviderId(request),
    )

  override def allocateParty(
      partyIdHint: Option[String] = None,
      displayName: Option[String] = None,
      localMetadata: Option[ObjectMeta] = None,
      identityProviderId: Option[String] = None,
  ): Future[Party] = withTimeout(
    s"Allocate party with hint $partyIdHint and display name $displayName",
    delegate.allocateParty(partyIdHint, displayName, localMetadata, identityProviderId),
  )

  override def getParties(req: GetPartiesRequest): Future[GetPartiesResponse] = withTimeout(
    s"Get parties",
    delegate.getParties(req),
  )

  override def allocateParties(n: Int): Future[Vector[Party]] = withTimeout(
    s"Allocate $n parties",
    delegate.allocateParties(n),
  )
  override def getParties(parties: Seq[Party]): Future[Seq[PartyDetails]] = withTimeout(
    s"Get parties $parties",
    delegate.getParties(parties),
  )
  override def listKnownParties(): Future[Set[Party]] = withTimeout(
    "List known parties",
    delegate.listKnownParties(),
  )
  override def listKnownPartiesResp(): Future[ListKnownPartiesResponse] = withTimeout(
    "List known parties",
    delegate.listKnownPartiesResp(),
  )

  override def waitForParties(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
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
      parties: Seq[Party],
      templateIds: Seq[Identifier],
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      activeAtOffset: String = "",
      useTemplateIdBasedLegacyFormat: Boolean = true,
  ): GetActiveContractsRequest =
    delegate.activeContractsRequest(
      parties,
      templateIds,
      interfaceFilters,
      activeAtOffset,
      useTemplateIdBasedLegacyFormat,
    )
  override def activeContracts(parties: Party*): Future[Vector[CreatedEvent]] =
    withTimeout(s"Active contracts for parties $parties", delegate.activeContracts(parties: _*))
  override def activeContractsByTemplateId(
      templateIds: Seq[Identifier],
      parties: Party*
  ): Future[Vector[CreatedEvent]] = withTimeout(
    s"Active contracts by template ids $templateIds for parties $parties",
    delegate.activeContractsByTemplateId(templateIds, parties: _*),
  )

  def transactionFilter(
      parties: Seq[Party],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      useTemplateIdBasedLegacyFormat: Boolean = true,
  ): TransactionFilter =
    delegate.transactionFilter(
      parties,
      templateIds,
      interfaceFilters,
      useTemplateIdBasedLegacyFormat,
    )
  override def filters(
      templateIds: Seq[Identifier],
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)],
      useTemplateIdBasedLegacyFormat: Boolean = true,
  ): Filters = delegate.filters(templateIds, interfaceFilters, useTemplateIdBasedLegacyFormat)
  override def getTransactionsRequest(
      transactionFilter: TransactionFilter,
      begin: LedgerOffset,
  ): GetTransactionsRequest = delegate.getTransactionsRequest(transactionFilter, begin)
  override def transactionStream(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit = delegate.transactionStream(request, responseObserver)
  override def flatTransactionsByTemplateId(
      templateId: Identifier,
      parties: Party*
  ): Future[Vector[Transaction]] = withTimeout(
    s"Flat transaction by template id $templateId for parties $parties",
    delegate.flatTransactionsByTemplateId(templateId, parties: _*),
  )
  override def flatTransactions(request: GetTransactionsRequest): Future[Vector[Transaction]] =
    withTimeout(s"Flat transactions for request $request", delegate.flatTransactions(request))
  override def flatTransactions(parties: Party*): Future[Vector[Transaction]] =
    withTimeout(s"Flat transactions for parties $parties", delegate.flatTransactions(parties: _*))
  override def flatTransactions(
      take: Int,
      request: GetTransactionsRequest,
  ): Future[Vector[Transaction]] = withTimeout(
    s"$take flat transactions for request $request",
    delegate.flatTransactions(take, request),
  )
  override def flatTransactions(take: Int, parties: Party*): Future[Vector[Transaction]] =
    withTimeout(
      s"$take flat transactions for parties $parties",
      delegate.flatTransactions(take, parties: _*),
    )
  override def transactionTreesByTemplateId(
      templateId: Identifier,
      parties: Party*
  ): Future[Vector[TransactionTree]] = withTimeout(
    s"Transaction trees by template id $templateId for parties $parties",
    delegate.transactionTreesByTemplateId(templateId, parties: _*),
  )
  override def transactionTrees(request: GetTransactionsRequest): Future[Vector[TransactionTree]] =
    withTimeout(s"Transaction trees for request $request", delegate.transactionTrees(request))
  override def transactionTrees(parties: Party*): Future[Vector[TransactionTree]] =
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
      parties: Party*
  ): Future[Vector[TransactionTree]] = withTimeout(
    s"$take transaction trees for parties $parties",
    delegate.transactionTrees(take, parties: _*),
  )
  override def getTransactionByIdRequest(
      transactionId: String,
      parties: Seq[Party],
  ): GetTransactionByIdRequest =
    delegate.getTransactionByIdRequest(transactionId, parties)
  override def transactionTreeById(request: GetTransactionByIdRequest): Future[TransactionTree] =
    withTimeout(
      s"Get transaction tree by id for request $request",
      delegate.transactionTreeById(request),
    )
  override def transactionTreeById(
      transactionId: String,
      parties: Party*
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
      parties: Party*
  ): Future[Transaction] = withTimeout(
    s"Flat transaction by id for transaction id $transactionId and parties $parties",
    delegate.flatTransactionById(transactionId, parties: _*),
  )
  override def getTransactionByEventIdRequest(
      eventId: String,
      parties: Seq[Party],
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
      parties: Party*
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
      parties: Party*
  ): Future[Transaction] = withTimeout(
    s"Flat transaction by event id for event id $eventId and parties $parties",
    delegate.flatTransactionByEventId(eventId, parties: _*),
  )

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = withTimeout(
    s"Get events by contract id for request $request",
    delegate.getEventsByContractId(request),
  )

  override def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  ): Future[GetEventsByContractKeyResponse] = withTimeout(
    s"Get events by contract key for request $request",
    delegate.getEventsByContractKey(request),
  )

  override def create[
      TCid <: ContractId[T],
      T <: com.daml.ledger.javaapi.data.Template,
  ](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] =
    withTimeout(s"Create template for party $party", delegate.create(party, template))
  override def create[TCid <: ContractId[T], T <: com.daml.ledger.javaapi.data.Template](
      actAs: List[Party],
      readAs: List[Party],
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] = withTimeout(
    s"Create template for actAs $actAs and readAs $readAs",
    delegate.create(actAs, readAs, template),
  )
  override def createAndGetTransactionId[TCid <: ContractId[
    T
  ], T <: com.daml.ledger.javaapi.data.Template](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[(String, TCid)] = withTimeout(
    s"Create and get transaction id for party $party",
    delegate.createAndGetTransactionId(party, template),
  )
  override def exercise[T](
      party: Party,
      exercise: Update[T],
  ): Future[TransactionTree] =
    withTimeout(s"Exercise for party $party", delegate.exercise(party, exercise))
  override def exercise[T](
      actAs: List[Party],
      readAs: List[Party],
      exercise: Update[T],
  ): Future[TransactionTree] = withTimeout(
    s"Exercise for actAs $actAs and readAs $readAs",
    delegate.exercise(actAs, readAs, exercise),
  )
  override def exerciseForFlatTransaction[T](
      party: Party,
      exercise: Update[T],
  ): Future[Transaction] = withTimeout(
    s"Exercise for flat transaction for party $party",
    delegate.exerciseForFlatTransaction(party, exercise),
  )
  override def exerciseAndGetContract[TCid <: ContractId[T], T](
      party: Party,
      exercise: Update[Exercised[TCid]],
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] = withTimeout(
    s"Exercise and get contract for party $party",
    delegate.exerciseAndGetContract[TCid, T](party, exercise),
  )
  override def exerciseAndGetContractNoDisclose[TCid <: ContractId[?]](
      party: Party,
      exercise: Update[Exercised[UnitData]],
  )(implicit companion: ContractCompanion[?, TCid, ?]): Future[TCid] = withTimeout(
    s"Exercise and get non disclosed contract for party $party",
    delegate.exerciseAndGetContractNoDisclose[TCid](party, exercise),
  )
  override def exerciseByKey(
      party: Party,
      template: Identifier,
      key: Value,
      choice: String,
      argument: Value,
  ): Future[TransactionTree] = withTimeout(
    s"Exercise by key for party $party, template $template, key $key, choice $choice and argument $argument.",
    delegate.exerciseByKey(party, template, key, choice, argument),
  )
  override def submitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
  ): SubmitRequest = delegate.submitRequest(actAs, readAs, commands)
  override def submitRequest(party: Party, commands: JList[Command] = JList.of()): SubmitRequest =
    delegate.submitRequest(party, commands)
  override def submitAndWaitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
  ): SubmitAndWaitRequest = delegate.submitAndWaitRequest(actAs, readAs, commands)
  override def submitAndWaitRequest(
      party: Party,
      commands: JList[Command],
  ): SubmitAndWaitRequest = delegate.submitAndWaitRequest(party, commands)
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
  override def submitRequestAndTolerateGrpcError[T](
      errorToTolerate: ErrorCode,
      submitAndWaitGeneric: ParticipantTestContext => Future[T],
  ): Future[T] = // timeout enforced by submitAndWaitGeneric
    delegate.submitRequestAndTolerateGrpcError(errorToTolerate, submitAndWaitGeneric)
  override def completionStreamRequest(from: LedgerOffset)(
      parties: Party*
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
  override def firstCompletions(parties: Party*): Future[Vector[Completion]] =
    withTimeout(
      s"First completions for parties $parties",
      delegate.firstCompletions(parties: _*),
    )
  override def findCompletionAtOffset(offset: HexString, p: Completion => Boolean)(
      parties: Party*
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
  override def findCompletion(parties: Party*)(
      p: Completion => Boolean
  ): Future[Option[ParticipantTestContext.CompletionResponse]] =
    withTimeout(s"Find completion for parties $parties", delegate.findCompletion(parties: _*)(p))
  override def checkpoints(n: Int, request: CompletionStreamRequest): Future[Vector[Checkpoint]] =
    withTimeout(s"$n checkpoints for request $request", delegate.checkpoints(n, request))
  override def checkpoints(n: Int, from: LedgerOffset)(
      parties: Party*
  ): Future[Vector[Checkpoint]] = withTimeout(
    s"$n checkpoints from offset $from for parties $parties",
    delegate.checkpoints(n, from)(parties: _*),
  )
  override def firstCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] = withTimeout(
    s"First checkpoint for request $request",
    delegate.firstCheckpoint(request),
  )
  override def firstCheckpoint(parties: Party*): Future[Checkpoint] = withTimeout(
    s"First checkpoint for parties $parties",
    delegate.firstCheckpoint(parties: _*),
  )
  override def nextCheckpoint(request: CompletionStreamRequest): Future[Checkpoint] = withTimeout(
    s"Next checkpoint for request $request",
    delegate.nextCheckpoint(request),
  )
  override def nextCheckpoint(from: LedgerOffset, parties: Party*): Future[Checkpoint] =
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

  override def pruneCantonSafe(
      pruneUpTo: LedgerOffset,
      party: Party,
      dummyCommand: Party => JList[Command],
      pruneAllDivulgedContracts: Boolean = false,
  )(implicit ec: ExecutionContext): Future[Unit] =
    delegate.pruneCantonSafe(pruneUpTo, party, dummyCommand, pruneAllDivulgedContracts)

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

  override def latestPrunedOffsets(): Future[(LedgerOffset, LedgerOffset)] = withTimeout(
    "Requesting the latest pruned offsets",
    delegate.latestPrunedOffsets(),
  )
}
