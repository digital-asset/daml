// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.error.ErrorCode

import java.time.Instant
import com.daml.ledger.api.testtool.infrastructure.Endpoint
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.{
  CompletionResponse,
  IncludeInterfaceView,
}
import com.daml.ledger.api.testtool.infrastructure.time.DelayMechanism
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
import com.daml.ledger.javaapi.data.{Command, Identifier, Party, Template, Value, Unit => UnitData}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, ContractId, Exercised, Update}
import com.daml.lf.data.Ref.HexString
import com.google.protobuf.ByteString
import io.grpc.health.v1.health.HealthCheckResponse
import io.grpc.stub.StreamObserver

import java.util.{List => JList}
import scala.concurrent.{ExecutionContext, Future}

trait ParticipantTestContext extends UserManagementTestContext {

  val begin: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  /** A reference to the moving ledger end. If you want a fixed reference to the offset at
    * a given point in time, use [[currentEnd]]
    */
  val end: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  val ledgerId: String
  val applicationId: String
  val endpointId: String
  def ledgerEndpoint: Endpoint
  def features: Features
  def referenceOffset: LedgerOffset
  def nextKeyId: () => String
  def nextUserId: () => String
  def nextIdentityProviderId: () => String
  def nextPartyId: () => String
  def delayMechanism: DelayMechanism

  /** Gets the absolute offset of the ledger end at a point in time. Use [[end]] if you need
    * a reference to the moving end of the ledger.
    */
  def currentEnd(): Future[LedgerOffset]

  /** Works just like [[currentEnd]] but allows to override the ledger identifier.
    *
    * Used only for low-level testing. Please use the other method unless you want to test the
    * behavior of the ledger end endpoint with a wrong ledger identifier.
    */
  def currentEnd(overrideLedgerId: String): Future[LedgerOffset]

  /** Returns an absolute offset that is beyond the current ledger end.
    *
    * Note: offsets are opaque byte strings, but they are lexicographically sortable.
    * Prepending the current absolute ledger end with non-zero bytes creates an offset that
    * is be beyond the current ledger end for the ledger API server.
    * The offset might however not be valid for the underlying ledger.
    * This method can therefore only be used for offsets that are only interpreted by the
    * ledger API server and not sent to the ledger.
    */
  def offsetBeyondLedgerEnd(): Future[LedgerOffset]
  def time(): Future[Instant]
  def setTime(currentTime: Instant, newTime: Instant): Future[Unit]
  def listKnownPackages(): Future[Seq[PackageDetails]]
  def uploadDarFile(bytes: ByteString): Future[Unit] =
    uploadDarFile(new UploadDarFileRequest(bytes))
  def uploadDarRequest(bytes: ByteString): UploadDarFileRequest
  def uploadDarFile(request: UploadDarFileRequest): Future[Unit]
  def participantId(): Future[String]
  def listPackages(): Future[Seq[String]]
  def getPackage(packageId: String): Future[GetPackageResponse]
  def getPackageStatus(packageId: String): Future[PackageStatus]

  /** Managed version of party allocation, should be used anywhere a party has
    * to be allocated unless the party management service itself is under test
    */
  def allocateParty(): Future[Party]

  /** Non managed version of party allocation. Use exclusively when testing the party management service.
    */
  def allocateParty(
      partyIdHint: Option[String] = None,
      displayName: Option[String] = None,
      localMetadata: Option[ObjectMeta] = None,
      identityProviderId: Option[String] = None,
  ): Future[Party]

  def allocateParty(req: AllocatePartyRequest): Future[AllocatePartyResponse]
  def updatePartyDetails(req: UpdatePartyDetailsRequest): Future[UpdatePartyDetailsResponse]
  def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderRequest
  ): Future[UpdatePartyIdentityProviderResponse]
  def allocateParties(n: Int): Future[Vector[Party]]
  def getParties(req: GetPartiesRequest): Future[GetPartiesResponse]
  def getParties(parties: Seq[Party]): Future[Seq[PartyDetails]]
  def listKnownParties(): Future[Set[Party]]
  def listKnownPartiesResp(): Future[ListKnownPartiesResponse]

  /** @return a future that completes when all the participants can list all the expected parties
    */
  def waitForParties(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
  ): Future[Unit]
  def activeContracts(
      request: GetActiveContractsRequest
  ): Future[(Option[LedgerOffset], Vector[CreatedEvent])]
  def activeContractsIds(
      request: GetActiveContractsRequest
  ): Future[(Option[LedgerOffset], Vector[ContractId[Any]])] = {
    activeContracts(request).map { case (offset, createEvents: Seq[CreatedEvent]) =>
      (offset, createEvents.map(c => new ContractId[Any](c.contractId)))
    }
  }

  def activeContractsRequest(
      parties: Seq[Party],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      activeAtOffset: String = "",
      useTemplateIdBasedLegacyFormat: Boolean = true,
  ): GetActiveContractsRequest
  def activeContracts(parties: Party*): Future[Vector[CreatedEvent]]
  def activeContractsByTemplateId(
      templateIds: Seq[Identifier],
      parties: Party*
  ): Future[Vector[CreatedEvent]]

  /** Create a [[TransactionFilter]] with a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[flatTransactions]]
    * or [[transactionTrees]], otherwise use the shortcut override that allows you to
    * directly pass a set of [[Party]]
    */
  def transactionFilter(
      parties: Seq[Party],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      useTemplateIdBasedLegacyFormat: Boolean = true,
  ): TransactionFilter

  def filters(
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      useTemplateIdBasedLegacyFormat: Boolean = true,
  ): Filters

  def getTransactionsRequest(
      transactionFilter: TransactionFilter,
      begin: LedgerOffset = referenceOffset,
  ): GetTransactionsRequest

  def transactionStream(
      request: GetTransactionsRequest,
      responseObserver: StreamObserver[GetTransactionsResponse],
  ): Unit
  def flatTransactionsByTemplateId(
      templateId: Identifier,
      parties: Party*
  ): Future[Vector[Transaction]]

  /** Non-managed version of [[flatTransactions]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(request: GetTransactionsRequest): Future[Vector[Transaction]]

  /** Managed version of [[flatTransactions]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(parties: Party*): Future[Vector[Transaction]]

  /** Non-managed version of [[flatTransactions]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(take: Int, request: GetTransactionsRequest): Future[Vector[Transaction]]

  /** Managed version of [[flatTransactions]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactions(take: Int, parties: Party*): Future[Vector[Transaction]]
  def transactionTreesByTemplateId(
      templateId: Identifier,
      parties: Party*
  ): Future[Vector[TransactionTree]]

  /** Non-managed version of [[transactionTrees]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(request: GetTransactionsRequest): Future[Vector[TransactionTree]]

  /** Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(parties: Party*): Future[Vector[TransactionTree]]

  /** Non-managed version of [[transactionTrees]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(
      take: Int,
      request: GetTransactionsRequest,
  ): Future[Vector[TransactionTree]]

  /** Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTrees(take: Int, parties: Party*): Future[Vector[TransactionTree]]

  /** Create a [[GetTransactionByIdRequest]] with an identifier and a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[transactionTreeById]] or
    * [[flatTransactionById]], otherwise use the shortcut override that allows you to directly
    * pass the identifier and parties.
    */
  def getTransactionByIdRequest(
      transactionId: String,
      parties: Seq[Party],
  ): GetTransactionByIdRequest

  /** Non-managed version of [[transactionTreeById]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeById(request: GetTransactionByIdRequest): Future[TransactionTree]

  /** Managed version of [[transactionTrees]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeById(transactionId: String, parties: Party*): Future[TransactionTree]

  /** Non-managed version of [[flatTransactionById]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionById(request: GetTransactionByIdRequest): Future[Transaction]

  /** Managed version of [[flatTransactionById]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionById(transactionId: String, parties: Party*): Future[Transaction]

  /** Create a [[GetTransactionByEventIdRequest]] with an identifier and a set of [[Party]] objects.
    * You should use this only when you need to tweak the request of [[transactionTreeByEventId]] or
    * [[flatTransactionByEventId]], otherwise use the shortcut override that allows you to directly
    * pass the identifier and parties.
    */
  def getTransactionByEventIdRequest(
      eventId: String,
      parties: Seq[Party],
  ): GetTransactionByEventIdRequest

  /** Non-managed version of [[transactionTreeByEventId]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeByEventId(request: GetTransactionByEventIdRequest): Future[TransactionTree]

  /** Managed version of [[transactionTreeByEventId]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionTreeByEventId(eventId: String, parties: Party*): Future[TransactionTree]

  /** Non-managed version of [[flatTransactionByEventId]], use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionByEventId(request: GetTransactionByEventIdRequest): Future[Transaction]

  /** Managed version of [[flatTransactionByEventId]], use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def flatTransactionByEventId(eventId: String, parties: Party*): Future[Transaction]

  def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse]

  def getEventsByContractKey(
      request: GetEventsByContractKeyRequest
  ): Future[GetEventsByContractKeyResponse]

  def create[
      TCid <: ContractId[T],
      T <: Template,
  ](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid]
  def create[TCid <: ContractId[T], T <: Template](
      actAs: List[Party],
      readAs: List[Party],
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid]
  def createAndGetTransactionId[TCid <: ContractId[T], T <: Template](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[(String, TCid)]
  def exercise[T](
      party: Party,
      exercise: Update[T],
  ): Future[TransactionTree]
  def exercise[T](
      actAs: List[Party],
      readAs: List[Party],
      exercise: Update[T],
  ): Future[TransactionTree]
  def exerciseForFlatTransaction[T](
      party: Party,
      exercise: Update[T],
  ): Future[Transaction]
  def exerciseAndGetContract[TCid <: ContractId[T], T](
      party: Party,
      exercise: Update[Exercised[TCid]],
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid]
  def exerciseAndGetContractNoDisclose[TCid <: ContractId[?]](
      party: Party,
      exercise: Update[Exercised[UnitData]],
  )(implicit companion: ContractCompanion[?, TCid, ?]): Future[TCid]
  def exerciseByKey(
      party: Party,
      template: Identifier,
      key: Value,
      choice: String,
      argument: Value,
  ): Future[TransactionTree]
  def submitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
  ): SubmitRequest
  def submitRequest(party: Party, commands: JList[Command] = JList.of()): SubmitRequest
  def submitAndWaitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
  ): SubmitAndWaitRequest
  def submitAndWaitRequest(party: Party, commands: JList[Command]): SubmitAndWaitRequest
  def submit(request: SubmitRequest): Future[Unit]
  def submitAndWait(request: SubmitAndWaitRequest): Future[Unit]
  def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse]
  def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse]
  def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse]
  def submitRequestAndTolerateGrpcError[T](
      errorCode: ErrorCode,
      submitAndWaitGeneric: ParticipantTestContext => Future[T],
  ): Future[T]
  def completionStreamRequest(from: LedgerOffset = referenceOffset)(
      parties: Party*
  ): CompletionStreamRequest
  def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse]
  def completionStream(
      request: CompletionStreamRequest,
      streamObserver: StreamObserver[CompletionStreamResponse],
  ): Unit
  def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]]
  def firstCompletions(parties: Party*): Future[Vector[Completion]]
  def findCompletionAtOffset(
      offset: HexString,
      p: Completion => Boolean,
  )(parties: Party*): Future[Option[CompletionResponse]]
  def findCompletion(
      request: CompletionStreamRequest
  )(p: Completion => Boolean): Future[Option[CompletionResponse]]
  def findCompletion(parties: Party*)(
      p: Completion => Boolean
  ): Future[Option[CompletionResponse]]
  def checkpoints(n: Int, request: CompletionStreamRequest): Future[Vector[Checkpoint]]
  def checkpoints(n: Int, from: LedgerOffset = referenceOffset)(
      parties: Party*
  ): Future[Vector[Checkpoint]]
  def firstCheckpoint(request: CompletionStreamRequest): Future[Checkpoint]
  def firstCheckpoint(parties: Party*): Future[Checkpoint]
  def nextCheckpoint(request: CompletionStreamRequest): Future[Checkpoint]
  def nextCheckpoint(from: LedgerOffset, parties: Party*): Future[Checkpoint]
  def configuration(overrideLedgerId: Option[String] = None): Future[LedgerConfiguration]
  def checkHealth(): Future[HealthCheckResponse]
  def watchHealth(): Future[Seq[HealthCheckResponse]]
  def getTimeModel(): Future[GetTimeModelResponse]
  def setTimeModel(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): Future[SetTimeModelResponse]
  def setTimeModelRequest(
      mrt: Instant,
      generation: Long,
      newTimeModel: TimeModel,
  ): SetTimeModelRequest

  def setTimeModel(
      request: SetTimeModelRequest
  ): Future[SetTimeModelResponse]

  private[infrastructure] def preallocateParties(
      n: Int,
      participants: Iterable[ParticipantTestContext],
  ): Future[Vector[Party]]

  def prune(
      pruneUpTo: LedgerOffset,
      attempts: Int = 10,
      pruneAllDivulgedContracts: Boolean = false,
  ): Future[PruneResponse]

  /** We are retrying a command submission + pruning to get a safe-to-prune offset for Canton.
    * That's because in Canton pruning will fail unless ACS commitments have been exchanged between participants.
    * To this end, repeatedly submitting commands is prompting Canton to exchange ACS commitments
    * and allows the pruning call to eventually succeed.
    */
  def pruneCantonSafe(
      pruneUpTo: LedgerOffset,
      party: Party,
      dummyCommand: Party => JList[Command],
      pruneAllDivulgedContracts: Boolean = false,
  )(implicit ec: ExecutionContext): Future[Unit]

  def latestPrunedOffsets(): Future[(LedgerOffset, LedgerOffset)]
}

object ParticipantTestContext {
  type IncludeInterfaceView = Boolean

  case class CompletionResponse(completion: Completion, offset: LedgerOffset, recordTime: Instant)

}
