// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.ChannelEndpoint.JsonApiEndpoint
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.{
  IncludeInterfaceView,
  topologyResultFilter,
}
import com.daml.ledger.api.testtool.infrastructure.time.DelayMechanism
import com.daml.ledger.api.testtool.infrastructure.{ChannelEndpoint, ExternalParty, Party}
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.package_management_service.{
  PackageDetails,
  UpdateVettedPackagesRequest,
  UpdateVettedPackagesResponse,
  UploadDarFileRequest,
  ValidateDarFileRequest,
}
import com.daml.ledger.api.v2.admin.participant_pruning_service.PruneResponse
import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.command_completion_service.{
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitRequest,
  SubmitAndWaitResponse,
}
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event_query_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  CostEstimationHints,
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionAndWaitForTransactionResponse,
  ExecuteSubmissionAndWaitRequest,
  ExecuteSubmissionAndWaitResponse,
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  GetPreferredPackageVersionResponse,
  GetPreferredPackagesResponse,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
}
import com.daml.ledger.api.v2.package_service.{
  GetPackageResponse,
  ListVettedPackagesRequest,
  ListVettedPackagesResponse,
  PackageStatus,
}
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
import com.daml.ledger.api.v2.update_service.*
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, ContractId, Exercised, Update}
import com.daml.ledger.javaapi.data.{Command, Identifier, Template, Value}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.util.MonadUtil
import com.google.protobuf.ByteString
import io.grpc.health.v1.health.HealthCheckResponse

import java.security.KeyPair
import java.time.Instant
import java.util.List as JList
import scala.concurrent.{ExecutionContext, Future}

trait ParticipantTestContext extends UserManagementTestContext {

  val begin: Long = 0L

  val userId: String
  val endpointId: String
  def ledgerEndpoint: Either[JsonApiEndpoint, ChannelEndpoint]
  def adminEndpoint: ChannelEndpoint
  def features: Features
  def referenceOffset: Long
  def nextKeyId: () => String
  def nextUserId: () => String
  def nextIdentityProviderId: () => String
  def nextPartyId: () => String
  def delayMechanism: DelayMechanism

  /** Gets the absolute offset of the ledger end at a point in time.
    */
  def currentEnd(): Future[Long]

  /** Returns an absolute offset (positive integer) that is beyond the current ledger end.
    *
    * Note: the offset might not be valid for the underlying ledger. This method can therefore only
    * be used for offsets that are only interpreted by the ledger API server and not sent to the
    * ledger.
    */
  def offsetBeyondLedgerEnd(): Future[Long]
  def time(): Future[Instant]
  def setTime(currentTime: Instant, newTime: Instant): Future[Unit]
  def listKnownPackages(): Future[Seq[PackageDetails]]

  def uploadDarFileAndVetOnConnectedSynchronizers(bytes: ByteString): Future[Unit] = for {
    connected <- connectedSynchronizers()
    _ <- MonadUtil.sequentialTraverse(connected)(synchronizerId =>
      uploadDarFile(
        UploadDarFileRequest(
          darFile = bytes,
          submissionId = "",
          UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES,
          synchronizerId = synchronizerId,
        )
      )
    )
  } yield ()

  def validateDarFile(bytes: ByteString): Future[Unit] =
    validateDarFile(ValidateDarFileRequest(bytes, "", ""))
  def validateDarFile(request: ValidateDarFileRequest): Future[Unit]

  def uploadDarRequest(bytes: ByteString, synchronizerId: String): UploadDarFileRequest
  def uploadDarFile(request: UploadDarFileRequest): Future[Unit]
  def getParticipantId(): Future[String]
  def listPackages(): Future[Seq[String]]
  def listVettedPackages(request: ListVettedPackagesRequest): Future[ListVettedPackagesResponse]
  def updateVettedPackages(
      request: UpdateVettedPackagesRequest
  ): Future[UpdateVettedPackagesResponse]
  def getPackage(packageId: String): Future[GetPackageResponse]
  def getPackageStatus(packageId: String): Future[PackageStatus]
  def prepareSubmission(
      prepareSubmissionRequest: PrepareSubmissionRequest
  ): Future[PrepareSubmissionResponse]
  def executeSubmission(
      executeSubmissionRequest: ExecuteSubmissionRequest
  ): Future[ExecuteSubmissionResponse]
  def executeSubmissionAndWait(
      executeSubmissionAndWaitRequest: ExecuteSubmissionAndWaitRequest
  ): Future[ExecuteSubmissionAndWaitResponse]
  def executeSubmissionAndWaitForTransaction(
      executeSubmissionAndWaitForTransactionRequest: ExecuteSubmissionAndWaitForTransactionRequest
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse]
  def getPreferredPackageVersion(
      parties: Seq[Party],
      packageName: String,
      vettingValidAt: Option[Instant] = None,
      synchronizerIdO: Option[String] = None,
  ): Future[GetPreferredPackageVersionResponse]
  def getPreferredPackages(
      vettingRequirements: Map[String, Seq[Party]],
      vettingValidAt: Option[Instant] = None,
      synchronizerIdO: Option[String] = None,
  ): Future[GetPreferredPackagesResponse]

  def connectedSynchronizers(): Future[Seq[String]]

  /** Managed version of party allocation, should be used anywhere a party has to be allocated
    * unless the party management service itself is under test
    */
  def allocateParty(): Future[Party]
  def allocateExternalPartyFromHint(
      partyIdHint: Option[String] = None,
      minSynchronizers: Int = 1,
  ): Future[ExternalParty]

  /** Non managed version of party allocation. Use exclusively when testing the party management
    * service.
    */
  def allocateParty(
      partyIdHint: Option[String] = None,
      localMetadata: Option[ObjectMeta] = None,
      identityProviderId: Option[String] = None,
      minSynchronizers: Option[Int] = None,
      userId: String = "",
  ): Future[Party]

  def allocateExternalPartyRequest(
      keyPair: KeyPair,
      partyIdHint: Option[String] = None,
      synchronizer: String = "",
  ): Future[AllocateExternalPartyRequest]

  def generateExternalPartyTopologyRequest(
      namespacePublicKey: Array[Byte],
      partyIdHint: Option[String] = None,
  ): Future[GenerateExternalPartyTopologyResponse]

  def allocateExternalParty(
      request: AllocateExternalPartyRequest,
      minSynchronizers: Option[Int] = None,
  ): Future[Party]

  def allocateParty(
      req: AllocatePartyRequest,
      connectedSynchronizers: Int,
  ): Future[(AllocatePartyResponse, Seq[String])]
  def updatePartyDetails(req: UpdatePartyDetailsRequest): Future[UpdatePartyDetailsResponse]
  def generateExternalPartyTopology(
      req: GenerateExternalPartyTopologyRequest
  ): Future[GenerateExternalPartyTopologyResponse]
  def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderIdRequest
  ): Future[UpdatePartyIdentityProviderIdResponse]
  def allocateExternalParties(
      partiesCount: Int,
      minSynchronizers: Int,
  ): Future[Vector[ExternalParty]]
  def allocateParties(n: Int, minSynchronizers: Int): Future[Vector[Party]]
  def getParties(req: GetPartiesRequest): Future[GetPartiesResponse]
  def getParties(parties: Seq[Party]): Future[Seq[PartyDetails]]

  def listKnownPartiesExpanded(): Future[Set[Party]]

  def listKnownParties(req: ListKnownPartiesRequest): Future[ListKnownPartiesResponse]

  def listKnownParties(): Future[ListKnownPartiesResponse]

  /** @return
    *   a future that completes when all the participants can list all the expected parties
    */
  def waitForPartiesOnOtherParticipants(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
      connectedSynchronizers: Int,
  ): Future[Unit]
  def activeContracts(
      request: GetActiveContractsRequest
  ): Future[Vector[CreatedEvent]]
  def activeContractsIds(
      request: GetActiveContractsRequest
  ): Future[Vector[ContractId[Any]]] =
    activeContracts(request).map { case createEvents: Seq[CreatedEvent] =>
      createEvents.map(c => new ContractId[Any](c.contractId))
    }

  def activeContractsRequest(
      parties: Option[Seq[Party]],
      activeAtOffset: Long,
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      verbose: Boolean = true,
  ): GetActiveContractsRequest
  def activeContracts(
      parties: Option[Seq[Party]],
      activeAtOffsetO: Option[Long] = None,
      verbose: Boolean = true,
  ): Future[Vector[CreatedEvent]]
  def activeContractsByTemplateId(
      templateIds: Seq[Identifier],
      parties: Option[Seq[Party]],
      activeAtOffsetO: Option[Long] = None,
      verbose: Boolean = true,
  ): Future[Vector[CreatedEvent]]

  def contract(
      queryingParties: Option[Seq[Party]],
      contractId: String,
  ): Future[Option[CreatedEvent]]

  /** Create an EventFormat with a set of Party objects.
    *
    * You should use this only when you need to tweak the request of
    * [[transactions(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdatesRequest):*]],
    * otherwise use the shortcut override that allows you to directly pass a set of Party
    */
  def eventFormat(
      verbose: Boolean,
      parties: Option[Seq[Party]],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
  ): EventFormat

  /** Create a TransactionFormat with a set of Party objects.
    *
    * You should use this only when you need to tweak the request of
    * [[transactions(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdatesRequest):*]],
    * otherwise use the shortcut override that allows you to directly pass a set of Party
    */
  def transactionFormat(
      parties: Option[Seq[Party]],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      transactionShape: TransactionShape = AcsDelta,
      verbose: Boolean = false,
  ): TransactionFormat

  def filters(
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
  ): Filters

  def updates(
      take: Int,
      request: GetUpdatesRequest,
  ): Future[Vector[GetUpdatesResponse.Update]]

  def updates(
      take: Int,
      request: GetUpdatesRequest,
      resultFilter: GetUpdatesResponse => Boolean,
  ): Future[Vector[GetUpdatesResponse.Update]]

  def updates(
      within: NonNegativeFiniteDuration,
      request: GetUpdatesRequest,
  ): Future[Vector[GetUpdatesResponse.Update]]

  def getTransactionsRequest(
      transactionFormat: TransactionFormat,
      begin: Long = referenceOffset,
  ): Future[GetUpdatesRequest]

  def getTransactionsRequestWithEnd(
      transactionFormat: TransactionFormat,
      begin: Long = referenceOffset,
      end: Option[Long],
  ): GetUpdatesRequest

  def getUpdatesRequestWithEnd(
      transactionFormatO: Option[TransactionFormat] = None,
      reassignmentsFormatO: Option[EventFormat] = None,
      topologyFilterO: Option[Seq[Party]] = None,
      begin: Long = referenceOffset,
      end: Option[Long] = None,
  ): GetUpdatesRequest

  def transactionsByTemplateId(
      templateId: Identifier,
      parties: Option[Seq[Party]],
  ): Future[Vector[Transaction]]

  /** Non-managed version of
    * [[transactions(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdatesRequest):*]], use
    * this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactions(request: GetUpdatesRequest): Future[Vector[Transaction]]

  /** Managed version of
    * [[transactions(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdatesRequest):*]], use
    * this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactions(transactionShape: TransactionShape, parties: Party*): Future[Vector[Transaction]]

  /** Non-managed version of
    * [[transactions(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdatesRequest):*]], use
    * this only if you need to tweak the request (i.e. to test low-level details)
    */
  def transactions(
      take: Int,
      request: GetUpdatesRequest,
  ): Future[Vector[Transaction]]

  def transactions(
      take: Int,
      transactionShape: TransactionShape,
      parties: Party*
  ): Future[Vector[Transaction]]

  /** Managed version of transactionTrees, use this unless you need to tweak the request (i.e. to
    * test low-level details)
    */
  def transactionTreeById(updateId: String, parties: Party*): Future[Transaction]

  /** Non-managed version of
    * [[updateById(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdateByIdRequest):*]],
    * use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def updateById(request: GetUpdateByIdRequest): Future[GetUpdateResponse]

  /** Managed version of
    * [[updateById(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdateByIdRequest):*]] for
    * transactions, use this unless you need to tweak the request (i.e. to test low-level details)
    */
  def transactionById(
      updateId: String,
      parties: Seq[Party],
      transactionShape: TransactionShape = AcsDelta,
      templateIds: Seq[Identifier] = Seq.empty,
  ): Future[Transaction]

  /** Managed version of
    * [[updateById(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdateByIdRequest):*]] for
    * topology transactions, use this unless you need to tweak the request (i.e. to test low-level
    * details). If the parties list is empty then no filtering is applied to the topology
    * transactions.
    */
  def topologyTransactionById(
      updateId: String,
      parties: Seq[Party],
  ): Future[TopologyTransaction]

  /** Managed version of transactionTreeByOffset
    */
  def transactionTreeByOffset(offset: Long, parties: Party*): Future[Transaction]

  /** Non-managed version of
    * [[updateByOffset(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdateByOffsetRequest):*]],
    * use this only if you need to tweak the request (i.e. to test low-level details)
    */
  def updateByOffset(request: GetUpdateByOffsetRequest): Future[GetUpdateResponse]

  /** Managed version of
    * [[updateByOffset(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdateByOffsetRequest):*]]
    * for transactions, use this unless you need to tweak the request (i.e. to test low-level
    * details)
    */
  def transactionByOffset(
      offset: Long,
      parties: Seq[Party],
      transactionShape: TransactionShape,
      templateIds: Seq[Identifier] = Seq.empty,
  ): Future[Transaction]

  /** Managed version of
    * [[updateByOffset(request:com\.daml\.ledger\.api\.v2\.update_service\.GetUpdateByOffsetRequest):*]]
    * for topology transactions, use this unless you need to tweak the request (i.e. to test
    * low-level details). If the parties list is empty then no filtering is applied to the topology
    * transactions.
    */
  def topologyTransactionByOffset(
      offset: Long,
      parties: Seq[Party],
  ): Future[TopologyTransaction]

  def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse]

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
  def createAndGetUpdateId[TCid <: ContractId[T], T <: Template](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[(String, TCid)]
  def exercise[T](
      party: Party,
      exercise: Update[T],
      transactionShape: TransactionShape = LedgerEffects,
      verbose: Boolean = true,
  ): Future[Transaction]
  def exercise[T](
      actAs: List[Party],
      readAs: List[Party],
      exercise: Update[T],
  ): Future[Transaction]
  def exerciseAndGetContract[TCid <: ContractId[T], T](
      party: Party,
      exercise: Update[Exercised[TCid]],
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid]
  def exerciseByKey(
      party: Party,
      template: Identifier,
      key: Value,
      choice: String,
      argument: Value,
  ): Future[Transaction]
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
  def submitAndWaitForTransactionRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
      transactionShape: TransactionShape,
  ): SubmitAndWaitForTransactionRequest
  def submitAndWaitRequest(party: Party, commands: JList[Command]): SubmitAndWaitRequest
  def prepareSubmissionRequest(
      party: Party,
      commands: JList[Command],
      estimateTrafficCost: Option[CostEstimationHints] = None,
  ): PrepareSubmissionRequest
  def executeSubmissionRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
  ): ExecuteSubmissionRequest
  def executeSubmissionAndWaitRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
  ): ExecuteSubmissionAndWaitRequest
  def executeSubmissionAndWaitForTransactionRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
      transactionFormat: Option[TransactionFormat],
  ): ExecuteSubmissionAndWaitForTransactionRequest
  def submitAndWaitForTransactionRequest(
      party: Party,
      commands: JList[Command],
  ): SubmitAndWaitForTransactionRequest
  def submitAndWaitForTransactionRequest(
      party: Party,
      commands: JList[Command],
      transactionShape: TransactionShape,
      filterParties: Option[Seq[Party]] = None,
      templateIds: Seq[Identifier] = Seq.empty,
      verbose: Boolean = true,
  ): SubmitAndWaitForTransactionRequest
  def submit(request: SubmitRequest): Future[Unit]
  def submitAndWait(request: SubmitAndWaitRequest): Future[SubmitAndWaitResponse]
  def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse]
  def submitRequestAndTolerateGrpcError[T](
      errorCode: ErrorCode,
      submitAndWaitGeneric: ParticipantTestContext => Future[T],
  ): Future[T]
  def completions(
      within: NonNegativeFiniteDuration,
      request: CompletionStreamRequest,
  ): Future[Vector[CompletionStreamResponse.CompletionResponse]]
  def completions(
      take: Int,
      request: CompletionStreamRequest,
  ): Future[Vector[CompletionStreamResponse.CompletionResponse]]

  def completionStreamRequest(from: Long = referenceOffset)(
      parties: Party*
  ): CompletionStreamRequest
  def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]]
  def firstCompletions(parties: Party*): Future[Vector[Completion]]
  def findCompletionAtOffset(
      offset: Long,
      p: Completion => Boolean,
  )(parties: Party*): Future[Option[Completion]]
  def findCompletion(
      request: CompletionStreamRequest
  )(p: Completion => Boolean): Future[Option[Completion]]
  def findCompletion(parties: Party*)(
      p: Completion => Boolean
  ): Future[Option[Completion]]
  def offsets(n: Int, request: CompletionStreamRequest): Future[Vector[Long]]
  def checkHealth(): Future[HealthCheckResponse]
  def watchHealth(): Future[Seq[HealthCheckResponse]]

  private[infrastructure] def preallocateParties(
      n: Int,
      participants: Iterable[ParticipantTestContext],
      connectedSynchronizers: Int,
  ): Future[Vector[Party]]

  def getConnectedSynchronizers(
      party: Option[Party],
      participantId: Option[String],
      identityProviderId: Option[String] = None,
  ): Future[Set[String]]

  def prune(
      pruneUpTo: Long,
      attempts: Int = 10,
  ): Future[PruneResponse]

  /** We are retrying a command submission + pruning to get a safe-to-prune offset for Canton.
    * That's because in Canton pruning will fail unless ACS commitments have been exchanged between
    * participants. To this end, repeatedly submitting commands is prompting Canton to exchange ACS
    * commitments and allows the pruning call to eventually succeed.
    */
  def pruneCantonSafe(
      pruneUpTo: Long,
      party: Party,
      dummyCommand: Party => JList[Command],
  )(implicit ec: ExecutionContext): Future[Unit]

  def latestPrunedOffsets(): Future[(Long, Long)]
  def maxOffsetCheckpointEmissionDelay: NonNegativeFiniteDuration

  def participantAuthorizationTransaction(
      partyIdSubstring: String,
      begin: Option[Long] = None,
      end: Option[Long] = None,
  ): Future[TopologyTransaction] = updates(
    1,
    getUpdatesRequestWithEnd(
      topologyFilterO = Some(Nil),
      begin = begin.getOrElse(referenceOffset),
      end = end,
    ),
    topologyResultFilter(partyIdSubstring),
  ).map(
    _.headOption
      .flatMap(_.topologyTransaction)
      .getOrElse(throw new RuntimeException("at least one transaction should have been found"))
  )
}

object ParticipantTestContext {
  type IncludeInterfaceView = Boolean

  def topologyResultFilter(partyIdSubstring: String)(
      response: GetUpdatesResponse
  ): Boolean =
    response.update.isTopologyTransaction && response.getTopologyTransaction.events
      // filtering for topology transaction about the party (for any participants, for any synchronizers)
      .exists(
        _.getParticipantAuthorizationAdded.partyId.contains(partyIdSubstring)
      )

}
