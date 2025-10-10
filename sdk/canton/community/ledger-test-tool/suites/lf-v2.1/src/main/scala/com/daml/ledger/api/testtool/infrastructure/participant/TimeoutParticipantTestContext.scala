// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.ChannelEndpoint.JsonApiEndpoint
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.IncludeInterfaceView
import com.daml.ledger.api.testtool.infrastructure.time.{DelayMechanism, Durations}
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
import com.daml.ledger.javaapi.data.{Command, Identifier, Template, Unit as UnitData, Value}
import com.daml.timer.Delayed
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.google.protobuf.ByteString
import io.grpc.health.v1.health.HealthCheckResponse

import java.security.KeyPair
import java.time.Instant
import java.util.List as JList
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class TimeoutParticipantTestContext(timeoutScaleFactor: Double, delegate: ParticipantTestContext)
    extends ParticipantTestContext {

  private val timeoutDuration = Durations.scaleDuration(15.seconds, timeoutScaleFactor)

  override val userId: String = delegate.userId
  override val endpointId: String = delegate.endpointId
  override private[participant] def services = delegate.services
  override private[participant] implicit val ec: ExecutionContext = delegate.ec
  override def ledgerEndpoint: Either[JsonApiEndpoint, ChannelEndpoint] = delegate.ledgerEndpoint
  override def adminEndpoint: ChannelEndpoint = delegate.adminEndpoint
  override def features: Features = delegate.features
  override def referenceOffset: Long = delegate.referenceOffset
  override def nextKeyId: () => String = delegate.nextKeyId
  override def nextUserId: () => String = delegate.nextUserId
  override def nextPartyId: () => String = delegate.nextUserId
  override def nextIdentityProviderId: () => String = delegate.nextIdentityProviderId

  override def delayMechanism: DelayMechanism = delegate.delayMechanism

  override def currentEnd(): Future[Long] =
    withTimeout("Get current end", delegate.currentEnd())
  override def offsetBeyondLedgerEnd(): Future[Long] = withTimeout(
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

  override def uploadDarRequest(bytes: ByteString, synchronizerId: String): UploadDarFileRequest =
    delegate.uploadDarRequest(bytes, synchronizerId)
  override def validateDarFile(request: ValidateDarFileRequest): Future[Unit] = withTimeout(
    s"Validate dar file ${request.submissionId}",
    delegate.validateDarFile(request),
  )
  override def uploadDarFile(request: UploadDarFileRequest): Future[Unit] = withTimeout(
    s"Upload dar file ${request.submissionId}",
    delegate.uploadDarFile(request),
  )

  override def listVettedPackages(
      request: ListVettedPackagesRequest
  ): Future[ListVettedPackagesResponse] =
    delegate.listVettedPackages(request)

  override def updateVettedPackages(
      request: UpdateVettedPackagesRequest
  ): Future[UpdateVettedPackagesResponse] =
    delegate.updateVettedPackages(request)

  override def getParticipantId(): Future[String] =
    withTimeout("Get participant id", delegate.getParticipantId())
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

  override def prepareSubmission(
      prepareSubmissionRequest: PrepareSubmissionRequest
  ): Future[PrepareSubmissionResponse] = withTimeout(
    s"Prepare submission",
    delegate.prepareSubmission(prepareSubmissionRequest),
  )
  def executeSubmission(
      executeSubmissionRequest: ExecuteSubmissionRequest
  ): Future[ExecuteSubmissionResponse] = withTimeout(
    s"Execute submission",
    delegate.executeSubmission(executeSubmissionRequest),
  )
  def executeSubmissionAndWait(
      executeSubmissionAndWaitRequest: ExecuteSubmissionAndWaitRequest
  ): Future[ExecuteSubmissionAndWaitResponse] = withTimeout(
    s"Execute submission and wait",
    delegate.executeSubmissionAndWait(executeSubmissionAndWaitRequest),
  )
  def executeSubmissionAndWaitForTransaction(
      executeSubmissionAndWaitForTransactionRequest: ExecuteSubmissionAndWaitForTransactionRequest
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse] = withTimeout(
    s"Execute submission and wait",
    delegate.executeSubmissionAndWaitForTransaction(executeSubmissionAndWaitForTransactionRequest),
  )
  override def getPreferredPackageVersion(
      parties: Seq[Party],
      packageName: String,
      vettingValidAt: Option[Instant] = None,
      synchronizerIdO: Option[String] = None,
  ): Future[GetPreferredPackageVersionResponse] = withTimeout(
    s"Get preferred package version for parties $parties, $packageName, $synchronizerIdO at $vettingValidAt",
    delegate.getPreferredPackageVersion(parties, packageName, vettingValidAt, synchronizerIdO),
  )

  override def getPreferredPackages(
      vettingRequirements: Map[String, Seq[Party]],
      vettingValidAt: Option[Instant] = None,
      synchronizerIdO: Option[String] = None,
  ): Future[GetPreferredPackagesResponse] = withTimeout(
    s"Get preferred package version for parties $vettingRequirements, $synchronizerIdO at $vettingValidAt",
    delegate.getPreferredPackages(vettingRequirements, vettingValidAt, synchronizerIdO),
  )

  override def connectedSynchronizers(): Future[Seq[String]] =
    withTimeout("Connected synchronizers", delegate.connectedSynchronizers())

  override def allocateParty(): Future[Party] =
    withTimeout("Allocate party", delegate.allocateParty())

  override def allocateExternalPartyFromHint(
      partyIdHint: Option[String] = None,
      minSynchronizers: Int,
  ): Future[ExternalParty] =
    withTimeout(
      "Allocate external party",
      delegate.allocateExternalPartyFromHint(partyIdHint, minSynchronizers),
    )

  def allocateParty(
      req: AllocatePartyRequest,
      connectedSynchronizers: Int,
  ): Future[(AllocatePartyResponse, Seq[String])] =
    withTimeout("Allocate party", delegate.allocateParty(req, connectedSynchronizers))

  override def updatePartyDetails(
      req: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] =
    withTimeout("Update party details", delegate.updatePartyDetails(req))

  override def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderIdRequest
  ): Future[UpdatePartyIdentityProviderIdResponse] =
    withTimeout(
      "Update party identity provider id",
      delegate.updatePartyIdentityProviderId(request),
    )

  override def allocateExternalParty(
      request: AllocateExternalPartyRequest,
      minSynchronizers: Option[Int] = None,
  ): Future[Party] = withTimeout(
    s"Allocate external party",
    delegate.allocateExternalParty(request, minSynchronizers),
  )

  override def allocateExternalPartyRequest(
      keyPair: KeyPair,
      partyIdHint: Option[String] = None,
      synchronizer: String = "",
  ): Future[AllocateExternalPartyRequest] =
    delegate.allocateExternalPartyRequest(keyPair, partyIdHint, synchronizer)

  override def generateExternalPartyTopologyRequest(
      namespacePublicKey: Array[Byte],
      partyIdHint: Option[String] = None,
  ): Future[GenerateExternalPartyTopologyResponse] = withTimeout(
    s"Generate topology transactions to allocate external party $partyIdHint",
    delegate.generateExternalPartyTopologyRequest(namespacePublicKey, partyIdHint),
  )

  override def allocateParty(
      partyIdHint: Option[String] = None,
      localMetadata: Option[ObjectMeta] = None,
      identityProviderId: Option[String] = None,
      minSynchronizers: Option[Int] = None,
      userId: String = "",
  ): Future[Party] = withTimeout(
    s"Allocate party with hint $partyIdHint",
    delegate.allocateParty(partyIdHint, localMetadata, identityProviderId, minSynchronizers, userId),
  )

  override def getParties(req: GetPartiesRequest): Future[GetPartiesResponse] = withTimeout(
    s"Get parties",
    delegate.getParties(req),
  )

  override def allocateExternalParties(
      n: Int,
      minSynchronizers: Int,
  ): Future[Vector[ExternalParty]] = withTimeout(
    s"Allocate $n parties",
    delegate.allocateExternalParties(n, minSynchronizers),
  )

  override def allocateParties(n: Int, minSynchronizers: Int): Future[Vector[Party]] = withTimeout(
    s"Allocate $n parties",
    delegate.allocateParties(n, minSynchronizers),
  )
  override def getParties(parties: Seq[Party]): Future[Seq[PartyDetails]] = withTimeout(
    s"Get parties $parties",
    delegate.getParties(parties),
  )
  override def listKnownPartiesExpanded(): Future[Set[Party]] = withTimeout(
    "List known parties",
    delegate.listKnownPartiesExpanded(),
  )
  override def listKnownParties(req: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    withTimeout(
      "List known parties",
      delegate.listKnownParties(req),
    )
  override def listKnownParties(): Future[ListKnownPartiesResponse] = withTimeout(
    "List known parties",
    delegate.listKnownParties(),
  )

  override def waitForPartiesOnOtherParticipants(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
      connectedSynchronizers: Int,
  ): Future[Unit] = withTimeout(
    s"Wait for parties $expectedParties on participants ${otherParticipants.map(_.ledgerEndpoint)}",
    delegate.waitForPartiesOnOtherParticipants(
      otherParticipants,
      expectedParties,
      connectedSynchronizers,
    ),
  )

  override def generateExternalPartyTopology(
      req: GenerateExternalPartyTopologyRequest
  ): Future[GenerateExternalPartyTopologyResponse] = delegate.generateExternalPartyTopology(req)

  override def activeContracts(
      request: GetActiveContractsRequest
  ): Future[Vector[CreatedEvent]] = withTimeout(
    s"Active contracts for request $request",
    delegate.activeContracts(request),
  )
  override def activeContractsRequest(
      parties: Option[Seq[Party]],
      activeAtOffset: Long,
      templateIds: Seq[Identifier],
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      verbose: Boolean = true,
  ): GetActiveContractsRequest =
    delegate.activeContractsRequest(
      parties,
      activeAtOffset,
      templateIds,
      interfaceFilters,
      verbose,
    )
  override def activeContracts(
      parties: Option[Seq[Party]],
      activeAtOffsetO: Option[Long],
      verbose: Boolean = true,
  ): Future[Vector[CreatedEvent]] =
    withTimeout(
      s"Active contracts for parties $parties",
      delegate.activeContracts(parties, activeAtOffsetO, verbose),
    )
  override def activeContractsByTemplateId(
      templateIds: Seq[Identifier],
      parties: Option[Seq[Party]],
      activeAtOffsetO: Option[Long],
      verbose: Boolean = true,
  ): Future[Vector[CreatedEvent]] = withTimeout(
    s"Active contracts by template ids $templateIds for parties $parties",
    delegate.activeContractsByTemplateId(templateIds, parties, activeAtOffsetO, verbose),
  )

  def eventFormat(
      verbose: Boolean,
      partiesO: Option[Seq[Party]],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
  ): EventFormat =
    delegate.eventFormat(
      verbose,
      partiesO,
      templateIds,
      interfaceFilters,
    )

  def transactionFormat(
      parties: Option[Seq[Party]],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      transactionShape: TransactionShape = AcsDelta,
      verbose: Boolean = false,
  ): TransactionFormat = delegate.transactionFormat(
    parties,
    templateIds,
    interfaceFilters,
    transactionShape,
    verbose,
  )

  override def filters(
      templateIds: Seq[Identifier],
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)],
  ): Filters = delegate.filters(templateIds, interfaceFilters)

  override def updates(
      take: Int,
      request: GetUpdatesRequest,
  ): Future[Vector[GetUpdatesResponse.Update]] =
    delegate.updates(take, request)

  override def updates(
      take: Int,
      request: GetUpdatesRequest,
      resultFilter: GetUpdatesResponse => Boolean,
  ): Future[Vector[GetUpdatesResponse.Update]] =
    delegate.updates(take, request, resultFilter)

  override def updates(
      within: NonNegativeFiniteDuration,
      request: GetUpdatesRequest,
  ): Future[Vector[GetUpdatesResponse.Update]] =
    delegate.updates(within, request)

  override def getTransactionsRequest(
      transactionFormat: TransactionFormat,
      begin: Long = referenceOffset,
  ): Future[GetUpdatesRequest] =
    delegate.getTransactionsRequest(
      transactionFormat = transactionFormat,
      begin = begin,
    )

  override def getTransactionsRequestWithEnd(
      transactionFormat: TransactionFormat,
      begin: Long = referenceOffset,
      end: Option[Long],
  ): GetUpdatesRequest =
    delegate.getTransactionsRequestWithEnd(
      transactionFormat = transactionFormat,
      begin = begin,
      end = end,
    )

  override def getUpdatesRequestWithEnd(
      transactionFormatO: Option[TransactionFormat] = None,
      reassignmentsFormatO: Option[EventFormat] = None,
      topologyFilterO: Option[Seq[Party]] = None,
      begin: Long = referenceOffset,
      end: Option[Long] = None,
  ): GetUpdatesRequest =
    delegate.getUpdatesRequestWithEnd(
      transactionFormatO = transactionFormatO,
      reassignmentsFormatO = reassignmentsFormatO,
      topologyFilterO = topologyFilterO,
      begin = begin,
      end = end,
    )

  override def transactionsByTemplateId(
      templateId: Identifier,
      parties: Option[Seq[Party]],
  ): Future[Vector[Transaction]] = withTimeout(
    s"Flat transaction by template id $templateId for parties $parties",
    delegate.transactionsByTemplateId(templateId, parties),
  )
  override def transactions(
      request: GetUpdatesRequest
  ): Future[Vector[Transaction]] =
    withTimeout(s"Flat transactions for request $request", delegate.transactions(request))

  override def transactions(
      transactionShape: TransactionShape,
      parties: Party*
  ): Future[Vector[Transaction]] =
    withTimeout(
      s"Flat transactions for parties $parties",
      delegate.transactions(transactionShape, parties*),
    )

  override def transactions(
      take: Int,
      request: GetUpdatesRequest,
  ): Future[Vector[Transaction]] = withTimeout(
    s"$take flat transactions for request $request",
    delegate.transactions(take, request),
  )
  override def transactions(
      take: Int,
      transactionShape: TransactionShape,
      parties: Party*
  ): Future[Vector[Transaction]] =
    withTimeout(
      s"$take $transactionShape transactions for parties $parties",
      delegate.transactions(take, transactionShape, parties*),
    )

  override def transactionTreeById(
      transactionId: String,
      parties: Party*
  ): Future[Transaction] = withTimeout(
    s"Get transaction tree by id for transaction id $transactionId and parties $parties",
    delegate.transactionTreeById(transactionId, parties*),
  )

  override def updateById(request: GetUpdateByIdRequest): Future[GetUpdateResponse] =
    withTimeout(
      s"Update by id for request $request",
      delegate.updateById(request),
    )

  override def transactionById(
      updateId: String,
      parties: Seq[Party],
      transactionShape: TransactionShape,
      templateIds: Seq[Identifier],
  ): Future[Transaction] = withTimeout(
    s"Transaction by id for update id $updateId, parties $parties and templates $templateIds",
    delegate.transactionById(updateId, parties, transactionShape, templateIds),
  )

  def topologyTransactionById(
      updateId: String,
      parties: Seq[Party],
  ): Future[TopologyTransaction] = withTimeout(
    s"Topology transaction by id for update id $updateId, parties $parties",
    delegate.topologyTransactionById(updateId, parties),
  )

  override def transactionTreeByOffset(
      offset: Long,
      parties: Party*
  ): Future[Transaction] = withTimeout(
    s"Transaction tree by offset for offset $offset and parties $parties",
    delegate.transactionTreeByOffset(offset, parties*),
  )

  override def updateByOffset(request: GetUpdateByOffsetRequest): Future[GetUpdateResponse] =
    withTimeout(
      s"Update by offset for request $request",
      delegate.updateByOffset(request),
    )

  def transactionByOffset(
      offset: Long,
      parties: Seq[Party],
      transactionShape: TransactionShape,
      templateIds: Seq[Identifier],
  ): Future[Transaction] = withTimeout(
    s"Transaction by offset for offset $offset, parties $parties, templates $templateIds",
    delegate.transactionByOffset(offset, parties, transactionShape, templateIds),
  )

  def topologyTransactionByOffset(
      offset: Long,
      parties: Seq[Party],
  ): Future[TopologyTransaction] = withTimeout(
    s"Topology transaction by offset for offset $offset, parties $parties",
    delegate.topologyTransactionByOffset(offset, parties),
  )

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] = withTimeout(
    s"Get events by contract id for request $request",
    delegate.getEventsByContractId(request),
  )

  override def create[
      TCid <: ContractId[T],
      T <: Template,
  ](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] =
    withTimeout(s"Create template for party $party", delegate.create(party, template))
  override def create[TCid <: ContractId[T], T <: Template](
      actAs: List[Party],
      readAs: List[Party],
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] = withTimeout(
    s"Create template for actAs $actAs and readAs $readAs",
    delegate.create(actAs, readAs, template),
  )
  override def createAndGetTransactionId[
      TCid <: ContractId[T],
      T <: Template,
  ](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[(String, TCid)] = withTimeout(
    s"Create and get transaction id for party $party",
    delegate.createAndGetTransactionId(party, template),
  )
  override def exercise[T](
      party: Party,
      exercise: Update[T],
      transactionShape: TransactionShape,
      verbose: Boolean,
  ): Future[Transaction] =
    withTimeout(
      s"Exercise for party $party",
      delegate.exercise(party, exercise, transactionShape, verbose = verbose),
    )
  override def exercise[T](
      actAs: List[Party],
      readAs: List[Party],
      exercise: Update[T],
  ): Future[Transaction] = withTimeout(
    s"Exercise for actAs $actAs and readAs $readAs",
    delegate.exercise(actAs, readAs, exercise),
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
  ): Future[Transaction] = withTimeout(
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
  override def submitAndWaitForTransactionRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
      transactionShape: TransactionShape,
  ): SubmitAndWaitForTransactionRequest =
    delegate.submitAndWaitForTransactionRequest(actAs, readAs, commands, transactionShape)
  override def submitAndWaitRequest(
      party: Party,
      commands: JList[Command],
  ): SubmitAndWaitRequest = delegate.submitAndWaitRequest(party, commands)

  override def submitAndWaitForTransactionRequest(
      party: Party,
      commands: JList[Command],
  ): SubmitAndWaitForTransactionRequest =
    delegate.submitAndWaitForTransactionRequest(party, commands)

  def prepareSubmissionRequest(party: Party, commands: JList[Command]): PrepareSubmissionRequest =
    delegate.prepareSubmissionRequest(party, commands)

  def executeSubmissionRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
  ): ExecuteSubmissionRequest =
    delegate.executeSubmissionRequest(party, preparedTx)

  def submitAndWaitForTransactionRequest(
      party: Party,
      commands: JList[Command],
      transactionShape: TransactionShape,
      filterParties: Option[Seq[Party]],
      templateIds: Seq[Identifier],
      verbose: Boolean,
  ): SubmitAndWaitForTransactionRequest =
    delegate.submitAndWaitForTransactionRequest(
      party = party,
      commands = commands,
      transactionShape = transactionShape,
      filterParties = filterParties,
      templateIds = templateIds,
      verbose = verbose,
    )

  override def submit(request: SubmitRequest): Future[Unit] =
    withTimeout(s"Submit for request $request", delegate.submit(request))
  override def submitAndWait(request: SubmitAndWaitRequest): Future[SubmitAndWaitResponse] =
    withTimeout(
      s"Submit and wait for request $request",
      delegate.submitAndWait(request),
    )
  override def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse] = withTimeout(
    s"Submit and wait for transaction request $request",
    delegate.submitAndWaitForTransaction(request),
  )
  override def submitRequestAndTolerateGrpcError[T](
      errorToTolerate: ErrorCode,
      submitAndWaitGeneric: ParticipantTestContext => Future[T],
  ): Future[T] = // timeout enforced by submitAndWaitGeneric
    delegate.submitRequestAndTolerateGrpcError(errorToTolerate, submitAndWaitGeneric)

  override def completions(
      within: NonNegativeFiniteDuration,
      request: CompletionStreamRequest,
  ): Future[Vector[CompletionStreamResponse.CompletionResponse]] =
    delegate.completions(within, request)

  override def completionStreamRequest(from: Long)(
      parties: Party*
  ): CompletionStreamRequest = delegate.completionStreamRequest(from)(parties*)

  override def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]] =
    withTimeout(
      s"First completions for request $request",
      delegate.firstCompletions(request),
    )
  override def firstCompletions(parties: Party*): Future[Vector[Completion]] =
    withTimeout(
      s"First completions for parties $parties",
      delegate.firstCompletions(parties*),
    )
  override def findCompletionAtOffset(offset: Long, p: Completion => Boolean)(
      parties: Party*
  ): Future[Option[Completion]] = withTimeout(
    s"Find completion at offset $offset for parties $parties",
    delegate.findCompletionAtOffset(offset, p)(parties*),
  )
  override def findCompletion(request: CompletionStreamRequest)(
      p: Completion => Boolean
  ): Future[Option[Completion]] = withTimeout(
    s"Find completion for request $request",
    delegate.findCompletion(request)(p),
  )
  override def findCompletion(parties: Party*)(
      p: Completion => Boolean
  ): Future[Option[Completion]] =
    withTimeout(s"Find completion for parties $parties", delegate.findCompletion(parties*)(p))
  override def offsets(n: Int, request: CompletionStreamRequest): Future[Vector[Long]] =
    withTimeout(s"$n checkpoints for request $request", delegate.offsets(n, request))
  override def checkHealth(): Future[HealthCheckResponse] =
    withTimeout("Check health", delegate.checkHealth())
  override def watchHealth(): Future[Seq[HealthCheckResponse]] =
    withTimeout("Watch health", delegate.watchHealth())

  private[infrastructure] override def preallocateParties(
      n: Int,
      participants: Iterable[ParticipantTestContext],
      connectedSynchronizers: Int,
  ): Future[Vector[Party]] = withTimeout(
    s"Preallocate $n parties on participants ${participants.map(_.ledgerEndpoint)}",
    delegate.preallocateParties(n, participants, connectedSynchronizers),
  )

  override def getConnectedSynchronizers(
      party: Option[Party],
      participantId: Option[String],
      identityProviderId: Option[String] = None,
  ): Future[Set[String]] = withTimeout(
    s"Querying connected synchronizers of a party $party",
    delegate.getConnectedSynchronizers(party, participantId, identityProviderId),
  )

  override def prune(
      pruneUpTo: Long,
      attempts: Int,
  ): Future[PruneResponse] = withTimeout(
    s"Prune up to $pruneUpTo, with $attempts attempts",
    delegate.prune(pruneUpTo, attempts),
  )

  override def pruneCantonSafe(
      pruneUpTo: Long,
      party: Party,
      dummyCommand: Party => JList[Command],
  )(implicit ec: ExecutionContext): Future[Unit] =
    delegate.pruneCantonSafe(pruneUpTo, party, dummyCommand)

  private def withTimeout[T](hint: String, future: => Future[T]): Future[T] =
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

  override def latestPrunedOffsets(): Future[(Long, Long)] = withTimeout(
    "Requesting the latest pruned offsets",
    delegate.latestPrunedOffsets(),
  )

  override def maxOffsetCheckpointEmissionDelay: NonNegativeFiniteDuration =
    delegate.maxOffsetCheckpointEmissionDelay

  override def executeSubmissionAndWaitRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
  ): ExecuteSubmissionAndWaitRequest =
    delegate.executeSubmissionAndWaitRequest(party, preparedTx)

  def executeSubmissionAndWaitForTransactionRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
      transactionFormat: Option[TransactionFormat],
  ): ExecuteSubmissionAndWaitForTransactionRequest =
    delegate.executeSubmissionAndWaitForTransactionRequest(party, preparedTx, transactionFormat)
}
