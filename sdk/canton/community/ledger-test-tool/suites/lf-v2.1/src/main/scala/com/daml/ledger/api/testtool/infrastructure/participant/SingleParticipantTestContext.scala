// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.testtool.infrastructure.ChannelEndpoint.JsonApiEndpoint
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext.IncludeInterfaceView
import com.daml.ledger.api.testtool.infrastructure.time.{
  DelayMechanism,
  StaticTimeDelayMechanism,
  TimeDelayMechanism,
}
import com.daml.ledger.api.testtool.infrastructure.{
  ChannelEndpoint,
  ExternalParty,
  FutureAssertions,
  Identification,
  LedgerServices,
  Party,
  PartyAllocationConfiguration,
  RetryingGetConnectedSynchronizersForParty,
}
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UpdateVettedPackagesRequest,
  UpdateVettedPackagesResponse,
  UploadDarFileRequest,
  ValidateDarFileRequest,
}
import com.daml.ledger.api.v2.admin.participant_pruning_service.{PruneRequest, PruneResponse}
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
import com.daml.ledger.api.v2.commands.{Command as ApiCommand, Commands}
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.contract_service.GetContractRequest
import com.daml.ledger.api.v2.event.Event.Event.Created
import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
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
  GetPreferredPackageVersionRequest,
  GetPreferredPackageVersionResponse,
  GetPreferredPackagesRequest,
  GetPreferredPackagesResponse,
  HashingSchemeVersion,
  PackageVettingRequirement,
  PartySignatures,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
  SinglePartySignatures,
}
import com.daml.ledger.api.v2.package_service.*
import com.daml.ledger.api.v2.state_service.*
import com.daml.ledger.api.v2.testing.time_service.{GetTimeRequest, SetTimeRequest}
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.update_service.*
import com.daml.ledger.api.v2.{crypto as lapicrypto, value as v1}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, ContractId, Exercised, Update}
import com.daml.ledger.javaapi.data.{Command, ExerciseByKeyCommand, Identifier, Template, Value}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.timer.Delayed
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects, toProto}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.canton.util.{MonadUtil, OptionUtil}
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse}
import io.grpc.protobuf.StatusProto
import io.grpc.stub.StreamObserver
import io.scalaland.chimney.dsl.*

import java.security.{KeyPair, KeyPairGenerator, Signature}
import java.time.{Clock, Instant}
import java.util.List as JList
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Exposes services running on some participant server in a test case.
  *
  * Each time a test case is run it receives a fresh instance of [[SingleParticipantTestContext]]
  * (one for every used participant server).
  */
final class SingleParticipantTestContext private[participant] (
    val endpointId: String,
    val userId: String,
    identifierSuffix: String,
    val referenceOffset: Long,
    protected[participant] val services: LedgerServices,
    partyAllocationConfig: PartyAllocationConfiguration,
    val ledgerEndpoint: Either[JsonApiEndpoint, ChannelEndpoint],
    val adminEndpoint: ChannelEndpoint,
    val features: Features,
    val participantId: String,
)(protected[participant] implicit val ec: ExecutionContext)
    extends ParticipantTestContext {
  private val logger = ContextualizedLogger.get(getClass)

  private[this] val identifierPrefix =
    s"$userId-$endpointId-$identifierSuffix"

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
  private[this] val workflowId: String = s"$userId-$identifierSuffix"
  override val nextKeyId: () => String = nextIdGenerator("key")
  override val nextUserId: () => String = nextIdGenerator("user", lowerCase = true)
  override val nextPartyId: () => String = nextIdGenerator("party", lowerCase = true)
  override val nextIdentityProviderId: () => String = nextIdGenerator("idp", lowerCase = true)

  override lazy val delayMechanism: DelayMechanism = if (features.staticTime) {
    new StaticTimeDelayMechanism(this)
  } else
    new TimeDelayMechanism()

  override def toString: String = s"participant $endpointId"

  override def currentEnd(): Future[Long] =
    services.state
      .getLedgerEnd(new GetLedgerEndRequest())
      .map(_.offset)

  override def latestPrunedOffsets(): Future[(Long, Long)] =
    services.state
      .getLatestPrunedOffsets(GetLatestPrunedOffsetsRequest())
      .map(response =>
        response.participantPrunedUpToInclusive -> response.allDivulgedContractsPrunedUpToInclusive
      )

  override def offsetBeyondLedgerEnd(): Future[Long] =
    currentEnd().map(_ + 1000000L)

  override def time(): Future[Instant] =
    services.time
      .getTime(new GetTimeRequest())
      .map(_.getCurrentTime.asJava)
      .recover { case NonFatal(_) =>
        Clock.systemUTC().instant()
      }

  override def setTime(currentTime: Instant, newTime: Instant): Future[Unit] =
    services.time
      .setTime(
        SetTimeRequest(
          currentTime = Some(currentTime.asProtobuf),
          newTime = Some(newTime.asProtobuf),
        )
      )
      .map(_ => ())

  override def listKnownPackages(): Future[Seq[PackageDetails]] =
    services.packageManagement
      .listKnownPackages(new ListKnownPackagesRequest)
      .map(_.packageDetails)

  override def validateDarFile(request: ValidateDarFileRequest): Future[Unit] =
    services.packageManagement.validateDarFile(request).map(_ => ())

  override def uploadDarRequest(bytes: ByteString, synchronizerId: String): UploadDarFileRequest =
    new UploadDarFileRequest(
      bytes,
      nextSubmissionId(),
      UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES,
      synchronizerId,
    )

  override def uploadDarFile(request: UploadDarFileRequest): Future[Unit] =
    services.packageManagement
      .uploadDarFile(request)
      .map(_ => ())

  override def listVettedPackages(
      request: ListVettedPackagesRequest
  ): Future[ListVettedPackagesResponse] =
    services.packages
      .listVettedPackages(request)

  override def updateVettedPackages(
      request: UpdateVettedPackagesRequest
  ): Future[UpdateVettedPackagesResponse] =
    services.packageManagement
      .updateVettedPackages(request)

  override def getParticipantId(): Future[String] =
    services.partyManagement
      .getParticipantId(new GetParticipantIdRequest)
      .map(_.participantId)

  override def listPackages(): Future[Seq[String]] =
    services.packages
      .listPackages(new ListPackagesRequest())
      .map(_.packageIds)

  override def getPackage(packageId: String): Future[GetPackageResponse] =
    services.packages.getPackage(new GetPackageRequest(packageId))

  override def getPackageStatus(packageId: String): Future[PackageStatus] =
    services.packages
      .getPackageStatus(new GetPackageStatusRequest(packageId))
      .map(_.packageStatus)

  override def prepareSubmission(
      prepareSubmissionRequest: PrepareSubmissionRequest
  ): Future[PrepareSubmissionResponse] =
    services.interactiveSubmission.prepareSubmission(prepareSubmissionRequest)

  def executeSubmission(
      executeSubmissionRequest: ExecuteSubmissionRequest
  ): Future[ExecuteSubmissionResponse] =
    services.interactiveSubmission.executeSubmission(executeSubmissionRequest)

  def executeSubmissionAndWait(
      executeSubmissionAndWaitRequest: ExecuteSubmissionAndWaitRequest
  ): Future[ExecuteSubmissionAndWaitResponse] =
    services.interactiveSubmission.executeSubmissionAndWait(executeSubmissionAndWaitRequest)

  def executeSubmissionAndWaitForTransaction(
      executeSubmissionAndWaitForTransactionRequest: ExecuteSubmissionAndWaitForTransactionRequest
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
    services.interactiveSubmission.executeSubmissionAndWaitForTransaction(
      executeSubmissionAndWaitForTransactionRequest
    )

  override def getPreferredPackageVersion(
      parties: Seq[Party],
      packageName: String,
      vettingValidAt: Option[Instant] = None,
      synchronizerIdO: Option[String] = None,
  ): Future[GetPreferredPackageVersionResponse] =
    services.interactiveSubmission.getPreferredPackageVersion(
      new GetPreferredPackageVersionRequest(
        parties = parties.map(_.getValue),
        packageName = packageName,
        synchronizerId = synchronizerIdO.getOrElse(""),
        vettingValidAt = vettingValidAt.map(_.asProtobuf),
      )
    )

  override def getPreferredPackages(
      vettingRequirements: Map[String, Seq[Party]],
      vettingValidAt: Option[Instant] = None,
      synchronizerIdO: Option[String] = None,
  ): Future[GetPreferredPackagesResponse] =
    services.interactiveSubmission.getPreferredPackages(
      new GetPreferredPackagesRequest(
        packageVettingRequirements = vettingRequirements.view.map { case (packageName, parties) =>
          PackageVettingRequirement(parties = parties.map(_.getValue), packageName = packageName)
        }.toSeq,
        synchronizerId = synchronizerIdO.getOrElse(""),
        vettingValidAt = vettingValidAt.map(_.asProtobuf),
      )
    )

  override def allocateParty(): Future[Party] =
    allocateParty(partyIdHint = Some(nextPartyHintId()))

  override def generateExternalPartyTopologyRequest(
      namespacePublicKey: Array[Byte],
      partyIdHint: Option[String] = None,
  ): Future[GenerateExternalPartyTopologyResponse] =
    for {
      syncIds <- getConnectedSynchronizers(None, None)
      syncId = syncIds.headOption.getOrElse(throw new Exception("No synchronizer connected"))
      onboardingTransactions <- generateExternalPartyTopology(
        GenerateExternalPartyTopologyRequest(
          synchronizer = syncId,
          partyHint = partyIdHint.getOrElse(nextPartyHintId()),
          publicKey = Some(
            lapicrypto.SigningPublicKey(
              format =
                lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
              keyData = ByteString.copyFrom(namespacePublicKey),
              keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
            )
          ),
          localParticipantObservationOnly = false,
          otherConfirmingParticipantUids = Seq(),
          confirmationThreshold = 1,
          observingParticipantUids = Seq(),
        )
      )
    } yield onboardingTransactions

  override def allocateExternalPartyRequest(
      keyPair: KeyPair,
      partyIdHint: Option[String] = None,
      synchronizer: String = "",
  ): Future[AllocateExternalPartyRequest] = {
    val signing = Signature.getInstance("Ed25519")
    signing.initSign(keyPair.getPrivate)
    for {
      onboardingTransactions <- generateExternalPartyTopologyRequest(
        keyPair.getPublic.getEncoded,
        partyIdHint,
      )
    } yield {
      signing.update(onboardingTransactions.multiHash.toByteArray)
      AllocateExternalPartyRequest(
        synchronizer = synchronizer,
        onboardingTransactions = onboardingTransactions.topologyTransactions.map { transaction =>
          AllocateExternalPartyRequest.SignedTransaction(
            transaction,
            Seq.empty,
          )
        },
        multiHashSignatures = Seq(
          lapicrypto.Signature(
            format = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
            signature = ByteString.copyFrom(signing.sign()),
            signedBy = onboardingTransactions.publicKeyFingerprint,
            signingAlgorithmSpec = lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
          )
        ),
        identityProviderId = "",
      )
    }
  }

  override def allocateExternalParty(
      req: AllocateExternalPartyRequest,
      minSynchronizers: Option[Int] = None,
  ): Future[Party] =
    for {
      result <- services.partyManagement.allocateExternalParty(req)
      synchronizerIds <- RetryingGetConnectedSynchronizersForParty(
        services,
        result.partyId,
        minSynchronizers.getOrElse(1),
      )
    } yield Party(result.partyId, synchronizerIds.toList)

  override def allocateExternalPartyFromHint(
      partyIdHint: Option[String],
      minSynchronizers: Int,
  ): Future[ExternalParty] = {
    val keyGen = KeyPairGenerator.getInstance("Ed25519")
    val keyPair = keyGen.generateKeyPair()
    for {
      connectedSynchronizerIds <- connectedSynchronizers()
      result <- MonadUtil.foldLeftM[Future, Option[AllocateExternalPartyResponse], String](
        None,
        connectedSynchronizerIds,
      ) { case (previousResponse, synchronizerId) =>
        val previouslyAllocatedPartyIdHint =
          previousResponse
            .map(_.partyId)
            .map(PartyId.tryFromProtoPrimitive)
            .map(_.identifier.unwrap)
        allocateExternalPartyRequest(
          keyPair,
          partyIdHint
            // otherwise use the dynamically generated party id as the party id hint, to allocate
            // the same party across all synchronizers
            .orElse(previouslyAllocatedPartyIdHint),
          synchronizer = synchronizerId,
        ).flatMap(services.partyManagement.allocateExternalParty)
          .map(Some(_))
      }
    } yield {
      val partyId = result.getOrElse(sys.error("Party missing in response")).partyId
      Party.external(
        partyId,
        UniqueIdentifier.tryFromProtoPrimitive(partyId).fingerprint,
        keyPair,
        signingThreshold = PositiveInt.one,
        connectedSynchronizerIds.toList,
      )
    }
  }

  override def allocateParty(
      partyIdHint: Option[String] = None,
      localMetadata: Option[ObjectMeta] = None,
      identityProviderId: Option[String] = None,
      minSynchronizers: Option[Int] = None,
      userId: String = "",
  ): Future[Party] =
    for {
      (result, synchronizerIds) <- allocateParty(
        new AllocatePartyRequest(
          partyIdHint = partyIdHint.getOrElse(""),
          localMetadata = localMetadata,
          identityProviderId = identityProviderId.getOrElse(""),
          synchronizerId = None.getOrElse(""),
          userId = userId,
        ),
        minSynchronizers.getOrElse(1),
      )
    } yield Party(
      result.partyDetails.getOrElse(sys.error("Party details not populated")).party,
      synchronizerIds.toList,
    )

  override def allocateParty(
      req: AllocatePartyRequest,
      minSynchronizers: Int,
  ): Future[(AllocatePartyResponse, Seq[String])] =
    for {
      connectedSynchronizerIds <- connectedSynchronizers()
      result <- MonadUtil.foldLeftM[Future, Option[AllocatePartyResponse], String](
        None,
        connectedSynchronizerIds,
      ) { case (previousResponse, synchronizerId) =>
        val previouslyAllocatedPartyIdHint =
          previousResponse
            .flatMap(_.partyDetails)
            .map(_.party)
            .map(PartyId.tryFromProtoPrimitive)
            .map(_.identifier.unwrap)
        services.partyManagement
          .allocateParty(
            req.copy(
              partyIdHint = OptionUtil
                // if the original request contained a partyIdHint, use it
                .emptyStringAsNone(req.partyIdHint)
                // otherwise use the dynamically generated party id as the party id hint, to allocate
                // the same party across all synchronizers
                .orElse(previouslyAllocatedPartyIdHint)
                .getOrElse(""),
              synchronizerId = synchronizerId,
            )
          )
          .map(Some(_))
      }
      allocatePartyResponse = result.getOrElse(sys.error("Allocate party response is empty"))
      synchronizerIdsForParty <- RetryingGetConnectedSynchronizersForParty(
        services,
        allocatePartyResponse.partyDetails
          .getOrElse(sys.error("Party details not populated"))
          .party,
        minSynchronizers,
      )
    } yield (allocatePartyResponse, synchronizerIdsForParty)

  override def connectedSynchronizers(): Future[Seq[String]] = for {
    participantId <- services.partyManagement.getParticipantId(GetParticipantIdRequest())
    synchronizerIds <- RetryingGetConnectedSynchronizersForParty(
      services,
      participantId.participantId,
      1,
    )
  } yield synchronizerIds

  override def updatePartyDetails(
      req: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] =
    services.partyManagement.updatePartyDetails(req)

  def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderIdRequest
  ): Future[UpdatePartyIdentityProviderIdResponse] =
    services.partyManagement.updatePartyIdentityProviderId(request)

  override def allocateParties(partiesCount: Int, minSynchronizers: Int): Future[Vector[Party]] = {
    MonadUtil.parTraverseWithLimit(
      PositiveInt.tryCreate(16)
    )(1 to partiesCount) { _ =>
      allocateParty(
        partyIdHint = Some(nextPartyHintId()),
        localMetadata = None,
        identityProviderId = None,
        minSynchronizers = Some(minSynchronizers),
      )
    }
  }.map(_.toVector)

  override def allocateExternalParties(
      partiesCount: Int,
      minSynchronizers: Int,
  ): Future[Vector[ExternalParty]] =
    Future.sequence(
      Vector.fill(partiesCount)(
        allocateExternalPartyFromHint(
          partyIdHint = Some(nextPartyHintId()),
          minSynchronizers,
        )
      )
    )

  override def getParties(req: GetPartiesRequest): Future[GetPartiesResponse] =
    services.partyManagement.getParties(req)

  override def getParties(parties: Seq[Party]): Future[Seq[PartyDetails]] =
    services.partyManagement
      .getParties(GetPartiesRequest(parties.map(_.getValue), ""))
      .map(_.partyDetails)

  override def listKnownPartiesExpanded(): Future[Set[Party]] =
    services.partyManagement
      .listKnownParties(ListKnownPartiesRequest("", 0, "", filterParty = ""))
      .map(_.partyDetails.map(partyDetails => Party(partyDetails.party)).toSet)

  override def listKnownParties(req: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    services.partyManagement
      .listKnownParties(req)

  override def listKnownParties(): Future[ListKnownPartiesResponse] =
    services.partyManagement
      .listKnownParties(new ListKnownPartiesRequest("", 0, "", filterParty = ""))

  override def waitForPartiesOnOtherParticipants(
      otherParticipants: Iterable[ParticipantTestContext],
      expectedParties: Set[Party],
      connectedSynchronizers: Int,
  ): Future[Unit] =
    if (partyAllocationConfig.waitForAllParticipants) {
      val expectedPartyNames = expectedParties.map(_.underlying.getValue)
      Future
        .sequence(otherParticipants.filter(_.endpointId != endpointId).map { participant =>
          for {
            _ <- eventually(s"Wait for parties on ${participant.endpointId}") {
              participant
                .listKnownPartiesExpanded()
                .map { actualParties =>
                  assert(
                    expectedPartyNames.subsetOf(actualParties.map(_.underlying.getValue)),
                    s"Parties from $endpointId never appeared on ${participant.endpointId}.",
                  )
                }
            }
            _ <- Future.sequence(expectedParties.map { party =>
              eventually(
                s"Wait for synchronizers for ${party.underlying.getValue} on ${participant.endpointId}"
              )(
                participant
                  .getConnectedSynchronizers(Some(party), Some(participantId))
                  .map(synchronizers =>
                    assert(
                      synchronizers.sizeIs == connectedSynchronizers,
                      s"Expecting party ${party.underlying.getValue} created on $endpointId to be connected to $connectedSynchronizers synchronizers on ${participant.endpointId} but found ${synchronizers.size}",
                    )
                  )
              )
            })
          } yield ()
        })
        .map(_ => ())
    } else
      Future.unit

  override def generateExternalPartyTopology(
      req: GenerateExternalPartyTopologyRequest
  ): Future[GenerateExternalPartyTopologyResponse] =
    services.partyManagement.generateExternalPartyTopology(req)

  override def activeContracts(
      request: GetActiveContractsRequest
  ): Future[Vector[CreatedEvent]] =
    for {
      contracts <- new StreamConsumer[GetActiveContractsResponse](
        services.state.getActiveContracts(request, _)
      ).all()
    } yield contracts.flatMap(_.contractEntry.activeContract.flatMap(_.createdEvent))

  override def activeContractsRequest(
      parties: Option[Seq[Party]],
      activeAtOffset: Long,
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      verbose: Boolean = true,
  ): GetActiveContractsRequest =
    GetActiveContractsRequest(
      activeAtOffset = activeAtOffset,
      eventFormat = Some(eventFormat(verbose, parties, templateIds, interfaceFilters)),
    )

  override def activeContracts(
      parties: Option[Seq[Party]],
      activeAtOffsetO: Option[Long] = None,
      verbose: Boolean = true,
  ): Future[Vector[CreatedEvent]] =
    activeContractsByTemplateId(Seq.empty, parties, activeAtOffsetO, verbose)

  override def activeContractsByTemplateId(
      templateIds: Seq[Identifier],
      parties: Option[Seq[Party]],
      activeAtOffsetO: Option[Long],
      verbose: Boolean = true,
  ): Future[Vector[CreatedEvent]] =
    for {
      activeAtOffset <- activeAtOffsetO match {
        case None => currentEnd()
        case Some(activeAt) => Future.successful(activeAt)
      }
      acs <- activeContracts(
        activeContractsRequest(
          parties,
          activeAtOffset,
          templateIds,
          verbose = verbose,
        )
      )
    } yield acs

  override def contract(
      queryingParties: Option[Seq[Party]],
      contractId: String,
  ): Future[Option[CreatedEvent]] =
    services.contract
      .getContract(
        GetContractRequest(
          contractId = contractId,
          queryingParties = queryingParties.getOrElse(Set.empty).toList.map(_.underlying.getValue),
        )
      )
      .map(_.createdEvent)

  def eventFormat(
      verbose: Boolean,
      partiesO: Option[Seq[Party]],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
  ): EventFormat = {
    lazy val fs = filters(templateIds, interfaceFilters)
    partiesO match {
      case None =>
        EventFormat(
          filtersByParty = Map.empty,
          filtersForAnyParty = Some(fs),
          verbose = verbose,
        )
      case Some(parties) =>
        EventFormat(
          filtersByParty = parties.map(party => party.getValue -> fs).toMap,
          filtersForAnyParty = None,
          verbose = verbose,
        )
    }
  }

  override def transactionFormat(
      parties: Option[Seq[Party]],
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
      transactionShape: TransactionShape = AcsDelta,
      verbose: Boolean = false,
  ): TransactionFormat =
    TransactionFormat(
      eventFormat = Some(
        eventFormat(
          verbose = verbose,
          partiesO = parties,
          templateIds = templateIds,
          interfaceFilters = interfaceFilters,
        )
      ),
      transactionShape = toProto(transactionShape),
    )

  override def filters(
      templateIds: Seq[Identifier] = Seq.empty,
      interfaceFilters: Seq[(Identifier, IncludeInterfaceView)] = Seq.empty,
  ): Filters = Filters(
    if (templateIds.isEmpty && interfaceFilters.isEmpty)
      Seq(
        CumulativeFilter.defaultInstance.withWildcardFilter(
          WildcardFilter(includeCreatedEventBlob = false)
        )
      )
    else
      interfaceFilters
        .map { case (id, includeInterfaceView) =>
          CumulativeFilter(
            IdentifierFilter.InterfaceFilter(
              InterfaceFilter(
                interfaceId = Some(v1.Identifier.fromJavaProto(id.toProto)),
                includeInterfaceView = includeInterfaceView,
                includeCreatedEventBlob = false,
              )
            )
          )
        }
        ++
          templateIds
            .map(tid =>
              CumulativeFilter(
                IdentifierFilter.TemplateFilter(
                  TemplateFilter(
                    templateId = Some(v1.Identifier.fromJavaProto(tid.toProto)),
                    includeCreatedEventBlob = false,
                  )
                )
              )
            )
  )

  override def getTransactionsRequest(
      transactionFormat: TransactionFormat,
      begin: Long = referenceOffset,
  ): Future[GetUpdatesRequest] = currentEnd().map { end =>
    getTransactionsRequestWithEnd(
      transactionFormat = transactionFormat,
      begin = begin,
      end = Some(end),
    )
  }

  override def getTransactionsRequestWithEnd(
      transactionFormat: TransactionFormat,
      begin: Long = referenceOffset,
      end: Option[Long],
  ): GetUpdatesRequest = getUpdatesRequestWithEnd(
    transactionFormatO = Some(transactionFormat),
    begin = begin,
    end = end,
  )

  override def getUpdatesRequestWithEnd(
      transactionFormatO: Option[TransactionFormat] = None,
      reassignmentsFormatO: Option[EventFormat] = None,
      topologyFilterO: Option[Seq[Party]] = None,
      begin: Long = referenceOffset,
      end: Option[Long] = None,
  ): GetUpdatesRequest = {

    val includeTopologyTransactions: Option[TopologyFormat] =
      topologyFilterO.map(parties =>
        TopologyFormat(
          Some(
            ParticipantAuthorizationTopologyFormat(
              parties.map(_.underlying.getValue)
            )
          )
        )
      )

    GetUpdatesRequest(
      beginExclusive = begin,
      endInclusive = end,
      updateFormat = Some(
        UpdateFormat(
          includeTransactions = transactionFormatO,
          includeReassignments = reassignmentsFormatO,
          includeTopologyEvents = includeTopologyTransactions,
        )
      ),
    )
  }

  private def transactions(
      n: Int,
      request: GetUpdatesRequest,
      service: (GetUpdatesRequest, StreamObserver[GetUpdatesResponse]) => Unit,
  ): Future[Vector[GetUpdatesResponse]] =
    new StreamConsumer[GetUpdatesResponse](service(request, _))
      .filterTake(_.update.isTransaction)(n)

  private def updates[Res](
      n: Int,
      request: GetUpdatesRequest,
      service: (GetUpdatesRequest, StreamObserver[Res]) => Unit,
      resultFilter: Res => Boolean,
  ): Future[Vector[Res]] =
    new StreamConsumer[Res](service(request, _)).filterTake(resultFilter)(n)

  private def transactions[Res](
      request: GetUpdatesRequest,
      service: (GetUpdatesRequest, StreamObserver[Res]) => Unit,
  ): Future[Vector[Res]] =
    new StreamConsumer[Res](service(request, _)).all()

  override def transactionsByTemplateId(
      templateId: Identifier,
      parties: Option[Seq[Party]],
  ): Future[Vector[Transaction]] =
    getTransactionsRequest(transactionFormat(parties, Seq(templateId)))
      .flatMap(transactions)

  override def transactions(
      request: GetUpdatesRequest
  ): Future[Vector[Transaction]] =
    transactions(request, services.update.getUpdates)
      .map(_.flatMap(_.update.transaction))

  override def transactions(
      transactionShape: TransactionShape,
      parties: Party*
  ): Future[Vector[Transaction]] =
    getTransactionsRequest(
      transactionFormat =
        transactionFormat(Some(parties), transactionShape = transactionShape, verbose = true)
    ).flatMap(transactions)

  override def transactions(
      take: Int,
      request: GetUpdatesRequest,
  ): Future[Vector[Transaction]] =
    transactions(take, request, services.update.getUpdates)
      .map(_.flatMap(_.update.transaction))

  override def updates(
      take: Int,
      request: GetUpdatesRequest,
  ): Future[Vector[GetUpdatesResponse.Update]] =
    updates(take, request, services.update.getUpdates, (_: GetUpdatesResponse) => true)
      .map(_.map(_.update))

  override def updates(
      take: Int,
      request: GetUpdatesRequest,
      resultFilter: GetUpdatesResponse => Boolean,
  ): Future[Vector[GetUpdatesResponse.Update]] =
    updates(take, request, services.update.getUpdates, resultFilter)
      .map(_.map(_.update))

  override def updates(
      within: NonNegativeFiniteDuration,
      request: GetUpdatesRequest,
  ): Future[Vector[GetUpdatesResponse.Update]] =
    new StreamConsumer(services.update.getUpdates(request, _))
      .within(within.toScala)
      .map(_.map(_.update))

  override def transactions(
      take: Int,
      transactionShape: TransactionShape,
      parties: Party*
  ): Future[Vector[Transaction]] =
    getTransactionsRequest(transactionFormat(Some(parties), transactionShape = transactionShape))
      .flatMap(txReq => transactions(take, txReq))

  override def transactionTreeById(
      updateId: String,
      parties: Party*
  ): Future[Transaction] = {
    val partiesList = parties.toList.headOption.map(_ => parties.toList)
    services.update
      .getUpdateById(
        GetUpdateByIdRequest(
          updateId = updateId,
          updateFormat = Some(
            UpdateFormat(
              includeTransactions =
                Some(transactionFormat(parties = partiesList, transactionShape = LedgerEffects)),
              includeReassignments = None,
              includeTopologyEvents = None,
            )
          ),
        )
      )
      .map(_.getTransaction)
  }

  override def updateById(request: GetUpdateByIdRequest): Future[GetUpdateResponse] =
    services.update.getUpdateById(request)

  override def transactionById(
      updateId: String,
      parties: Seq[Party],
      transactionShape: TransactionShape = AcsDelta,
      templateIds: Seq[Identifier] = Seq.empty,
  ): Future[Transaction] =
    updateById(
      GetUpdateByIdRequest(
        updateId = updateId,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              transactionFormat(
                parties = Some(parties),
                templateIds = templateIds,
                transactionShape = transactionShape,
                verbose = true,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
      )
    ).map(_.getTransaction)

  override def topologyTransactionById(
      updateId: String,
      parties: Seq[Party],
  ): Future[TopologyTransaction] =
    updateById(
      GetUpdateByIdRequest(
        updateId = updateId,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = None,
            includeTopologyEvents = Some(
              TopologyFormat(includeParticipantAuthorizationEvents =
                Some(ParticipantAuthorizationTopologyFormat(parties.map(_.getValue)))
              )
            ),
          )
        ),
      )
    ).map(_.getTopologyTransaction)

  override def transactionTreeByOffset(offset: Long, parties: Party*): Future[Transaction] = {
    val partiesList = parties.toList.headOption.map(_ => parties.toList)
    services.update
      .getUpdateByOffset(
        GetUpdateByOffsetRequest(
          offset = offset,
          updateFormat = Some(
            UpdateFormat(
              includeTransactions =
                Some(transactionFormat(parties = partiesList, transactionShape = LedgerEffects)),
              includeReassignments = None,
              includeTopologyEvents = None,
            )
          ),
        )
      )
      .map(_.getTransaction)
  }
  override def updateByOffset(
      request: GetUpdateByOffsetRequest
  ): Future[GetUpdateResponse] =
    services.update
      .getUpdateByOffset(request)

  override def transactionByOffset(
      offset: Long,
      parties: Seq[Party],
      transactionShape: TransactionShape,
      templateIds: Seq[Identifier] = Seq.empty,
  ): Future[Transaction] =
    updateByOffset(
      GetUpdateByOffsetRequest(
        offset = offset,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              transactionFormat(
                parties = Some(parties),
                templateIds = templateIds,
                transactionShape = transactionShape,
                verbose = true,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
      )
    ).map(_.getTransaction)

  override def topologyTransactionByOffset(
      offset: Long,
      parties: Seq[Party],
  ): Future[TopologyTransaction] =
    updateByOffset(
      GetUpdateByOffsetRequest(
        offset = offset,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = None,
            includeTopologyEvents = Some(
              TopologyFormat(includeParticipantAuthorizationEvents =
                Some(ParticipantAuthorizationTopologyFormat(parties.map(_.getValue)))
              )
            ),
          )
        ),
      )
    ).map(_.getTopologyTransaction)

  private def extractContracts[TCid <: ContractId[?]](transaction: Transaction)(implicit
      companion: ContractCompanion[?, TCid, ?]
  ): Seq[TCid] =
    transaction.events.collect { case Event(Created(e)) =>
      companion.toContractId(new ContractId(e.contractId))
    }

  override def getEventsByContractId(
      request: GetEventsByContractIdRequest
  ): Future[GetEventsByContractIdResponse] =
    services.eventQuery.getEventsByContractId(request)

  override def create[
      TCid <: ContractId[T],
      T <: Template,
  ](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] =
    submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(party, template.create.commands, AcsDelta)
    )
      .map(response =>
        extractContracts(response.getTransaction).headOption
          .getOrElse(sys.error("Expected at least one contract"))
      )

  override def create[TCid <: ContractId[T], T <: Template](
      actAs: List[Party],
      readAs: List[Party],
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] =
    submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(actAs, readAs, template.create.commands, AcsDelta)
    ).map(response =>
      extractContracts(response.getTransaction).headOption
        .getOrElse(sys.error("Expected at least one contract"))
    )

  override def createAndGetUpdateId[
      TCid <: ContractId[T],
      T <: Template,
  ](
      party: Party,
      template: T,
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[(String, TCid)] =
    submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(party, template.create.commands)
    )
      .map(_.getTransaction)
      .map(tx =>
        tx.updateId -> tx.events
          .collectFirst { case Event(Created(e)) =>
            companion.toContractId(new ContractId(e.contractId))
          }
          .getOrElse(sys.error("Expected at least one contract"))
      )

  override def exercise[T](
      party: Party,
      exercise: Update[T],
      transactionShape: TransactionShape = LedgerEffects,
      verbose: Boolean = true,
  ): Future[Transaction] =
    submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(
        party,
        exercise.commands,
        transactionShape,
        verbose = verbose,
      )
    ).map(_.getTransaction)

  override def exercise[T](
      actAs: List[Party],
      readAs: List[Party],
      exercise: Update[T],
  ): Future[Transaction] =
    submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(
        actAs,
        readAs,
        exercise.commands,
        transactionShape = LedgerEffects,
      )
    ).map(_.getTransaction)

  override def exerciseAndGetContract[TCid <: ContractId[T], T](
      party: Party,
      exercise: Update[Exercised[TCid]],
  )(implicit companion: ContractCompanion[?, TCid, T]): Future[TCid] =
    submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(party, exercise.commands)
    )
      .map(_.getTransaction)
      .map(t =>
        extractContracts(t)(companion).headOption
          .getOrElse(sys.error("Expected at least one contract"))
      )

  override def exerciseByKey(
      party: Party,
      template: Identifier,
      key: Value,
      choice: String,
      argument: Value,
  ): Future[Transaction] =
    submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(
        party,
        JList.of(
          new ExerciseByKeyCommand(
            template,
            key,
            choice,
            argument,
          )
        ),
        LedgerEffects,
      )
    ).map(_.getTransaction)

  override def submitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
  ): SubmitRequest =
    new SubmitRequest(
      Some(
        Commands.defaultInstance.copy(
          userId = userId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = actAs.map(_.getValue),
          readAs = readAs.map(_.getValue),
          commands = commands.asScala.toSeq.map(c => ApiCommand.fromJavaProto(c.toProtoCommand)),
          workflowId = workflowId,
        )
      )
    )

  override def submitRequest(party: Party, commands: JList[Command] = JList.of()): SubmitRequest =
    new SubmitRequest(
      Some(
        Commands.defaultInstance.copy(
          userId = userId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = Seq(party.getValue),
          commands = commands.asScala.toSeq.map(c => ApiCommand.fromJavaProto(c.toProtoCommand)),
          workflowId = workflowId,
        )
      )
    )

  override def submitAndWaitRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
  ): SubmitAndWaitRequest =
    new SubmitAndWaitRequest(
      Some(
        Commands.defaultInstance.copy(
          userId = userId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = actAs.map(_.getValue),
          readAs = readAs.map(_.getValue),
          commands = commands.asScala.toSeq.map(c => ApiCommand.fromJavaProto(c.toProtoCommand)),
          workflowId = workflowId,
        )
      )
    )

  override def submitAndWaitForTransactionRequest(
      actAs: List[Party],
      readAs: List[Party],
      commands: JList[Command],
      transactionShape: TransactionShape,
  ): SubmitAndWaitForTransactionRequest =
    new SubmitAndWaitForTransactionRequest(
      commands = Some(
        Commands.defaultInstance.copy(
          userId = userId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = actAs.map(_.getValue),
          readAs = readAs.map(_.getValue),
          commands = commands.asScala.toSeq.map(c => ApiCommand.fromJavaProto(c.toProtoCommand)),
          workflowId = workflowId,
        )
      ),
      transactionFormat = Some(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = actAs.map(_.getValue -> Filters(Nil)).toMap,
              filtersForAnyParty = None,
              verbose = true,
            )
          ),
          transactionShape = toProto(transactionShape),
        )
      ),
    )

  override def prepareSubmissionRequest(
      party: Party,
      commands: JList[Command],
      estimateTrafficCost: Option[CostEstimationHints] = None,
  ): PrepareSubmissionRequest =
    PrepareSubmissionRequest(
      userId = userId,
      commandId = nextCommandId(),
      commands = Commands.defaultInstance
        .copy(
          userId = userId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = Seq(party.getValue),
          commands = commands.asScala.toSeq.map(c => ApiCommand.fromJavaProto(c.toProtoCommand)),
          workflowId = workflowId,
        )
        .commands,
      minLedgerTime = None,
      actAs = Seq(party.getValue),
      readAs = Seq(party.getValue),
      disclosedContracts = Seq.empty,
      synchronizerId = "",
      packageIdSelectionPreference = Seq.empty,
      verboseHashing = false,
      prefetchContractKeys = Seq.empty,
      maxRecordTime = Option.empty,
      estimateTrafficCost = estimateTrafficCost,
    )

  override def executeSubmissionRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
  ): ExecuteSubmissionRequest = {
    val signature = party.signProto(preparedTx.preparedTransactionHash)
    ExecuteSubmissionRequest(
      preparedTransaction = preparedTx.preparedTransaction,
      partySignatures = Some(
        PartySignatures(
          Seq(
            SinglePartySignatures(
              party.underlying.getValue,
              Seq(signature),
            )
          )
        )
      ),
      deduplicationPeriod = ExecuteSubmissionRequest.DeduplicationPeriod.Empty,
      submissionId = nextSubmissionId(),
      userId = userId,
      hashingSchemeVersion = HashingSchemeVersion.HASHING_SCHEME_VERSION_V2,
      minLedgerTime = None,
    )
  }

  override def executeSubmissionAndWaitRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
  ): ExecuteSubmissionAndWaitRequest =
    executeSubmissionRequest(party, preparedTx).transformInto[ExecuteSubmissionAndWaitRequest]

  override def executeSubmissionAndWaitForTransactionRequest(
      party: ExternalParty,
      preparedTx: PrepareSubmissionResponse,
      transactionFormat: Option[TransactionFormat],
  ): ExecuteSubmissionAndWaitForTransactionRequest =
    executeSubmissionRequest(party, preparedTx)
      .into[ExecuteSubmissionAndWaitForTransactionRequest]
      .withFieldConst(_.transactionFormat, transactionFormat)
      .transform

  override def submitAndWaitRequest(party: Party, commands: JList[Command]): SubmitAndWaitRequest =
    new SubmitAndWaitRequest(
      Some(
        Commands.defaultInstance.copy(
          userId = userId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = Seq(party.getValue),
          commands = commands.asScala.toSeq.map(c => ApiCommand.fromJavaProto(c.toProtoCommand)),
          workflowId = workflowId,
        )
      )
    )

  override def submitAndWaitForTransactionRequest(
      party: Party,
      commands: JList[Command],
  ): SubmitAndWaitForTransactionRequest =
    submitAndWaitForTransactionRequest(
      party = party,
      commands = commands,
      transactionShape = AcsDelta,
      filterParties = Some(Seq(party)),
    )

  override def submitAndWaitForTransactionRequest(
      party: Party,
      commands: JList[Command],
      transactionShape: TransactionShape,
      filterParties: Option[Seq[Party]] = None,
      templateIds: Seq[Identifier] = Seq.empty,
      verbose: Boolean = true,
  ): SubmitAndWaitForTransactionRequest =
    SubmitAndWaitForTransactionRequest(
      commands = Some(
        Commands.defaultInstance.copy(
          userId = userId,
          commandId = nextCommandId(),
          submissionId = nextSubmissionId(),
          actAs = Seq(party.getValue),
          commands = commands.asScala.toSeq.map(c => ApiCommand.fromJavaProto(c.toProtoCommand)),
          workflowId = workflowId,
        )
      ),
      transactionFormat = Some(
        transactionFormat(
          parties = filterParties,
          templateIds = templateIds,
          transactionShape = transactionShape,
          verbose = verbose,
        )
      ),
    )

  override def submit(request: SubmitRequest): Future[Unit] =
    services.commandSubmission.submit(request).map(_ => ())

  override def submitAndWait(request: SubmitAndWaitRequest): Future[SubmitAndWaitResponse] =
    services.command.submitAndWait(request)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    services.command.submitAndWaitForTransaction(request)

  /** This addresses a narrow case in which we tolerate a single occurrence of a specific and
    * transient (and rare) error by retrying only a single time.
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

  override def completions(
      within: NonNegativeFiniteDuration,
      request: CompletionStreamRequest,
  ): Future[Vector[CompletionStreamResponse.CompletionResponse]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    )
      .within(within.toScala)
      .map(_.map(_.completionResponse))

  override def completions(
      take: Int,
      request: CompletionStreamRequest,
  ): Future[Vector[CompletionStreamResponse.CompletionResponse]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).take(take).map(_.map(_.completionResponse))

  override def completionStreamRequest(from: Long = referenceOffset)(
      parties: Party*
  ): CompletionStreamRequest =
    new CompletionStreamRequest(
      userId,
      parties.map(_.getValue),
      from,
    )

  override def firstCompletions(request: CompletionStreamRequest): Future[Vector[Completion]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).find(_.completionResponse.completion.nonEmpty)
      .map(_.completionResponse.completion.toList.toVector)

  override def firstCompletions(parties: Party*): Future[Vector[Completion]] =
    firstCompletions(completionStreamRequest()(parties*))

  override def findCompletionAtOffset(
      offset: Long,
      p: Completion => Boolean,
  )(parties: Party*): Future[Option[Completion]] = {
    // We have to request an offset before the reported offset, as offsets are exclusive in the completion service.
    val offsetPreviousToReportedOffset =
      if (offset == 0L) referenceOffset else offset - 1
    val reportedOffsetCompletionStreamRequest =
      completionStreamRequest(offsetPreviousToReportedOffset)(parties*)
    findCompletion(reportedOffsetCompletionStreamRequest)(p)
  }

  override def findCompletion(
      request: CompletionStreamRequest
  )(p: Completion => Boolean): Future[Option[Completion]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).find(_.completionResponse.completion.exists(p))
      .map(_.completionResponse.completion.filter(p))

  override def findCompletion(parties: Party*)(
      p: Completion => Boolean
  ): Future[Option[Completion]] =
    findCompletion(completionStreamRequest()(parties*))(p)

  override def offsets(n: Int, request: CompletionStreamRequest): Future[Vector[Long]] =
    new StreamConsumer[CompletionStreamResponse](
      services.commandCompletion.completionStream(request, _)
    ).filterTake(_.completionResponse.isCompletion)(n)
      .map(_.map(_.getCompletion.offset))

  override def checkHealth(): Future[HealthCheckResponse] =
    services.health.check(HealthCheckRequest())

  override def watchHealth(): Future[Seq[HealthCheckResponse]] =
    new StreamConsumer[HealthCheckResponse](services.health.watch(HealthCheckRequest(), _))
      .within(1.second)

  override def prune(
      pruneUpTo: Long,
      attempts: Int = 10,
  ): Future[PruneResponse] =
    // Distributed ledger participants need to reach global consensus prior to pruning. Hence the "eventually" here:
    eventually(assertionName = "Prune", attempts = attempts) {
      services.participantPruning
        .prune(
          PruneRequest(pruneUpTo, nextSubmissionId(), pruneAllDivulgedContracts = true)
        )
        .thereafterP { case Failure(exception) =>
          logger.warn("Failed to prune", exception)(LoggingContext.ForTesting)
        }
    }

  override def pruneCantonSafe(
      pruneUpTo: Long,
      party: Party,
      dummyCommand: Party => JList[Command],
  )(implicit ec: ExecutionContext): Future[Unit] =
    FutureAssertions.succeedsEventually(
      retryDelay = 100.millis,
      maxRetryDuration = 10.seconds,
      delayMechanism,
      "Pruning",
    ) {
      for {
        _ <- submitAndWait(submitAndWaitRequest(party, dummyCommand(party)))
        _ <- prune(
          pruneUpTo = pruneUpTo,
          attempts = 1,
        )
      } yield ()
    }(ec, LoggingContext.ForTesting)

  private[infrastructure] override def preallocateParties(
      n: Int,
      participants: Iterable[ParticipantTestContext],
      connectedSynchronizers: Int,
  ): Future[Vector[Party]] =
    for {
      parties <-
        if (partyAllocationConfig.allocateParties) {
          allocateParties(n, connectedSynchronizers)
        } else {
          reservePartyNames(n)
        }
      _ <- waitForPartiesOnOtherParticipants(participants, parties.toSet, connectedSynchronizers)
    } yield parties

  override def getConnectedSynchronizers(
      party: Option[Party],
      participantId: Option[String],
      identityProviderId: Option[String] = None,
  ): Future[Set[String]] =
    services.state
      .getConnectedSynchronizers(
        new GetConnectedSynchronizersRequest(
          party.map(_.getValue).getOrElse(""),
          participantId.getOrElse(""),
          identityProviderId.getOrElse(""),
        )
      )
      .map(_.connectedSynchronizers.map(_.synchronizerId).toSet)

  private def reservePartyNames(n: Int): Future[Vector[Party]] =
    Future.successful(Vector.fill(n)(Party(nextPartyHintId())))

  val maxOffsetCheckpointEmissionDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryCreate(
      features.offsetCheckpoint.getMaxOffsetCheckpointEmissionDelay.asJava
    )
}
