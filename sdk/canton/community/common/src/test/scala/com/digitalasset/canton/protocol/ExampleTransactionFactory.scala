// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Eval
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.{
  ContractIdAbsolutizationData,
  ContractIdAbsolutizationDataV1,
  ContractIdAbsolutizationDataV2,
}
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, VettedPackage}
import com.digitalasset.canton.topology.{
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LfTransactionUtil.{
  metadataFromCreate,
  metadataFromExercise,
  metadataFromFetch,
}
import com.digitalasset.canton.util.{LfTransactionBuilder, LfTransactionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref.PackageName
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance, Versioned}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.*
import org.scalatest.EitherValues

import java.time.Duration as JDuration
import java.util.UUID
import scala.collection.immutable.HashMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

import BaseTest.*

/** Provides convenience methods for creating [[ExampleTransaction]]s and parts thereof.
  */
object ExampleTransactionFactory {
  import EitherValues.*

  val pureCrypto: CryptoPureApi = new SymbolicPureCrypto()
  // Helper methods for Daml-LF types
  val languageVersion: LanguageVersion = LfTransactionBuilder.defaultLanguageVersion
  val packageId: LfPackageId = LfTransactionBuilder.defaultPackageId
  val upgradePackageId: LfPackageId = LfPackageId.assertFromString("upgraded-pkg-id")
  val templateId: LfTemplateId = LfTransactionBuilder.defaultTemplateId
  val packageName: PackageName = LfTransactionBuilder.defaultPackageName
  val someOptUsedPackages: Option[Set[LfPackageId]] = Some(Set(packageId))
  val defaultGlobalKey: LfGlobalKey = LfTransactionBuilder.defaultGlobalKey
  val transactionVersion: LfLanguageVersion = LfTransactionBuilder.defaultTransactionVersion

  private val random = new Random(0)

  private def valueCapturing(coid: List[LfContractId]): Value = {
    val captives = coid.map(c => (None, ValueContractId(c)))
    ValueRecord(None, captives.to(ImmArray))
  }

  private def versionedValueCapturing(coid: List[LfContractId]): Value.VersionedValue =
    LfVersioned(transactionVersion, valueCapturing(coid))

  def contractInstance(
      capturedIds: Seq[LfContractId] = Seq.empty,
      templateId: LfTemplateId = templateId,
      packageName: LfPackageName = packageName,
  ): LfThinContractInst =
    LfThinContractInst(
      packageName = packageName,
      template = templateId,
      arg = versionedValueCapturing(capturedIds.toList),
    )

  def authenticatedContractInstance(
      metadata: ContractMetadata,
      instance: LfThinContractInst = ExampleTransactionFactory.contractInstance(),
      ledgerTime: CantonTimestamp = CantonTimestamp.Epoch,
  ): ContractInstance = {
    val contractIdVersion =
      CantonContractIdVersion.maximumSupportedVersion(BaseTest.testedProtocolVersion).value match {
        case v1: CantonContractIdV1Version => v1
        case other =>
          throw new IllegalArgumentException(s"Unsupported contract ID version: $other")
      }
    val crypto = new SymbolicPureCrypto()
    val contractIdSuffixer = new ContractIdSuffixer(crypto, contractIdVersion)
    val contractSalt = ContractSalt.createV1(crypto)(
      transactionUuid = new UUID(1L, 1L),
      psid = SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::da")).toPhysical,
      mediator = MediatorGroupRecipient(MediatorGroupIndex.one),
      viewParticipantDataSalt = TestSalt.generateSalt(1),
      createIndex = 0,
      viewPosition = ViewPosition(List.empty),
    )
    val createdAt = CreationTime.CreatedAt(ledgerTime.toLf)
    val unsuffixedContractId = LfContractId.V1(ExampleTransactionFactory.lfHash(1337))
    val unsuffixedCreateNode = LfNodeCreate(
      coid = unsuffixedContractId,
      packageName = instance.unversioned.packageName,
      templateId = instance.unversioned.template,
      arg = instance.unversioned.arg,
      signatories = metadata.signatories,
      stakeholders = metadata.stakeholders,
      keyOpt = metadata.maybeKeyWithMaintainers,
      version = instance.version,
    )

    val ContractIdSuffixer.RelativeSuffixResult(suffixedCreateNode, _, _, authenticationData) =
      contractIdSuffixer
        .relativeSuffixForLocalContract(contractSalt, createdAt, unsuffixedCreateNode)
        .valueOr(err =>
          throw new IllegalArgumentException(s"Failed to compute suffix for contract: $err")
        )
    val fci = FatContractInstance.fromCreateNode(
      suffixedCreateNode,
      createdAt,
      authenticationData.toLfBytes,
    )
    ContractInstance.create(fci).value
  }

  val veryDeepValue: Value = {
    def deepValue(depth: Int): Value =
      if (depth <= 0) ValueUnit else ValueOptional(Some(deepValue(depth - 1)))

    deepValue(Value.MAXIMUM_NESTING + 10)
  }
  val veryDeepVersionedValue: VersionedValue =
    LfVersioned(transactionVersion, veryDeepValue)

  val veryDeepContractInstance: LfThinContractInst =
    LfThinContractInst(
      packageName = packageName,
      template = templateId,
      arg = veryDeepVersionedValue,
    )

  def globalKey(
      templateId: LfTemplateId,
      value: LfValue,
      packageName: LfPackageName = packageName,
  ): Versioned[LfGlobalKey] =
    LfVersioned(
      transactionVersion,
      LfGlobalKey.assertBuild(templateId, value, packageName),
    )

  def globalKeyWithMaintainers(
      key: LfGlobalKey = defaultGlobalKey,
      maintainers: Set[LfPartyId] = Set(signatory),
  ): Versioned[LfGlobalKeyWithMaintainers] =
    LfVersioned(transactionVersion, LfGlobalKeyWithMaintainers(key, maintainers))

  def fetchNode(
      cid: LfContractId,
      actingParties: Set[LfPartyId] = Set.empty,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      key: Option[LfGlobalKeyWithMaintainers] = None,
      byKey: Boolean = false,
      version: LfLanguageVersion = transactionVersion,
      templateId: LfTemplateId = templateId,
      interfaceId: Option[LfTemplateId] = None,
  ): LfNodeFetch =
    LfNodeFetch(
      coid = cid,
      packageName = packageName,
      templateId = templateId,
      actingParties = actingParties,
      signatories = signatories,
      stakeholders = signatories ++ observers,
      keyOpt = key,
      byKey = byKey,
      version = version,
      interfaceId = interfaceId,
    )

  def createNode(
      cid: LfContractId,
      contractInstance: LfThinContractInst = this.contractInstance(),
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      key: Option[LfGlobalKeyWithMaintainers] = None,
  ): LfNodeCreate = {
    val unversionedContractInst = contractInstance.unversioned
    LfNodeCreate(
      coid = cid,
      packageName = unversionedContractInst.packageName,
      templateId = unversionedContractInst.template,
      arg = unversionedContractInst.arg,
      signatories = signatories,
      stakeholders = signatories ++ observers,
      keyOpt = key,
      version = transactionVersion,
    )
  }

  def exerciseNode(
      targetCoid: LfContractId,
      consuming: Boolean = true,
      args: List[LfContractId] = Nil,
      children: List[LfNodeId] = Nil,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      choiceObservers: Set[LfPartyId] = Set.empty,
      actingParties: Set[LfPartyId] = Set.empty,
      exerciseResult: Option[Value] = Some(Value.ValueNone),
      key: Option[LfGlobalKeyWithMaintainers] = None,
      byKey: Boolean = false,
      templateId: LfTemplateId = templateId,
      packageName: LfPackageName = packageName,
  ): LfNodeExercises =
    LfNodeExercises(
      targetCoid = targetCoid,
      packageName = packageName,
      templateId = templateId,
      interfaceId = None,
      choiceId = LfChoiceName.assertFromString("choice"),
      consuming = consuming,
      actingParties = actingParties,
      chosenValue = valueCapturing(args),
      stakeholders = signatories ++ observers,
      signatories = signatories,
      choiceObservers = choiceObservers,
      choiceAuthorizers = None,
      children = children.to(ImmArray),
      exerciseResult = exerciseResult,
      keyOpt = key,
      byKey = byKey,
      version = transactionVersion,
    )

  def exerciseNodeWithoutChildren(
      targetCoid: LfContractId,
      consuming: Boolean = true,
      args: List[LfContractId] = Nil,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      actingParties: Set[LfPartyId] = Set.empty,
      exerciseResult: Option[Value] = Some(Value.ValueNone),
  ): LfNodeExercises =
    exerciseNode(
      targetCoid = targetCoid,
      consuming = consuming,
      args = args,
      children = Nil,
      signatories = signatories,
      observers = observers,
      actingParties = actingParties,
      exerciseResult = exerciseResult,
    ).copy(children = ImmArray.empty)

  def lookupByKeyNode(
      key: LfGlobalKey,
      maintainers: Set[LfPartyId] = Set.empty,
      resolution: Option[LfContractId] = None,
  ): LfNodeLookupByKey =
    LfNodeLookupByKey(
      templateId = key.templateId,
      packageName = key.packageName,
      key = LfGlobalKeyWithMaintainers(key, maintainers),
      result = resolution,
      version = transactionVersion,
    )

  def nodeId(index: Int): LfNodeId = LfNodeId(index)

  val submissionSeed: LfHash = LfHash.secureRandom(
    LfHash.hashPrivateKey("example transaction factory tests")
  )() // avoiding dependency on SeedService.staticRandom after move to ledger api server

  def transaction(rootIndices: Seq[Int], nodes: LfNode*): LfVersionedTransaction =
    transactionFrom(rootIndices, 0, nodes*)

  def transactionFrom(
      rootIndices: Seq[Int],
      startIndex: Int,
      nodes: LfNode*
  ): LfVersionedTransaction = {
    val roots = rootIndices.map(nodeId).to(ImmArray)

    val nodesMap = HashMap(nodes.zipWithIndex.map { case (node, index) =>
      (nodeId(index + startIndex), node)
    }*)

    val version = protocol.maxTransactionVersion(
      NonEmpty
        .from(nodesMap.values.toSeq.mapFilter(_.optVersion))
        .getOrElse(NonEmpty(Seq, transactionVersion))
    )

    LfVersionedTransaction(version, nodesMap, roots)
  }

  def inventSeeds(tx: LfVersionedTransaction): Map[LfNodeId, LfHash] =
    tx.nodes.collect {
      case (nodeId, node) if LfTransactionUtil.nodeHasSeed(node) => nodeId -> lfHash(nodeId.index)
    }

  val malformedLfTransaction: LfVersionedTransaction = transaction(Seq(0))

  // Helper methods for contract ids and transaction ids
  def transactionId(index: Int): TransactionId = TransactionId(
    TestHash.digest(s"transactionId$index")
  )

  def unicum(index: Int): Unicum = Unicum(TestHash.digest(s"unicum$index"))

  def lfHash(index: Int): LfHash =
    LfHash.assertFromBytes(
      Bytes.assertFromString(f"$index%04x".padTo(LfHash.underlyingHashLength * 2, '0'))
    )

  def suffixedId(
      discriminator: Int,
      suffix: Int,
      contractIdVersion: CantonContractIdVersion = AuthenticatedContractIdVersionV11,
  ): LfContractId =
    contractIdVersion match {
      case v1: CantonContractIdV1Version =>
        LfContractId.V1(
          discriminator = lfHash(discriminator),
          suffix = v1.versionPrefixBytes ++ Bytes.fromByteString(
            TestHash.digest(f"$suffix%04x").getCryptographicEvidence
          ),
        )
      case v2: CantonContractIdV2Version =>
        val local = LfContractId.V2.unsuffixed(LfTimestamp.Epoch, lfHash(discriminator)).local
        val suffixBytes =
          v2.versionPrefixBytesAbsolute ++ Bytes.fromByteString(TestHash.digest(suffix).unwrap)
        LfContractId.V2.assertBuild(local, suffixBytes)
    }

  def unsuffixedId(index: Int): LfContractId.V1 = LfContractId.V1(lfHash(index))

  def rootViewPosition(index: Int, total: Int): ViewPosition =
    ViewPosition(List(MerkleSeq.indicesFromSeq(total)(index)))

  def asSerializableRaw(contractInstance: LfThinContractInst): SerializableRawContractInstance =
    SerializableRawContractInstance
      .create(contractInstance)
      .fold(err => throw new IllegalArgumentException(err.toString), Predef.identity)

  private def asAuthenticationData(
      salt: Salt,
      version: CantonContractIdVersion,
      transactionId: Option[TransactionId] = None,
  ): ContractAuthenticationData =
    version match {
      case v1: CantonContractIdV1Version => ContractAuthenticationDataV1(salt)(v1)
      case v2: CantonContractIdV2Version =>
        ContractAuthenticationDataV2(
          Bytes.fromByteString(salt.forHashing),
          transactionId,
          Seq.empty,
        )(v2)
    }

  def asContractInstance[Time <: CreationTime](
      contractId: LfContractId,
      contractInstance: LfThinContractInst = this.contractInstance(),
      metadata: ContractMetadata =
        ContractMetadata.tryCreate(Set(this.signatory), Set(this.signatory), None),
      ledgerTime: Time = CreationTime.CreatedAt(LfTimestamp.Epoch),
  )(
      authenticationData: ContractAuthenticationData = asAuthenticationData(
        TestSalt.generateSalt(random.nextInt()),
        CantonContractIdVersion.extractCantonContractIdVersion(contractId).value,
      )
  ): GenContractInstance { type InstCreatedAtTime <: Time } = {
    val createNode = LfNodeCreate(
      coid = contractId,
      packageName = contractInstance.unversioned.packageName,
      templateId = contractInstance.unversioned.template,
      arg = contractInstance.unversioned.arg,
      signatories = metadata.signatories,
      stakeholders = metadata.stakeholders,
      keyOpt = metadata.maybeKeyWithMaintainers,
      version = contractInstance.version,
    )
    val fci = FatContractInstance.fromCreateNode(
      createNode,
      ledgerTime,
      authenticationData.toLfBytes,
    )
    ContractInstance.create(fci).value
  }

  // Parties and participants

  val submittingParticipant: ParticipantId = ParticipantId("submittingParticipant")
  val signatoryParticipant: ParticipantId = ParticipantId("signatoryParticipant")
  val observerParticipant: ParticipantId = ParticipantId("observerParticipant")
  val extraParticipant: ParticipantId = ParticipantId("extraParticipant")
  val signatory: LfPartyId = LfPartyId.assertFromString("signatory::default")
  val signatoryReplica: LfPartyId = LfPartyId.assertFromString("signatoryReplica::default")
  val observer: LfPartyId = LfPartyId.assertFromString("observer::default")
  val extra: LfPartyId = LfPartyId.assertFromString("extra::default")
  val submitter: LfPartyId = submittingParticipant.adminParty.toLf
  val submitters: List[LfPartyId] = List(submitter)

  // Request metadata

  val userId: UserId = DefaultDamlValues.userId()
  val commandId: CommandId = DefaultDamlValues.commandId()
  val workflowId: WorkflowId = WorkflowId.assertFromString("testWorkflowId")

  val defaultTestingTopology: TestingTopology =
    TestingTopology.from(
      topology = Map(
        submitter -> Map(submittingParticipant -> Submission),
        signatory -> Map(
          signatoryParticipant -> Confirmation
        ),
        signatoryReplica -> Map(
          signatoryParticipant -> Confirmation
        ),
        observer -> Map(
          observerParticipant -> Observation
        ),
        extra -> Map(
          extraParticipant -> Observation
        ),
      ),
      participants = Map(submittingParticipant -> ParticipantAttributes(Submission)),
      packages =
        Seq(submittingParticipant, signatoryParticipant, observerParticipant, extraParticipant)
          .map(
            _ -> VettedPackage.unbounded(Seq(ExampleTransactionFactory.packageId, upgradePackageId))
          )
          .toMap,
    )

  def defaultTestingIdentityFactory: TestingIdentityFactory =
    defaultTestingTopology.build()

  // Topology
  def defaultTopologySnapshot: TopologySnapshot =
    defaultTestingIdentityFactory.topologySnapshot()

  // Merkle trees
  def blinded[A](tree: MerkleTree[A]): MerkleTree[A] = BlindedNode(tree.rootHash)

  final case class CreateInfo(
      relativeAuthenticationData: ContractAuthenticationData,
      relativeFci: FatContractInstance,
  ) {
    def relativeContractId: LfContractId = relativeFci.contractId
  }
}

/** Factory for [[ExampleTransaction]]. Also contains a number of predefined example transactions.
  * Also provides convenience methods for creating [[ExampleTransaction]]s and parts thereof.
  */
class ExampleTransactionFactory(
    val cryptoOps: HashOps with HmacOps with RandomOps = new SymbolicPureCrypto,
    versionOverride: Option[ProtocolVersion] = None,
)(
    val transactionSalt: Salt = TestSalt.generateSalt(0),
    val transactionSeed: SaltSeed = TestSalt.generateSeed(0),
    val transactionUuid: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555"),
    val psid: PhysicalSynchronizerId = SynchronizerId(
      UniqueIdentifier.tryFromProtoPrimitive("example::default")
    ).toPhysical,
    val mediatorGroup: MediatorGroupRecipient = MediatorGroupRecipient(MediatorGroupIndex.zero),
    val ledgerTime: CantonTimestamp = CantonTimestamp.Epoch,
    val preparationTime: CantonTimestamp = CantonTimestamp.Epoch.minusMillis(9),
    val topologySnapshot: TopologySnapshot = defaultTopologySnapshot,
    val cantonContractIdVersion: CantonContractIdVersion = AuthenticatedContractIdVersionV11,
)(implicit ec: ExecutionContext, tc: TraceContext)
    extends EitherValues {

  private val protocolVersion = versionOverride.getOrElse(BaseTest.testedProtocolVersion)
  private val random = new Random(0)

  private def createNewView(
      rootNode: LfActionNode,
      rootSeed: Option[LfHash],
      rootNodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
      isRoot: Boolean,
  ): FutureUnlessShutdown[NewView] = {

    val rootRbContext = RollbackContext.empty

    val submittingAdminPartyO =
      Option.when(isRoot)(submitterMetadata.submittingParticipant.adminParty.toLf)
    TransactionViewDecompositionFactory
      .informeesParticipantsAndThreshold(rootNode, topologySnapshot, submittingAdminPartyO)
      .map { case (viewInformeesWithParticipantData, viewThreshold) =>
        val viewInformees = viewInformeesWithParticipantData.fmap(_._2)
        NewView(
          rootNode,
          ViewConfirmationParameters.create(viewInformees, viewThreshold),
          rootSeed,
          rootNodeId,
          tailNodes,
          rootRbContext,
        )
      }
  }

  private def awaitCreateNewView(
      rootNode: LfActionNode,
      rootSeed: Option[LfHash],
      rootNodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
      isRoot: Boolean,
  ): NewView =
    Await
      .result(
        createNewView(
          rootNode,
          rootSeed,
          rootNodeId,
          tailNodes,
          isRoot,
        ),
        10.seconds,
      )
      .onShutdown(throw new RuntimeException("Aborted due to shutdown"))

  /** Yields standard test cases that the sync-protocol must be able to handle. Yields only "happy"
    * cases, i.e., the sync-protocol must not emit an error.
    */
  lazy val standardHappyCases: Seq[ExampleTransaction] =
    Seq[ExampleTransaction](
      EmptyTransaction,
      SingleCreate(seed = deriveNodeSeed(0)),
      SingleCreate(
        seed = deriveNodeSeed(0),
        capturedContractIds = Seq(
          suffixedId(-1, 0, cantonContractIdVersion) -> suffixedId(-1, 0, cantonContractIdVersion),
          suffixedId(-1, 1, cantonContractIdVersion) -> suffixedId(-1, 1, cantonContractIdVersion),
        ),
      ),
      SingleFetch(version = LfLanguageVersion.v2_dev),
      SingleExercise(seed = deriveNodeSeed(0)),
      SingleExerciseWithNonstakeholderActor(seed = deriveNodeSeed(0)),
      MultipleRoots,
      MultipleRootsAndSimpleViewNesting,
      MultipleRootsAndViewNestings,
      ViewInterleavings,
      TransientContracts,
    )

  // Helpers for GenTransactions

  private val numberOfLeavesPerView: Int = 2
  private val numberOfLeavesAtTransactionRoot: Int = 3

  def commonDataSalt(viewIndex: Int): Salt =
    Salt.tryDeriveSalt(
      transactionSeed,
      viewIndex * numberOfLeavesPerView + numberOfLeavesAtTransactionRoot + 0,
      cryptoOps,
    )
  def participantDataSalt(viewIndex: Int): Salt =
    Salt.tryDeriveSalt(
      transactionSeed,
      viewIndex * numberOfLeavesPerView + numberOfLeavesAtTransactionRoot + 1,
      cryptoOps,
    )

  val lfTransactionSeed: LfHash = LfHash.deriveTransactionSeed(
    ExampleTransactionFactory.submissionSeed,
    ExampleTransactionFactory.submittingParticipant.toLf,
    preparationTime.toLf,
  )

  def deriveNodeSeed(path: Int*): LfHash =
    path.foldLeft(lfTransactionSeed)((seed, i) => LfHash.deriveNodeSeed(seed, i))

  def discriminator(nodeSeed: LfHash, stakeholders: Set[LfPartyId]): LfHash =
    LfHash.deriveContractDiscriminator(nodeSeed, preparationTime.toLf, stakeholders)

  val contractIdSuffixer = new ContractIdSuffixer(
    cryptoOps,
    cantonContractIdVersion,
  )
  val unicumGenerator = new UnicumGenerator(cryptoOps)

  def absolutizer(transactionId: TransactionId): ContractIdAbsolutizer = {
    val absolutizationData: ContractIdAbsolutizationData =
      cantonContractIdVersion match {
        case _: CantonContractIdV1Version =>
          ContractIdAbsolutizationDataV1
        case _: CantonContractIdV2Version =>
          ContractIdAbsolutizationDataV2(transactionId, ledgerTime)
      }
    new ContractIdAbsolutizer(cryptoOps, absolutizationData)
  }

  def localContractId(discriminator: LfHash): LfContractId = cantonContractIdVersion match {
    case _: CantonContractIdV1Version => LfContractId.V1(discriminator)
    case _: CantonContractIdV2Version =>
      LfContractId.V2.unsuffixed(ledgerTime.toLf, discriminator)
  }

  private val relativeCreateTime = cantonContractIdVersion match {
    case _: CantonContractIdV1Version => CreationTime.CreatedAt(ledgerTime.toLf)
    case _: CantonContractIdV2Version => CreationTime.Now
  }

  def fromLocalContractId(
      viewPosition: ViewPosition,
      viewIndex: Int,
      createIndex: Int,
      suffixedContractInstance: LfThinContractInst,
      localContractId: LfContractId,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      maybeKeyWithMaintainers: Option[protocol.LfGlobalKeyWithMaintainers] = None,
  ): CreateInfo = {
    val metadata = ContractMetadata.tryCreate(
      signatories,
      signatories ++ observers,
      maybeKeyWithMaintainers.map(LfVersioned(transactionVersion, _)),
    )
    val viewParticipantDataSalt = participantDataSalt(viewIndex)
    val contractSalt = cantonContractIdVersion match {
      case _: CantonContractIdV1Version =>
        ContractSalt.createV1(cryptoOps)(
          transactionUuid,
          psid,
          mediatorGroup,
          viewParticipantDataSalt,
          createIndex,
          viewPosition,
        )
      case _: CantonContractIdV2Version =>
        ContractSalt.createV2(cryptoOps)(viewParticipantDataSalt, createIndex, viewPosition)
    }
    val unsuffixedCreateNode = createNode(
      localContractId,
      suffixedContractInstance,
      metadata.signatories,
      metadata.stakeholders,
      metadata.maybeKeyWithMaintainers,
    )
    val ContractIdSuffixer.RelativeSuffixResult(
      relativeCreateNode,
      _,
      _,
      relativeAuthenticationData,
    ) =
      contractIdSuffixer
        .relativeSuffixForLocalContract(
          contractSalt,
          relativeCreateTime,
          unsuffixedCreateNode,
        )
        .valueOr(err =>
          throw new IllegalArgumentException(s"Cannot compute suffix for contract: $err")
        )
    val relativeFci = FatContractInstance.fromCreateNode(
      relativeCreateNode,
      relativeCreateTime,
      relativeAuthenticationData.toLfBytes,
    )
    CreateInfo(
      relativeAuthenticationData,
      relativeFci,
    )

  }

  def toAbsolute(
      transactionId: TransactionId,
      relativeFci: FatContractInstance,
  ): (ContractAuthenticationData, LfContractId) = {
    val absoluteFci = absolutizer(transactionId)
      .absolutizeFci(relativeFci)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Cannot absolutize contract instance: $err")
      )
    val authenticationData = ContractAuthenticationData
      .fromLfBytes(cantonContractIdVersion, absoluteFci.authenticationData)
      .valueOr(err => throw new IllegalArgumentException(s"Cannot parse authentication data: $err"))
    authenticationData -> absoluteFci.contractId
  }

  def rootViewPosition(index: Int, total: Int): ViewPosition =
    ViewPosition(List(MerkleSeq.indicesFromSeq(total)(index)))

  def subViewIndex(index: Int, total: Int): MerklePathElement =
    TransactionSubviews.indices(total)(index)

  private def instanceFromCreate(
      node: LfNodeCreate,
      authenticationData: ContractAuthenticationData,
  ): NewContractInstance =
    asContractInstance(
      node.coid,
      node.versionedCoinst,
      metadataFromCreate(node),
      relativeCreateTime,
    )(authenticationData)

  private def viewInternal(
      node: LfActionNode,
      viewConfirmationParameters: ViewConfirmationParameters,
      viewIndex: Int,
      consumed: Set[LfContractId],
      coreInputs: Seq[GenContractInstance],
      created: Seq[NewContractInstance],
      resolvedKeys: Map[LfGlobalKey, SerializableKeyResolution],
      seed: Option[LfHash],
      packagePreference: Set[LfPackageId],
      subviews: Seq[TransactionView],
  ): TransactionView = {
    val viewCommonData =
      ViewCommonData.tryCreate(cryptoOps)(
        viewConfirmationParameters,
        commonDataSalt(viewIndex),
        protocolVersion,
      )

    val createWithSerialization = created.map { contract =>
      val coid = contract.contractId
      CreatedContract.tryCreate(
        contract,
        consumed.contains(coid),
        rolledBack = false,
      )
    }

    val coreInputContracts = coreInputs.map { contract =>
      val coid = contract.contractId
      coid -> InputContract(contract, consumed.contains(coid))
    }.toMap

    val createdInSubviews = (for {
      childView <- subviews
      subView <- childView.flatten
      createdContract <- subView.viewParticipantData.tryUnwrap.createdCore
    } yield createdContract.contract.contractId).toSet

    val createdInSubviewArchivedInCore = consumed.intersect(createdInSubviews)

    val actionDescription =
      ActionDescription.tryFromLfActionNode(
        LfTransactionUtil.lightWeight(node),
        seed,
        packagePreference = packagePreference,
        protocolVersion,
      )

    val viewParticipantData = ViewParticipantData.tryCreate(cryptoOps)(
      coreInputContracts,
      createWithSerialization,
      createdInSubviewArchivedInCore,
      resolvedKeys.fmap(LfVersioned(transactionVersion, _)),
      actionDescription,
      RollbackContext.empty,
      participantDataSalt(viewIndex),
      protocolVersion,
    )

    val subViews = TransactionSubviews(subviews)(protocolVersion, cryptoOps)
    TransactionView.tryCreate(cryptoOps)(
      viewCommonData,
      viewParticipantData,
      subviews = subViews,
      protocolVersion,
    )
  }

  def view(
      node: LfActionNode,
      viewIndex: Int,
      consumed: Set[LfContractId],
      coreInputs: Seq[GenContractInstance],
      created: Seq[NewContractInstance],
      resolvedKeys: Map[LfGlobalKey, SerializableKeyResolution],
      seed: Option[LfHash],
      isRoot: Boolean,
      packagePreference: Set[LfPackageId],
      subviews: TransactionView*
  ): TransactionView = {

    val submittingAdminPartyO =
      Option.when(isRoot)(submitterMetadata.submittingParticipant.adminParty.toLf)
    val (rawInformeesWithParticipantData, rawThreshold) =
      Await.result(
        TransactionViewDecompositionFactory
          .informeesParticipantsAndThreshold(node, topologySnapshot, submittingAdminPartyO)
          .failOnShutdownTo(new Exception("Aborted due to shutdown")),
        10.seconds,
      )
    val rawInformees = rawInformeesWithParticipantData.fmap { case (_, weight) => weight }
    val viewConfirmationParameters = ViewConfirmationParameters.create(rawInformees, rawThreshold)

    viewInternal(
      node,
      viewConfirmationParameters,
      viewIndex,
      consumed,
      coreInputs,
      created,
      resolvedKeys,
      seed,
      packagePreference,
      subviews,
    )
  }

  def viewWithInformeesMerge(
      node: LfActionNode,
      nodesToMerge: Seq[LfActionNode],
      viewIndex: Int,
      consumed: Set[LfContractId],
      coreInputs: Seq[ContractInstance],
      created: Seq[NewContractInstance],
      resolvedKeys: Map[LfGlobalKey, SerializableKeyResolution],
      seed: Option[LfHash],
      isRoot: Boolean,
      packagePreference: Set[LfPackageId],
      subviews: TransactionView*
  ): TransactionView = {

    val viewConfirmationParametersToMerge = (node +: nodesToMerge).map { nodeToMerge =>
      val (rawInformeesWithParticipantData, rawThreshold) =
        Await.result(
          TransactionViewDecompositionFactory
            .informeesParticipantsAndThreshold(
              nodeToMerge,
              topologySnapshot,
              Option.when(isRoot && nodeToMerge == node)(
                submitterMetadata.submittingParticipant.adminParty.toLf
              ),
            )
            .failOnShutdownTo(new Exception("Aborted due to shutdown")),
          10.seconds,
        )
      val rawInformees = rawInformeesWithParticipantData.fmap { case (_, weight) => weight }
      ViewConfirmationParameters.create(rawInformees, rawThreshold)
    }

    val viewConfirmationParameters =
      ViewConfirmationParameters.tryCreate(
        viewConfirmationParametersToMerge
          .flatMap(_.informees)
          .toSet,
        viewConfirmationParametersToMerge
          .flatMap(
            _.quorums
          )
          .distinct,
      )

    viewInternal(
      node,
      viewConfirmationParameters,
      viewIndex,
      consumed,
      coreInputs,
      created,
      resolvedKeys,
      seed,
      packagePreference,
      subviews,
    )
  }

  def mkMetadata(seeds: Map[LfNodeId, LfHash] = Map.empty): TransactionMetadata =
    TransactionMetadata(ledgerTime, preparationTime, seeds)

  def versionedTransactionWithSeeds(
      rootIndices: Seq[Int],
      nodes: LfNode*
  ): (LfVersionedTransaction, TransactionMetadata) = {
    val tx = transaction(rootIndices, nodes*)
    val seeds = inventSeeds(tx)
    (tx, mkMetadata(seeds))
  }

  val submitterMetadata: SubmitterMetadata =
    SubmitterMetadata(
      NonEmpty(Set, submitter),
      userId,
      commandId,
      submittingParticipant,
      Salt.tryDeriveSalt(transactionSeed, 0, cryptoOps),
      DefaultDamlValues.submissionId().some,
      DeduplicationDuration(JDuration.ofSeconds(100)),
      ledgerTime.plusSeconds(100),
      None,
      cryptoOps,
      protocolVersion,
    )

  val commonMetadata: CommonMetadata =
    CommonMetadata
      .create(cryptoOps)(
        psid,
        mediatorGroup,
        Salt.tryDeriveSalt(transactionSeed, 1, cryptoOps),
        transactionUuid,
      )

  val participantMetadata: ParticipantMetadata =
    ParticipantMetadata(cryptoOps)(
      ledgerTime,
      preparationTime,
      Some(workflowId),
      Salt.tryDeriveSalt(transactionSeed, 2, cryptoOps),
      protocolVersion,
    )

  def genTransactionTree(rootViews: TransactionView*): GenTransactionTree =
    GenTransactionTree.tryCreate(cryptoOps)(
      submitterMetadata,
      commonMetadata,
      participantMetadata,
      MerkleSeq.fromSeq(cryptoOps, protocolVersion)(rootViews),
    )

  def blindedForInformeeTree(
      view: TransactionView,
      subviews: MerkleTree[TransactionView]*
  ): TransactionView =
    view match {
      case TransactionView(viewCommonData, viewParticipantData, _) =>
        val subViews =
          TransactionSubviews(subviews)(
            protocolVersion,
            cryptoOps,
          )
        TransactionView.tryCreate(cryptoOps)(
          viewCommonData,
          blinded(viewParticipantData),
          subviews = subViews,
          protocolVersion,
        )
    }

  def mkFullInformeeTree(rootViews: MerkleTree[TransactionView]*): FullInformeeTree =
    FullInformeeTree.tryCreate(
      GenTransactionTree.tryCreate(cryptoOps)(
        submitterMetadata,
        commonMetadata,
        blinded(participantMetadata),
        MerkleSeq.fromSeq(cryptoOps, protocolVersion)(rootViews),
      ),
      protocolVersion,
    )

  def rootTransactionViewTree(rootViews: MerkleTree[TransactionView]*): FullTransactionViewTree =
    FullTransactionViewTree.tryCreate(
      GenTransactionTree.tryCreate(cryptoOps)(
        submitterMetadata,
        commonMetadata,
        participantMetadata,
        MerkleSeq.fromSeq(cryptoOps, protocolVersion)(rootViews),
      )
    )

  def leafsBlinded(view: TransactionView, subviews: MerkleTree[TransactionView]*): TransactionView =
    view match {
      case TransactionView(viewCommonData, viewParticipantData, _) =>
        val subViews =
          TransactionSubviews(subviews)(
            protocolVersion,
            cryptoOps,
          )
        TransactionView.tryCreate(cryptoOps)(
          blinded(viewCommonData),
          blinded(viewParticipantData),
          subviews = subViews,
          protocolVersion,
        )
    }

  def nonRootTransactionViewTree(rootViews: MerkleTree[TransactionView]*): FullTransactionViewTree =
    FullTransactionViewTree.tryCreate(
      GenTransactionTree.tryCreate(cryptoOps)(
        blinded(submitterMetadata),
        commonMetadata,
        participantMetadata,
        MerkleSeq.fromSeq(cryptoOps, protocolVersion)(rootViews),
      )
    )

  // ExampleTransactions

  case object EmptyTransaction extends ExampleTransaction {

    override def keyResolver: LfKeyResolver = Map.empty

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "empty transaction"

    override def versionedUnsuffixedTransaction: LfVersionedTransaction = transaction(Seq.empty)

    override def rootViewDecompositions: Seq[NewView] = Seq.empty

    override def rootViews: Seq[TransactionView] = Seq.empty

    override def viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])] = Seq.empty

    override def transactionTree: GenTransactionTree = genTransactionTree()

    override def fullInformeeTree: FullInformeeTree = mkFullInformeeTree()

    override def reinterpretedSubtransactions: Seq[
      (
          FullTransactionViewTree,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
          Witnesses,
      )
    ] =
      Seq.empty

    override def rootTransactionViewTrees: Seq[FullTransactionViewTree] = Seq.empty

    override def versionedSuffixedTransaction: LfVersionedTransaction =
      LfVersionedTransaction(
        version = transactionVersion,
        roots = ImmArray.empty,
        nodes = HashMap.empty,
      )

    override def metadata: TransactionMetadata = mkMetadata()

    override def usedAndCreated: UsedAndCreatedContracts = UsedAndCreatedContracts.empty
  }

  abstract class SingleNode(val nodeSeed: Option[LfHash]) extends ExampleTransaction {
    override def cryptoOps: HashOps & RandomOps = ExampleTransactionFactory.this.cryptoOps

    def interpretedContractId: LfContractId

    def relativizedContractId: LfContractId

    lazy val absolutizedContractId: LfContractId =
      absolutizer(transactionId)
        .absolutizeContractId(relativizedContractId)
        .valueOr(err =>
          throw new IllegalArgumentException(
            s"Cannot absolutize contract id $relativizedContractId: $err"
          )
        )

    def relativeAuthenticationData: ContractAuthenticationData

    def absoluteAuthenticationData: ContractAuthenticationData

    def nodeId: LfNodeId

    protected def relativeContractInstance: LfThinContractInst

    protected def absoluteContractInstance: LfThinContractInst

    def lfNode: LfActionNode

    def node: LfActionNode

    def reinterpretedNode: LfActionNode

    def relativeNode: LfActionNode = reinterpretedNode

    def consuming: Boolean

    def created: Seq[NewContractInstance] = relativeNode match {
      case n: LfNodeCreate =>
        Seq(
          asContractInstance(n.coid, n.versionedCoinst, metadataFromCreate(n), relativeCreateTime)(
            relativeAuthenticationData
          )
        )
      case _ => Seq.empty
    }

    def createdAbsolute: Map[LfContractId, NewContractInstance] = node match {
      case n: LfNodeCreate =>
        Map(
          n.coid -> asContractInstance(n.coid, n.versionedCoinst, metadataFromCreate(n))(
            absoluteAuthenticationData
          )
        )
      case _ => Map.empty
    }

    def used: Seq[GenContractInstance] = {
      val creationTime = cantonContractIdVersion match {
        case _: CantonContractIdV1Version => CreationTime.CreatedAt(ledgerTime.toLf)
        case _: CantonContractIdV2Version =>
          if (relativizedContractId.isAbsolute) CreationTime.CreatedAt(ledgerTime.toLf)
          else CreationTime.Now
      }
      relativeNode match {
        case n: LfNodeExercises =>
          Seq(
            asContractInstance(
              n.targetCoid,
              relativeContractInstance,
              metadataFromExercise(n),
              creationTime,
            )(relativeAuthenticationData)
          )
        case n: LfNodeFetch =>
          Seq(
            asContractInstance(
              n.coid,
              relativeContractInstance,
              metadataFromFetch(n),
              creationTime,
            )(relativeAuthenticationData)
          )
        case _ => Seq.empty
      }
    }

    def usedAbsolute: Map[LfContractId, GenContractInstance] =
      node match {
        case n: LfNodeExercises =>
          Map(
            n.targetCoid -> asContractInstance(
              n.targetCoid,
              absoluteContractInstance,
              metadataFromExercise(n),
            )(absoluteAuthenticationData)
          )
        case n: LfNodeFetch =>
          Map(
            n.coid -> asContractInstance(
              n.coid,
              absoluteContractInstance,
              metadataFromFetch(n),
            )(absoluteAuthenticationData)
          )
        case _ => Map.empty
      }

    def consumed: Set[LfContractId] = if (consuming) used.map(_.contractId).toSet else Set.empty

    def consumedAbsolute: Map[LfContractId, Set[LfPartyId]] =
      if (consuming) usedAbsolute.fmap(instance => instance.stakeholders)
      else Map.empty

    def metadata: TransactionMetadata =
      mkMetadata(nodeSeed.fold(Map.empty[LfNodeId, LfHash])(seed => Map(nodeId -> seed)))

    override def keyResolver: LfKeyResolver =
      node.gkeyOpt.fold(Map.empty: LfKeyResolver)(k => Map(k -> LfTransactionUtil.contractId(node)))

    override lazy val versionedUnsuffixedTransaction: LfVersionedTransaction =
      transaction(Seq(0), lfNode)

    override lazy val rootViewDecompositions: Seq[NewView] =
      Seq(
        awaitCreateNewView(
          lfNode,
          nodeSeed,
          nodeId,
          Seq.empty,
          isRoot = true,
        )
      )

    lazy val view0: TransactionView =
      view(relativeNode, 0, consumed, used, created, Map.empty, nodeSeed, isRoot = true, Set.empty)

    override lazy val rootViews: Seq[TransactionView] = Seq(view0)

    override lazy val viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])] = Seq(
      view0 -> Seq(view0)
    )

    override lazy val transactionTree: GenTransactionTree = genTransactionTree(view0)

    override lazy val fullInformeeTree: FullInformeeTree = mkFullInformeeTree(
      blindedForInformeeTree(view0)
    )

    override lazy val rootTransactionViewTrees: Seq[FullTransactionViewTree] = transactionViewTrees

    override lazy val versionedSuffixedTransaction: LfVersionedTransaction =
      transaction(Seq(0), node)

    override lazy val reinterpretedSubtransactions: Seq[
      (
          FullTransactionViewTree,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
          Witnesses,
      )
    ] =
      Seq(
        (
          rootTransactionViewTree(view0),
          (transaction(Seq(0), reinterpretedNode), metadata, keyResolver),
          Witnesses(
            NonEmpty(
              List,
              view0.viewCommonData.tryUnwrap.viewConfirmationParameters.informees,
            )
          ),
        )
      )

    override def usedAndCreated: UsedAndCreatedContracts = UsedAndCreatedContracts.empty.copy(
      checkActivenessTxInputs = usedAbsolute.keySet,
      consumedInputsOfHostedStakeholders = consumedAbsolute,
      used = usedAbsolute,
      maybeCreated = createdAbsolute.fmap(Some(_)),
    )
  }

  /** Single create. By default, [[submitter]] is the only signatory and [[observer]] the only
    * observer.
    *
    * @param seed
    *   the node seed for the create node, used to derive the contract id
    * @param capturedContractIds
    *   contract ids captured by the contract instance
    *   1. the contract ID seen during interpretation
    *   1. the contract ID seen after suffixing (relative)
    */
  case class SingleCreate(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      viewPosition: ViewPosition = rootViewPosition(0, 1),
      viewIndex: Int = 0,
      capturedContractIds: Seq[(LfContractId, LfContractId)] = Seq.empty,
      signatories: Set[LfPartyId] = Set(submitter),
      observers: Set[LfPartyId] = Set(observer),
      key: Option[LfGlobalKeyWithMaintainers] = None,
  ) extends SingleNode(Some(seed)) {

    private def interpretedCapturedContractIds: Seq[LfContractId] = capturedContractIds.map(_._1)
    private def relativeCapturedContractIds: Seq[LfContractId] = capturedContractIds.map(_._2)
    private def absoluteCapturedContractIds: Seq[LfContractId] =
      relativeCapturedContractIds.map(cid =>
        absolutizer(transactionId)
          .absolutizeContractId(cid)
          .valueOr(err =>
            throw new IllegalArgumentException(s"Cannot absolutize contract id $cid: $err")
          )
      )

    lazy val relativeContractInstance: LfThinContractInst =
      ExampleTransactionFactory.contractInstance(relativeCapturedContractIds)

    override val absoluteContractInstance: LfThinContractInst =
      ExampleTransactionFactory.contractInstance(absoluteCapturedContractIds)

    lazy val interpretedContractId: LfContractId = localContractId(discriminator)

    lazy val createInfo: CreateInfo = fromLocalContractId(
      viewPosition,
      viewIndex,
      0,
      relativeContractInstance,
      interpretedContractId,
      signatories,
      observers,
      key,
    )

    override def relativizedContractId: LfContractId = createInfo.relativeContractId

    override def relativeAuthenticationData: ContractAuthenticationData =
      createInfo.relativeAuthenticationData

    override lazy val absoluteAuthenticationData: ContractAuthenticationData = {
      val (authData, absContractId) = toAbsolute(transactionId, createInfo.relativeFci)
      // Sanity check that absolutization works as expected
      require(
        absContractId == absolutizedContractId,
        s"recomputed absolutization mismatch: $absContractId vs $absolutizedContractId",
      )
      authData
    }

    private def discriminator: LfHash =
      ExampleTransactionFactory.this.discriminator(seed, signatories union observers)

    override def toString: String = {
      val captured =
        if (capturedContractIds.nonEmpty) s", capturing ${capturedContractIds.size} ids" else ""

      s"single create$captured"
    }

    override def lfNode: LfNodeCreate =
      createNode(
        interpretedContractId,
        ExampleTransactionFactory.contractInstance(interpretedCapturedContractIds),
        signatories,
        observers,
        key,
      )

    override def node: LfNodeCreate =
      createNode(absolutizedContractId, absoluteContractInstance, signatories, observers, key)

    override def reinterpretedNode: LfNodeCreate =
      createNode(interpretedContractId, relativeContractInstance, signatories, observers, key)

    override def relativeNode: LfNodeCreate =
      createNode(relativizedContractId, relativeContractInstance, signatories, observers, key)

    override def consuming: Boolean = false
  }

  abstract class SingleUseNode(
      nodeSeed: Option[LfHash],
      authenticationData: Either[
        Salt,
        (ContractAuthenticationData, Eval[ContractAuthenticationData]),
      ],
  ) extends SingleNode(nodeSeed) {
    override def relativeAuthenticationData: ContractAuthenticationData =
      authenticationData match {
        case Left(salt) => asAuthenticationData(salt, cantonContractIdVersion, None)
        case Right((relative, _)) => relative
      }

    override def absoluteAuthenticationData: ContractAuthenticationData =
      authenticationData match {
        case Left(salt) =>
          asAuthenticationData(salt, cantonContractIdVersion, Some(TransactionId.zero))
        case Right((_, absoluteE)) => absoluteE.value
      }
  }

  /** Single fetch with [[submitter]] as signatory and [[observer]] as observer and acting party.
    *
    * @param interpretedContractId
    *   id of the fetched contract
    * @param relativizedContractId
    *   id of the fetched contract after suffixing
    * @param fetchedContractInstance
    *   instance of the used contract.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleFetch(
      override val nodeId: LfNodeId = LfNodeId(0),
      interpretedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      relativizedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      fetchedContractInstance: LfThinContractInst = contractInstance(),
      version: LfLanguageVersion = transactionVersion,
      authenticationData: Either[
        Salt,
        (ContractAuthenticationData, Eval[ContractAuthenticationData]),
      ] = Left(TestSalt.generateSalt(random.nextInt())),
  ) extends SingleUseNode(None, authenticationData) {

    override def relativeContractInstance: LfThinContractInst = fetchedContractInstance
    override def absoluteContractInstance: LfThinContractInst = fetchedContractInstance

    override def toString: String = "single fetch"

    private def genNode(id: LfContractId): LfNodeFetch =
      fetchNode(
        id,
        actingParties = Set(observer),
        signatories = Set(submitter),
        observers = Set(observer),
        version = version,
      )

    override def node: LfNodeFetch = genNode(absolutizedContractId)
    override def lfNode: LfNodeFetch = genNode(interpretedContractId)
    override def reinterpretedNode: LfNodeFetch = genNode(relativizedContractId)

    override def consuming: Boolean = false
  }

  /** Single consuming exercise without children with [[submitter]] as signatory, acting party and
    * controller, and [[observer]] as observer.
    *
    * @param interpretedContractId
    *   id of the exercised contract during interpretation
    * @param relativizedContractId
    *   id of the exercised contract after suffixing
    * @param inputContractInstance
    *   instance of the used contract.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleExercise(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      interpretedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      relativizedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      inputContractInstance: LfThinContractInst = contractInstance(),
      authenticationData: Either[
        Salt,
        (ContractAuthenticationData, Eval[ContractAuthenticationData]),
      ] = Left(TestSalt.generateSalt(random.nextInt())),
  ) extends SingleUseNode(Some(seed), authenticationData) {
    override def toString: String = "single exercise"

    override def relativeContractInstance: LfThinContractInst = inputContractInstance
    override def absoluteContractInstance: LfThinContractInst = inputContractInstance

    private def genNode(id: LfContractId): LfNodeExercises =
      exerciseNodeWithoutChildren(
        targetCoid = id,
        actingParties = Set(submitter),
        signatories = Set(submitter),
        observers = Set(observer),
      )

    override def node: LfNodeExercises = genNode(absolutizedContractId)
    override def lfNode: LfNodeExercises = genNode(interpretedContractId)
    override def reinterpretedNode: LfNodeExercises = genNode(relativizedContractId)

    override def consuming: Boolean = true
  }

  /** Single consuming exercise without children without any acting party or signatory, and
    * [[observer]] as observer.
    *
    * @param interpretedContractId
    *   id of the exercised contract during interpretation
    * @param relativizedContractId
    *   id of the exercised contract after suffixing
    * @param inputContractInstance
    *   instance of the used contract.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleExerciseWithoutConfirmingParties(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      interpretedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      relativizedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      inputContractInstance: LfThinContractInst = contractInstance(),
      authenticationData: Either[
        Salt,
        (ContractAuthenticationData, Eval[ContractAuthenticationData]),
      ] = Left(TestSalt.generateSalt(random.nextInt())),
  ) extends SingleUseNode(Some(seed), authenticationData) {
    override def toString: String = "single exercise"

    override def relativeContractInstance: LfThinContractInst = inputContractInstance
    override def absoluteContractInstance: LfThinContractInst = inputContractInstance

    private def genNode(id: LfContractId): LfNodeExercises =
      exerciseNodeWithoutChildren(
        targetCoid = id,
        actingParties = Set.empty,
        signatories = Set.empty,
        observers = Set(observer),
      )

    override def node: LfNodeExercises = genNode(absolutizedContractId)
    override def lfNode: LfNodeExercises = genNode(interpretedContractId)
    override def reinterpretedNode: LfNodeExercises = genNode(relativizedContractId)

    override def consuming: Boolean = true
  }

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class UpgradedSingleExercise(
      seed: LfHash,
      nodeId: LfNodeId = LfNodeId(0),
      interpretedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      relativizedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      override val relativeContractInstance: LfThinContractInst =
        ExampleTransactionFactory.contractInstance(),
      authenticationData: Either[
        Salt,
        (ContractAuthenticationData, Eval[ContractAuthenticationData]),
      ] = Left(TestSalt.generateSalt(random.nextInt())),
      consuming: Boolean = true,
  ) extends SingleUseNode(Some(seed), authenticationData) {
    val upgradedTemplateId: canton.protocol.LfTemplateId =
      templateId.copy(pkg = upgradePackageId)
    private def genNode(id: LfContractId): LfNodeExercises =
      exerciseNode(targetCoid = id, templateId = upgradedTemplateId, signatories = Set(submitter))
    override def node: LfNodeExercises = genNode(absolutizedContractId)
    override def lfNode: LfNodeExercises = genNode(interpretedContractId)
    override def reinterpretedNode: LfNodeExercises = genNode(relativizedContractId)

    override def absoluteContractInstance: LfThinContractInst = relativeContractInstance
  }

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleExerciseWithNonstakeholderActor(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      interpretedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      relativizedContractId: LfContractId = suffixedId(-1, 0, cantonContractIdVersion),
      inputContractInstance: LfThinContractInst = contractInstance(),
      authenticationData: Either[
        Salt,
        (ContractAuthenticationData, Eval[ContractAuthenticationData]),
      ] = Left(TestSalt.generateSalt(random.nextInt())),
  ) extends SingleUseNode(Some(seed), authenticationData) {

    override def relativeContractInstance: LfThinContractInst = inputContractInstance
    override def absoluteContractInstance: LfThinContractInst = inputContractInstance

    private def genNode(id: LfContractId): LfActionNode =
      exerciseNodeWithoutChildren(
        id,
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(observer),
      )
    override def node: LfActionNode = genNode(absolutizedContractId)
    override def lfNode: LfActionNode = genNode(interpretedContractId)
    override def reinterpretedNode: LfActionNode = genNode(relativizedContractId)

    override def consuming: Boolean = true

    override def toString: String = "single exercise with a non-stakeholder actor"

  }

  def absolutizeAuthenticationData(
      transactionId: TransactionId,
      createInfo: CreateInfo,
  ): ContractAuthenticationData = {
    val absoluteFci = absolutizer(transactionId).absolutizeFci(createInfo.relativeFci).value
    ContractInstance.contractAuthenticationData(absoluteFci).value
  }

  /** Transaction structure:
    *   - 0. create
    *   - \1. create capturing 0.
    *   - 2. fetch
    *   - 3. fetch 0.
    *   - 4. exercise
    *   - 5. exercise 1.
    */
  case object MultipleRoots extends ExampleTransaction {

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "multiple roots"

    private val rootViewCount: Int = 6

    private val create0: SingleCreate =
      SingleCreate(
        seed = deriveNodeSeed(0),
        nodeId = LfNodeId(0),
        viewPosition = rootViewPosition(0, rootViewCount),
      )
    private val create1: SingleCreate = SingleCreate(
      seed = deriveNodeSeed(1),
      nodeId = LfNodeId(1),
      viewIndex = 1,
      viewPosition = rootViewPosition(1, rootViewCount),
      capturedContractIds = Seq(
        suffixedId(-1, 1, cantonContractIdVersion) -> suffixedId(-1, 1, cantonContractIdVersion),
        create0.interpretedContractId -> create0.relativizedContractId,
      ),
    )
    private val fetch2: SingleFetch = SingleFetch(LfNodeId(2), suffixedId(-1, 2), suffixedId(-1, 2))
    private val fetch3: SingleFetch =
      SingleFetch(
        nodeId = LfNodeId(3),
        interpretedContractId = create0.interpretedContractId,
        relativizedContractId = create0.relativizedContractId,
        fetchedContractInstance = create0.relativeContractInstance,
        // ensure we test merging transactions with different versions
        version = LfLanguageVersion.v2_dev,
        authenticationData = Right(
          create0.relativeAuthenticationData ->
            Eval.later(absolutizeAuthenticationData(transactionId, create0.createInfo))
        ),
      )
    private val exercise4: SingleExercise =
      SingleExercise(deriveNodeSeed(4), LfNodeId(4), suffixedId(-1, 4), suffixedId(-1, 4))
    private val exercise5: SingleExercise = SingleExercise(
      seed = deriveNodeSeed(5),
      nodeId = LfNodeId(5),
      interpretedContractId = create1.interpretedContractId,
      relativizedContractId = create1.relativizedContractId,
      inputContractInstance = create1.relativeContractInstance,
      authenticationData = Right(
        create1.relativeAuthenticationData ->
          Eval.later(absolutizeAuthenticationData(transactionId, create1.createInfo))
      ),
    )

    private val examples: List[SingleNode] =
      List[SingleNode](create0, create1, fetch2, fetch3, exercise4, exercise5).take(rootViewCount)
    require(examples.sizeIs == rootViewCount)

    override def metadata: TransactionMetadata = mkMetadata(
      examples.zipWithIndex.mapFilter { case (node, index) =>
        node.nodeSeed.map(seed => LfNodeId(index) -> seed)
      }.toMap
    )

    override def versionedUnsuffixedTransaction: LfVersionedTransaction =
      transaction(examples.map(_.nodeId.index), examples.map(_.lfNode)*)

    override def keyResolver: LfKeyResolver = Map.empty // No keys involved here

    override def rootViewDecompositions: Seq[NewView] =
      examples.flatMap(_.rootViewDecompositions)

    override lazy val rootViews: Seq[TransactionView] = examples.zipWithIndex.map {
      case (ex, index) =>
        view(
          ex.relativeNode,
          index,
          ex.consumed,
          ex.used,
          ex.created,
          Map.empty,
          ex.nodeSeed,
          isRoot = true,
          Set.empty,
        )
    }

    override def viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])] =
      rootViews.map(view => view -> Seq(view))

    override def transactionTree: GenTransactionTree = genTransactionTree(rootViews*)

    override def fullInformeeTree: FullInformeeTree =
      mkFullInformeeTree(rootViews.map(blindedForInformeeTree(_))*)

    override def reinterpretedSubtransactions: Seq[
      (
          FullTransactionViewTree,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
          Witnesses,
      )
    ] = {
      val blindedRootViews = rootViews.map(blinded)
      examples.zipWithIndex.map { case (example, i) =>
        val rootViewsWithOneViewUnblinded = blindedRootViews.updated(i, rootViews(i))
        (
          rootTransactionViewTree(rootViewsWithOneViewUnblinded*),
          (transactionFrom(Seq(i), i, example.reinterpretedNode), example.metadata, Map.empty),
          Witnesses(
            NonEmpty(
              List,
              example.view0.viewCommonData.tryUnwrap.viewConfirmationParameters.informees,
            )
          ),
        )
      }
    }

    override def rootTransactionViewTrees: Seq[FullTransactionViewTree] = transactionViewTrees

    override def versionedSuffixedTransaction: LfVersionedTransaction = {
      val relativeNodes = examples.map(_.relativeNode)
      val abs = absolutizer(transactionId)
      val absoluteNodes = relativeNodes.map(abs.absolutizeNode(_).value)
      transaction(0 until rootViewCount, absoluteNodes*)
    }

    override def usedAndCreated: UsedAndCreatedContracts = UsedAndCreatedContracts.empty.copy(
      checkActivenessTxInputs = Set(fetch2.absolutizedContractId, exercise4.absolutizedContractId),
      consumedInputsOfHostedStakeholders =
        Map(exercise4.absolutizedContractId -> exercise4.allInformees),
      used = Map(
        fetch2.absolutizedContractId -> asContractInstance(
          fetch2.absolutizedContractId,
          fetch2.absoluteContractInstance,
          metadataFromFetch(fetch2.node),
        )(fetch2.absoluteAuthenticationData),
        fetch3.absolutizedContractId -> asContractInstance(
          fetch3.absolutizedContractId,
          fetch3.absoluteContractInstance,
          metadataFromFetch(fetch3.node),
        )(fetch3.absoluteAuthenticationData),
        exercise4.absolutizedContractId -> asContractInstance(
          exercise4.absolutizedContractId,
          exercise4.absoluteContractInstance,
          metadataFromExercise(exercise4.node),
        )(exercise4.absoluteAuthenticationData),
        exercise5.absolutizedContractId -> asContractInstance(
          exercise5.absolutizedContractId,
          exercise5.absoluteContractInstance,
          metadataFromExercise(exercise5.node),
        )(exercise5.absoluteAuthenticationData),
      ),
      maybeCreated = Map(
        create0.absolutizedContractId -> Some(
          asContractInstance(
            create0.absolutizedContractId,
            create0.absoluteContractInstance,
            metadataFromCreate(create0.node),
          )(create0.absoluteAuthenticationData)
        ),
        create1.absolutizedContractId -> Some(
          asContractInstance(
            create1.absolutizedContractId,
            create1.absoluteContractInstance,
            metadataFromCreate(create1.node),
          )(create1.absoluteAuthenticationData)
        ),
      ),
      transient = Map(create1.absolutizedContractId -> create1.allInformees),
    )
  }

  /** Transaction structure:
    *   - 0. create
    *   - \1. exercise absolute
    *     - 1.0. create
    *     - 1.1. fetch 1.0.
    *     - 1.2. create
    *     - 1.3. exercise absolute
    *
    * In this specific scenario we make sure informees and quorums for action nodes 1.0, 1.1. and
    * 1.3 are correctly merged to the parent view (v1):
    *   - 0. View0
    *   - \1. View1
    *     - 1.2 View10
    */
  case object MultipleRootsAndSimpleViewNesting extends ExampleTransaction {
    override def cryptoOps: HashOps & RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with multiple roots and a simple view nesting"

    val create0Agreement = "create0"
    def create0Inst: LfThinContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0local: LfContractId = localContractId(
      discriminator(create0seed, Set(submitter, observer))
    )
    def genCreate0(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create0Inst,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val lfCreate0: LfNodeCreate = genCreate0(create0local)

    def genExercise1(cid: LfContractId): LfNodeExercises =
      exerciseNode(
        cid,
        children = List(2, 3, 4, 5).map(nodeId),
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(submitter),
      )

    val lfExercise1Id: LfContractId = suffixedId(-1, 0)
    val lfExercise1: LfNodeExercises = genExercise1(lfExercise1Id)

    def create10Inst: LfThinContractInst = contractInstance()
    def create12Inst: LfThinContractInst = contractInstance()
    def genCreate10(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create10Inst,
        signatories = Set(submitter, signatory, signatoryReplica),
      )

    def genCreate12(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create12Inst,
        signatories = Set(submitter, signatory, extra),
      )

    val create10Agreement = "create10"
    val create10seed: LfHash = deriveNodeSeed(1, 0)
    val create10local: LfContractId = localContractId(
      discriminator(create10seed, Set(submitter, signatory, signatoryReplica))
    )

    val lfCreate10: LfNodeCreate = genCreate10(create10local)
    val create12Agreement = "create12"
    val create12seed: LfHash = deriveNodeSeed(1, 2)
    val create12local: LfContractId = localContractId(
      discriminator(create12seed, Set(submitter, signatory, extra))
    )
    val lfCreate12: LfNodeCreate = genCreate12(create12local)

    def genFetch11(cid: LfContractId): LfNodeFetch =
      fetchNode(
        cid,
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(submitter),
      )
    val lfFetch11: LfNodeFetch = genFetch11(lfCreate10.coid)

    def genExercise13(cid: LfContractId): LfNodeExercises =
      exerciseNode(
        cid,
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(submitter),
      )

    val lfExercise13Id: LfContractId = suffixedId(-1, 0)
    val lfExercise13: LfNodeExercises = genExercise13(lfExercise13Id)

    override lazy val versionedUnsuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1),
        lfCreate0,
        lfExercise1,
        lfCreate10,
        lfFetch11,
        lfCreate12,
        lfExercise13,
      )

    val exercise1seed: LfHash = deriveNodeSeed(1)
    val exercise13seed: LfHash = deriveNodeSeed(1, 3)

    override lazy val metadata: TransactionMetadata = mkMetadata(
      Map(
        LfNodeId(0) -> create0seed,
        LfNodeId(1) -> exercise1seed,
        LfNodeId(2) -> create10seed,
        LfNodeId(4) -> create12seed,
        LfNodeId(5) -> exercise13seed,
      )
    )

    override def keyResolver: LfKeyResolver = Map.empty // No keys involved here

    override lazy val rootViewDecompositions: Seq[NewView] = {
      val v0 = awaitCreateNewView(
        lfCreate0,
        Some(create0seed),
        LfNodeId(0),
        Seq.empty,
        isRoot = true,
      )

      val v10 = awaitCreateNewView(
        lfCreate12,
        Some(create12seed),
        LfNodeId(4),
        Seq.empty,
        isRoot = false,
      )

      /* if running [[com.digitalasset.canton.version.ProtocolVersion.v6]] the create action 1.2
       * spawns a new view because the child's informee participants are not a subset of the parents'
       * informee participants (i.e. party <<extra>> is hosted in the <<extraParticipant>>)
       */
      val v1TailNodes = Seq(
        SameView(lfCreate10, LfNodeId(2), RollbackContext.empty),
        SameView(lfFetch11, LfNodeId(3), RollbackContext.empty),
        v10,
        SameView(LfTransactionUtil.lightWeight(lfExercise13), LfNodeId(5), RollbackContext.empty),
      )

      val v1Pre =
        awaitCreateNewView(
          LfTransactionUtil.lightWeight(lfExercise1),
          Some(exercise1seed),
          LfNodeId(1),
          v1TailNodes,
          isRoot = true,
        )

      /* if running [[com.digitalasset.canton.version.ProtocolVersion.v6]] the merged action nodes'
       * informees and quorums for v1 must be added to the parent's informees and quorums
       */
      val (v1Informees, v1Quorums) = {
        val nodesNotChildren = v1Pre.childViews.flatMap(_.tailNodes.map(_.nodeId))

        val informeesAux = v1TailNodes.flatMap {
          case SameView(lfNode, nodeId, _) if !nodesNotChildren.contains(nodeId) =>
            lfNode.informeesOfNode
          case _ => Set.empty
        }.toSet ++ v1Pre.viewConfirmationParameters.informees

        val quorumsAux =
          (v1Pre.viewConfirmationParameters.quorums ++ v1TailNodes.mapFilter {
            case SameView(lfNode, nodeId, _) if !nodesNotChildren.contains(nodeId) =>
              val confirmingParties =
                LfTransactionUtil.signatoriesOrMaintainers(lfNode) | LfTransactionUtil
                  .actingParties(lfNode)
              Some(
                Quorum(
                  confirmers = confirmingParties.map(pId => pId -> PositiveInt.one).toMap,
                  threshold = NonNegativeInt.tryCreate(confirmingParties.size),
                )
              )
            case _ => None
          }).distinct

        (informeesAux, quorumsAux)
      }

      val v1 = v1Pre.copy(viewConfirmationParameters =
        ViewConfirmationParameters.tryCreate(
          v1Informees,
          v1Quorums,
        )
      )

      Seq(v0, v1)
    }

    // Nodes with translated contract ids
    val create0Info: CreateInfo = fromLocalContractId(
      rootViewPosition(0, 2),
      0,
      0,
      create0Inst,
      create0local,
      signatories = Set(submitter),
      observers = Set(observer),
    )
    val create0Relative: LfNodeCreate = genCreate0(create0Info.relativeContractId)

    val exercise1Agreement = "exercise1"
    val exercise1Id: LfContractId = suffixedId(-1, 0)
    val exercise1: LfNodeExercises = genExercise1(exercise1Id)
    val exercise1Instance: LfThinContractInst = contractInstance()

    val create10Info: CreateInfo =
      fromLocalContractId(
        rootViewPosition(1, 2),
        1,
        0,
        create10Inst,
        create10local,
        signatories = Set(submitter, signatory, signatoryReplica),
      )
    val create10Relative: LfNodeCreate = genCreate10(create10Info.relativeContractId)

    val fetch11: LfNodeFetch = lfFetch11

    val create12Info: CreateInfo =
      fromLocalContractId(
        subViewIndex(0, 1) +: rootViewPosition(1, 2),
        2,
        0,
        create12Inst,
        create12local,
        signatories = Set(submitter, signatory, extra),
      )
    val create12Relative: LfNodeCreate = genCreate12(create12Info.relativeContractId)

    // TODO(#23971) Use a different contract ID than exercise1Id here
    val exercise13Id: LfContractId = suffixedId(-1, 0)
    val exercise13: LfNodeExercises = genExercise13(exercise13Id)

    // Views
    val view0: TransactionView =
      view(
        create0Relative,
        0,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create0Relative, create0Info.relativeAuthenticationData)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )

    val view10: TransactionView =
      view(
        create12Relative,
        2,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create12Relative, create12Info.relativeAuthenticationData)),
        Map.empty,
        Some(create12seed),
        isRoot = false,
        Set.empty,
      )

    val exercise1Input = asContractInstance(
      exercise1Id,
      exercise1Instance,
      metadataFromExercise(exercise1),
      CreationTime.CreatedAt(ledgerTime.toLf),
    )()

    val view1: TransactionView =
      viewWithInformeesMerge(
        exercise1,
        Seq[LfActionNode](create10Relative, fetch11, exercise13),
        1,
        Set(exercise1Id, exercise13Id),
        Seq(exercise1Input),
        Seq(instanceFromCreate(create10Relative, create10Info.relativeAuthenticationData)),
        Map.empty,
        Some(deriveNodeSeed(1)),
        isRoot = true,
        Set.empty,
        view10,
      )

    override lazy val rootViews: Seq[TransactionView] = Seq(view0, view1)

    override lazy val viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])] =
      Seq(
        view0 -> Seq(view0),
        view1 -> Seq(view1, view10),
        view10 -> Seq(view10),
      )

    override lazy val transactionTree: GenTransactionTree = genTransactionTree(view0, view1)

    override lazy val fullInformeeTree: FullInformeeTree =
      mkFullInformeeTree(
        blindedForInformeeTree(view0),
        blindedForInformeeTree(
          view1,
          blindedForInformeeTree(view10),
        ),
      )

    val transactionViewTree0: FullTransactionViewTree =
      rootTransactionViewTree(view0, blinded(view1))

    val transactionViewTree1: FullTransactionViewTree =
      rootTransactionViewTree(blinded(view0), view1)

    val transactionViewTree10: FullTransactionViewTree =
      nonRootTransactionViewTree(blinded(view0), leafsBlinded(view1, view10))

    override lazy val reinterpretedSubtransactions: Seq[
      (
          FullTransactionViewTree,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
          Witnesses,
      )
    ] =
      Seq(
        (
          transactionViewTree0,
          (
            transaction(Seq(0), lfCreate0),
            mkMetadata(seeds.filter(_._1 == LfNodeId(0))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree0.informees)),
        ),
        (
          transactionViewTree1,
          (
            transactionFrom(
              Seq(1),
              1,
              exercise1,
              lfCreate10,
              lfFetch11,
              lfCreate12,
              lfExercise13,
            ),
            mkMetadata(
              seeds.filter(seed => Seq(1, 2, 3, 4, 5).map(LfNodeId.apply).contains(seed._1))
            ),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree10,
          (
            transactionFrom(Seq(4), 4, lfCreate12),
            mkMetadata(seeds.filter(_._1 == LfNodeId(4))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree10.informees, transactionViewTree1.informees)),
        ),
      )

    override lazy val rootTransactionViewTrees: Seq[FullTransactionViewTree] =
      Seq(transactionViewTree0, transactionViewTree1)

    val (create0auth, create0Absolute) = toAbsolute(transactionId, create0Info.relativeFci)
    val create0: LfNodeCreate = genCreate0(create0Absolute)
    val (create10auth, create10Absolute) = toAbsolute(transactionId, create10Info.relativeFci)
    val create10: LfNodeCreate = genCreate10(create10Absolute)
    val (create12auth, create12Absolute) = toAbsolute(transactionId, create12Info.relativeFci)
    val create12: LfNodeCreate = genCreate12(create12Absolute)
    val fetch11Abs: LfNodeFetch = genFetch11(create10Absolute)

    override lazy val versionedSuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1),
        create0,
        exercise1,
        create10,
        fetch11Abs,
        create12,
        exercise13,
      )

    override def usedAndCreated: UsedAndCreatedContracts = {
      val create0Instance =
        asContractInstance(create0Absolute, create0.versionedCoinst, metadataFromCreate(create0))(
          create0auth
        )
      val create10Instance = asContractInstance(
        create10Absolute,
        create10.versionedCoinst,
        metadataFromCreate(create10),
      )(create10auth)
      val create12Instance = asContractInstance(
        create12Absolute,
        create12.versionedCoinst,
        metadataFromCreate(create12),
      )(create12auth)

      UsedAndCreatedContracts.empty.copy(
        checkActivenessTxInputs = Set(exercise1Id, exercise13Id),
        consumedInputsOfHostedStakeholders = Map(
          exercise1Id -> exercise1.stakeholders,
          exercise13Id -> exercise13.stakeholders,
        ),
        used = Map(exercise1Id -> exercise1Input),
        maybeCreated = Map(
          create0Absolute -> Some(create0Instance),
          create10Absolute -> Some(create10Instance),
          create12Absolute -> Some(create12Instance),
        ),
      )
    }
  }

  /** Transaction structure:
    *   - 0. create
    *   - \1. exercise absolute
    *     - 1.0. create
    *     - 1.1. fetch 1.0.
    *     - 1.2. create
    *     - 1.3. exercise 1.2.
    *       - 1.3.0. create
    *       - 1.3.1. exercise absolute
    *         - 1.3.1.0 create
    *
    * View structure:
    *   - 0. View0
    *   - \1. View1
    *     - 1.3.0. View10
    *     - 1.3.1. View11
    *       - 1.3.1.0 View110
    */
  case object MultipleRootsAndViewNestings extends ExampleTransaction {

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with multiple roots and view nestings"

    def create0Inst: LfThinContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0local: LfContractId = localContractId(
      discriminator(deriveNodeSeed(0), Set(submitter, observer))
    )
    def genCreate0(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create0Inst,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val lfCreate0: LfNodeCreate = genCreate0(create0local)

    def genExercise1(cid: LfContractId): LfNodeExercises =
      exerciseNode(
        cid,
        children = List(2, 3, 4, 5).map(nodeId),
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(submitter),
      )

    val lfExercise1Id: LfContractId = suffixedId(-1, 0)
    val lfExercise1: LfNodeExercises = genExercise1(lfExercise1Id)

    def create10Inst: LfThinContractInst = contractInstance()
    def create12Inst: LfThinContractInst = contractInstance()
    def genCreate1x(cid: LfContractId, contractInstance: LfThinContractInst): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInstance,
        signatories = Set(submitter, signatory),
      )

    val create10seed: LfHash = deriveNodeSeed(1, 0)
    val create10local: LfContractId = localContractId(
      discriminator(create10seed, Set(submitter, signatory))
    )
    val lfCreate10: LfNodeCreate = genCreate1x(create10local, create10Inst)
    val create12seed: LfHash = deriveNodeSeed(1, 2)
    val create12local: LfContractId = localContractId(
      discriminator(create12seed, Set(submitter, signatory))
    )
    val lfCreate12: LfNodeCreate = genCreate1x(create12local, create12Inst)

    def genFetch11(cid: LfContractId): LfNodeFetch =
      fetchNode(
        cid,
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(submitter),
      )
    val lfFetch11: LfNodeFetch = genFetch11(lfCreate10.coid)

    def genExercise13(cid: LfContractId): LfNodeExercises =
      exerciseNode(
        cid,
        children = List(nodeId(6), nodeId(7)),
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(submitter),
      )
    val lfExercise13: LfNodeExercises = genExercise13(lfCreate12.coid)

    def create130Inst: LfThinContractInst = contractInstance()
    val create130seed: LfHash = deriveNodeSeed(1, 3, 0)
    def genCreate130(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create130Inst,
        signatories = Set(signatory),
        observers = Set(extra),
      )
    val create130local: LfContractId = localContractId(
      discriminator(create130seed, Set(signatory, extra))
    )
    val lfCreate130: LfNodeCreate = genCreate130(create130local)

    def genExercise131(cid: LfContractId): LfNodeExercises =
      exerciseNode(
        cid,
        children = List(nodeId(8)),
        actingParties = Set(signatory),
        signatories = Set(submitter),
        observers = Set(observer),
      )

    val lfExercise131Id: LfContractId = suffixedId(-1, 1)
    val lfExercise131: LfNodeExercises = genExercise131(lfExercise131Id)

    def create1310Inst: LfThinContractInst = contractInstance()
    val create1310seed: LfHash = deriveNodeSeed(1, 3, 1, 0)
    def genCreate1310(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create1310Inst,
        signatories = Set(submitter),
        observers = Set(extra),
      )
    val create1310local: LfContractId = localContractId(
      discriminator(create1310seed, Set(submitter, extra))
    )
    val lfCreate1310: LfNodeCreate = genCreate1310(create1310local)

    override lazy val versionedUnsuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1),
        lfCreate0,
        lfExercise1,
        lfCreate10,
        lfFetch11,
        lfCreate12,
        lfExercise13,
        lfCreate130,
        lfExercise131,
        lfCreate1310,
      )

    val exercise1seed: LfHash = deriveNodeSeed(1)
    val exercise13seed: LfHash = deriveNodeSeed(1, 3)
    val exercise131seed: LfHash = deriveNodeSeed(1, 3, 1)

    override lazy val metadata: TransactionMetadata = mkMetadata(
      Map(
        LfNodeId(0) -> create0seed,
        LfNodeId(1) -> exercise1seed,
        LfNodeId(2) -> create10seed,
        LfNodeId(4) -> create12seed,
        LfNodeId(5) -> exercise13seed,
        LfNodeId(6) -> create130seed,
        LfNodeId(7) -> exercise131seed,
        LfNodeId(8) -> create1310seed,
      )
    )

    override def keyResolver: LfKeyResolver = Map.empty // No keys involved here

    override lazy val rootViewDecompositions: Seq[NewView] = {
      val v0 = awaitCreateNewView(
        lfCreate0,
        Some(create0seed),
        LfNodeId(0),
        Seq.empty,
        isRoot = true,
      )

      val v10 = awaitCreateNewView(
        lfCreate130,
        Some(create130seed),
        LfNodeId(6),
        Seq.empty,
        isRoot = false,
      )

      val v110 = awaitCreateNewView(
        lfCreate1310,
        Some(create1310seed),
        LfNodeId(8),
        Seq.empty,
        isRoot = false,
      )

      val v11 = awaitCreateNewView(
        LfTransactionUtil.lightWeight(lfExercise131),
        Some(exercise131seed),
        LfNodeId(7),
        Seq(v110),
        isRoot = false,
      )

      val v1TailNodes = Seq(
        SameView(lfCreate10, LfNodeId(2), RollbackContext.empty),
        SameView(lfFetch11, LfNodeId(3), RollbackContext.empty),
        SameView(lfCreate12, LfNodeId(4), RollbackContext.empty),
        SameView(LfTransactionUtil.lightWeight(lfExercise13), LfNodeId(5), RollbackContext.empty),
        v10,
        v11,
      )
      val v1 =
        awaitCreateNewView(
          LfTransactionUtil.lightWeight(lfExercise1),
          Some(exercise1seed),
          LfNodeId(1),
          v1TailNodes,
          isRoot = true,
        )

      Seq(v0, v1)
    }

    // Nodes with translated contract ids
    val create0Info: CreateInfo = fromLocalContractId(
      rootViewPosition(0, 2),
      0,
      0,
      create0Inst,
      create0local,
      signatories = Set(submitter),
      observers = Set(observer),
    )
    val create0Relative: LfNodeCreate = genCreate0(create0Info.relativeContractId)

    val exercise1Id: LfContractId = suffixedId(-1, 0)
    val exercise1: LfNodeExercises = genExercise1(exercise1Id)
    val exercise1Instance: LfThinContractInst = contractInstance()

    val create10Info: CreateInfo = fromLocalContractId(
      rootViewPosition(1, 2),
      1,
      0,
      create10Inst,
      create10local,
      signatories = Set(submitter, signatory),
    )
    val create10Relative: LfNodeCreate = genCreate1x(create10Info.relativeContractId, create10Inst)

    val fetch11: LfNodeFetch = lfFetch11

    val create12Info: CreateInfo = fromLocalContractId(
      rootViewPosition(1, 2),
      1,
      1,
      create12Inst,
      create12local,
      signatories = Set(submitter, signatory),
    )
    val create12Relative: LfNodeCreate = genCreate1x(create12Info.relativeContractId, create12Inst)

    val create130Info: CreateInfo = fromLocalContractId(
      subViewIndex(0, 2) +: rootViewPosition(1, 2),
      2,
      0,
      create130Inst,
      create130local,
      signatories = Set(signatory),
      observers = Set(extra),
    )
    val create130Relative: LfNodeCreate = genCreate130(create130Info.relativeContractId)

    val exercise131Id: LfContractId = suffixedId(-1, 1)
    val exercise131: LfNodeExercises = genExercise131(exercise131Id)
    val exercise131Instance: LfThinContractInst = contractInstance()

    val create1310Info: CreateInfo = fromLocalContractId(
      subViewIndex(0, 1) +: subViewIndex(1, 2) +: rootViewPosition(1, 2),
      4,
      0,
      create1310Inst,
      create1310local,
      signatories = Set(submitter),
      observers = Set(extra),
    )
    val create1310Relative: LfNodeCreate = genCreate1310(create1310Info.relativeContractId)

    // Views
    val view0: TransactionView =
      view(
        create0Relative,
        0,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create0Relative, create0Info.relativeAuthenticationData)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )
    val view10: TransactionView =
      view(
        create130Relative,
        2,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create130Relative, create130Info.relativeAuthenticationData)),
        Map.empty,
        Some(create130seed),
        isRoot = false,
        Set.empty,
      )
    val view110: TransactionView =
      view(
        create1310Relative,
        4,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create1310Relative, create1310Info.relativeAuthenticationData)),
        Map.empty,
        Some(create1310seed),
        isRoot = false,
        Set.empty,
      )

    val exercise131Input = asContractInstance(
      contractId = exercise131Id,
      contractInstance = exercise131Instance,
      metadata = metadataFromExercise(exercise131),
      ledgerTime = CreationTime.CreatedAt(ledgerTime.toLf),
    )()

    val view11: TransactionView =
      view(
        exercise131,
        3,
        Set(exercise131Id),
        Seq(exercise131Input),
        Seq.empty,
        Map.empty,
        Some(deriveNodeSeed(1, 3, 1)),
        isRoot = false,
        Set.empty,
        view110,
      )

    val exercise1Input = asContractInstance(
      exercise1Id,
      exercise1Instance,
      metadataFromExercise(exercise1),
      CreationTime.CreatedAt(ledgerTime.toLf),
    )()

    val view1: TransactionView =
      view(
        exercise1,
        1,
        Set(exercise1Id, create12Info.relativeContractId),
        Seq(exercise1Input),
        Seq(
          instanceFromCreate(create10Relative, create10Info.relativeAuthenticationData),
          instanceFromCreate(create12Relative, create12Info.relativeAuthenticationData),
        ),
        Map.empty,
        Some(deriveNodeSeed(1)),
        isRoot = true,
        Set.empty,
        view10,
        view11,
      )

    override lazy val rootViews: Seq[TransactionView] = Seq(view0, view1)

    override lazy val viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])] =
      Seq(
        view0 -> Seq(view0),
        view1 -> Seq(view1, view10, view11, view110),
        view10 -> Seq(view10),
        view11 -> Seq(view11, view110),
        view110 -> Seq(view110),
      )

    override lazy val transactionTree: GenTransactionTree = genTransactionTree(view0, view1)

    override lazy val fullInformeeTree: FullInformeeTree =
      mkFullInformeeTree(
        blindedForInformeeTree(view0),
        blindedForInformeeTree(
          view1,
          blindedForInformeeTree(view10),
          blindedForInformeeTree(view11, blindedForInformeeTree(view110)),
        ),
      )

    val transactionViewTree0: FullTransactionViewTree =
      rootTransactionViewTree(view0, blinded(view1))

    val transactionViewTree1: FullTransactionViewTree =
      rootTransactionViewTree(blinded(view0), view1)

    val transactionViewTree10: FullTransactionViewTree =
      nonRootTransactionViewTree(blinded(view0), leafsBlinded(view1, view10, blinded(view11)))

    val transactionViewTree11: FullTransactionViewTree =
      nonRootTransactionViewTree(blinded(view0), leafsBlinded(view1, blinded(view10), view11))

    val transactionViewTree110: FullTransactionViewTree =
      nonRootTransactionViewTree(
        blinded(view0),
        leafsBlinded(view1, blinded(view10), leafsBlinded(view11, view110)),
      )

    override lazy val reinterpretedSubtransactions: Seq[
      (
          FullTransactionViewTree,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
          Witnesses,
      )
    ] =
      Seq(
        (
          transactionViewTree0,
          (
            transaction(Seq(0), lfCreate0),
            mkMetadata(seeds.filter(_._1 == LfNodeId(0))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree0.informees)),
        ),
        (
          transactionViewTree1,
          (
            transactionFrom(
              Seq(1),
              1,
              exercise1,
              lfCreate10,
              lfFetch11,
              lfCreate12,
              lfExercise13,
              lfCreate130,
              lfExercise131,
              lfCreate1310,
            ),
            mkMetadata(
              seeds.filter(seed =>
                Seq(1, 2, 3, 4, 5, 6, 7, 8).map(LfNodeId.apply).contains(seed._1)
              )
            ),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree10,
          (
            transactionFrom(Seq(6), 6, lfCreate130),
            mkMetadata(seeds.filter(_._1 == LfNodeId(6))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree10.informees, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree11,
          (
            transactionFrom(Seq(7), 7, lfExercise131, lfCreate1310),
            mkMetadata(seeds.filter(seed => Seq(7, 8).map(LfNodeId.apply).contains(seed._1))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree11.informees, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree110,
          (
            transactionFrom(Seq(8), 8, lfCreate1310),
            mkMetadata(seeds.filter(_._1 == LfNodeId(8))),
            Map.empty,
          ),
          Witnesses(
            NonEmpty(
              List,
              transactionViewTree110.informees,
              transactionViewTree11.informees,
              transactionViewTree1.informees,
            )
          ),
        ),
      )

    override lazy val rootTransactionViewTrees: Seq[FullTransactionViewTree] =
      Seq(transactionViewTree0, transactionViewTree1)

    val (create0auth, create0Absolute) = toAbsolute(transactionId, create0Info.relativeFci)
    val create0: LfNodeCreate = genCreate0(create0Absolute)
    val (create10auth, create10Absolute) = toAbsolute(transactionId, create10Info.relativeFci)
    val create10: LfNodeCreate = genCreate1x(create10Absolute, create10Inst)
    val (create12auth, create12Absolute) = toAbsolute(transactionId, create12Info.relativeFci)
    val create12: LfNodeCreate = genCreate1x(create12Absolute, create12Inst)
    val fetch11Abs: LfNodeFetch = genFetch11(create10Absolute)
    val exercise13Abs: LfNodeExercises = genExercise13(create12Absolute)
    val (create130auth, create130Absolute) = toAbsolute(transactionId, create130Info.relativeFci)
    val create130: LfNodeCreate = genCreate130(create130Absolute)
    val (create1310auth, create1310Absolute) = toAbsolute(transactionId, create1310Info.relativeFci)
    val create1310: LfNodeCreate = genCreate1310(create1310Absolute)

    override lazy val versionedSuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1),
        create0,
        exercise1,
        create10,
        fetch11Abs,
        create12,
        exercise13Abs,
        create130,
        exercise131,
        create1310,
      )

    override def usedAndCreated: UsedAndCreatedContracts = {
      val create0Instance =
        asContractInstance(create0Absolute, create0.versionedCoinst, metadataFromCreate(create0))(
          create0auth
        )
      val create10Instance = asContractInstance(
        create10Absolute,
        create10.versionedCoinst,
        metadataFromCreate(create10),
      )(create10auth)
      val create12Instance = asContractInstance(
        create12Absolute,
        create12.versionedCoinst,
        metadataFromCreate(create12),
      )(create12auth)
      val create130Instance = asContractInstance(
        create130Absolute,
        create130.versionedCoinst,
        metadataFromCreate(create130),
      )(create130auth)
      val create1310Instance = asContractInstance(
        create1310Absolute,
        create1310.versionedCoinst,
        metadataFromCreate(create1310),
      )(create1310auth)

      UsedAndCreatedContracts.empty.copy(
        checkActivenessTxInputs = Set(exercise1Id, exercise131Id),
        consumedInputsOfHostedStakeholders = Map(
          exercise1Id -> exercise1.stakeholders,
          exercise131Id -> exercise131.stakeholders,
        ),
        used = Map(
          exercise1Id -> exercise1Input,
          exercise131Id -> exercise131Input,
        ),
        maybeCreated = Map(
          create0Absolute -> Some(create0Instance),
          create10Absolute -> Some(create10Instance),
          create12Absolute -> Some(create12Instance),
          create130Absolute -> Some(create130Instance),
          create1310Absolute -> Some(create1310Instance),
        ),
        transient = Map(create12Absolute -> create12.stakeholders),
      )
    }
  }

  /** Transaction structure:
    *   - 0. create
    *   - \1. exerciseN
    *     - 1.0. exercise
    *       - 1.0.0. create
    *     - 1.1. create(capturing 1.0.0)
    *     - 1.2. exercise
    *       - 1.2.0. create(capturing 1.0.0)
    *     - 1.3. create(capturing 1.2.0)
    *   - 2. create
    *
    * View structure:
    *   - 0. View0
    *   - \1. View1
    *     - 1.0. View10
    *       - 1.0.0. View100
    *     - 1.2. View11
    *       - 1.2.0. View110
    *   - 2. View2
    */
  case object ViewInterleavings extends ExampleTransaction {

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with subviews and core nodes interleaved"

    def stakeholdersX: Set[LfPartyId] = Set(submitter, observer)
    def genCreateX(
        cid: LfContractId,
        contractInst: LfThinContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = Set(submitter),
        observers = Set(observer),
      )

    val create0Inst: LfThinContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0local: LfContractId = localContractId(discriminator(create0seed, stakeholdersX))
    val lfCreate0: LfNodeCreate = genCreateX(create0local, create0Inst)

    def genExercise1(cid: LfContractId): LfNodeExercises =
      exerciseNode(
        cid,
        children = List(nodeId(2), nodeId(4), nodeId(5), nodeId(7)),
        signatories = Set(signatory),
        observers = Set(
          observer,
          submitter,
        ), // note the observer is not an informee, as the exercise is non-consuming
        actingParties = Set(submitter),
        consuming = false,
      )
    val lfExercise1: LfNodeExercises = genExercise1(suffixedId(-1, 1))

    def genExercise1X(cid: LfContractId, childIndex: Int): LfNodeExercises =
      exerciseNode(
        cid,
        children = List(nodeId(childIndex)),
        signatories = Set(signatory),
        observers = Set(extra),
        actingParties = Set(signatory),
      )

    val lfExercise10: LfNodeExercises = genExercise1X(suffixedId(-1, 10), 3)

    def stakeholders3X: Set[LfPartyId] = Set(signatory, observer)
    def genCreate3X(
        cid: LfContractId,
        contractInst: LfThinContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = Set(signatory),
        observers = Set(observer),
      )

    val create100Inst: LfThinContractInst = contractInstance()
    val create100seed: LfHash = deriveNodeSeed(1, 0, 0)
    val create100local: LfContractId = localContractId(discriminator(create100seed, stakeholders3X))
    val lfCreate100: LfNodeCreate = genCreate3X(create100local, create100Inst)

    def stakeholdersXX: Set[LfPartyId] = Set(signatory, submitter)
    def genCreateXX(
        cid: LfContractId,
        contractInst: LfThinContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = stakeholdersXX,
        observers = Set.empty,
      )

    def genCreate11Inst(capturedId: LfContractId): LfThinContractInst = contractInstance(
      Seq(capturedId)
    )
    val create11seed: LfHash = deriveNodeSeed(1, 1)
    val create11local: LfContractId = localContractId(discriminator(create11seed, stakeholdersXX))
    val lfCreate11: LfNodeCreate =
      genCreateXX(create11local, genCreate11Inst(create100local))

    val lfExercise12: LfNodeExercises = genExercise1X(suffixedId(-1, 12), 6)

    def genCreate120Inst(capturedId: LfContractId): LfThinContractInst = contractInstance(
      Seq(capturedId)
    )
    val lfCreate120Inst: LfThinContractInst = genCreate120Inst(create100local)
    val create120seed: LfHash = deriveNodeSeed(1, 2, 0)
    val create120local: LfContractId = localContractId(discriminator(create120seed, stakeholders3X))
    val lfCreate120: LfNodeCreate = genCreate3X(create120local, lfCreate120Inst)

    def genCreate13Inst(capturedId: LfContractId): LfThinContractInst = contractInstance(
      Seq(capturedId)
    )
    val create13seed: LfHash = deriveNodeSeed(1, 3)
    val create13local: LfContractId = localContractId(discriminator(create13seed, stakeholdersXX))
    val lfCreate13: LfNodeCreate = genCreateXX(create13local, genCreate13Inst(create120local))

    val create2Inst: LfThinContractInst = contractInstance()
    val create2seed: LfHash = deriveNodeSeed(2)
    val create2local: LfContractId = localContractId(discriminator(create2seed, stakeholdersX))
    val lfCreate2: LfNodeCreate = genCreateX(create2local, create2Inst)

    override lazy val versionedUnsuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1, 8),
        lfCreate0,
        lfExercise1,
        lfExercise10,
        lfCreate100,
        lfCreate11,
        lfExercise12,
        lfCreate120,
        lfCreate13,
        lfCreate2,
      )

    val exercise1seed = deriveNodeSeed(1)
    val exercise10seed = deriveNodeSeed(1, 0)
    val exercise12seed = deriveNodeSeed(1, 2)

    override lazy val metadata: TransactionMetadata = mkMetadata(
      Map(
        LfNodeId(0) -> create0seed,
        LfNodeId(1) -> exercise1seed,
        LfNodeId(2) -> exercise10seed,
        LfNodeId(3) -> create100seed,
        LfNodeId(4) -> create11seed,
        LfNodeId(5) -> exercise12seed,
        LfNodeId(6) -> create120seed,
        LfNodeId(7) -> create13seed,
        LfNodeId(8) -> create2seed,
      )
    )

    override def keyResolver: LfKeyResolver = Map.empty // No keys involved here

    override lazy val rootViewDecompositions: Seq[NewView] = {
      val v0 = awaitCreateNewView(
        lfCreate0,
        Some(create0seed),
        LfNodeId(0),
        Seq.empty,
        isRoot = true,
      )

      val v100 = awaitCreateNewView(
        lfCreate100,
        Some(create100seed),
        LfNodeId(3),
        Seq.empty,
        isRoot = false,
      )

      val v10 = awaitCreateNewView(
        LfTransactionUtil.lightWeight(lfExercise10),
        Some(exercise10seed),
        LfNodeId(2),
        Seq(v100),
        isRoot = false,
      )

      val v110 = awaitCreateNewView(
        lfCreate120,
        Some(create120seed),
        LfNodeId(6),
        Seq.empty,
        isRoot = false,
      )

      val v11 = awaitCreateNewView(
        LfTransactionUtil.lightWeight(lfExercise12),
        Some(exercise12seed),
        LfNodeId(5),
        Seq(v110),
        isRoot = false,
      )

      val v1 = awaitCreateNewView(
        LfTransactionUtil.lightWeight(lfExercise1),
        Some(exercise1seed),
        LfNodeId(1),
        Seq(
          v10,
          SameView(lfCreate11, LfNodeId(4), RollbackContext.empty),
          v11,
          SameView(lfCreate13, LfNodeId(7), RollbackContext.empty),
        ),
        isRoot = true,
      )

      val v2 = awaitCreateNewView(
        lfCreate2,
        Some(create2seed),
        LfNodeId(8),
        Seq.empty,
        isRoot = true,
      )

      Seq(v0, v1, v2)
    }

    val create0Info: CreateInfo = fromLocalContractId(
      rootViewPosition(0, 3),
      0,
      0,
      create0Inst,
      create0local,
      signatories = Set(submitter),
      observers = Set(observer),
    )
    val create0Relative: LfNodeCreate = genCreateX(create0Info.relativeContractId, create0Inst)

    val exercise1Id: LfContractId = suffixedId(-1, 1)
    val exercise1: LfNodeExercises = genExercise1(exercise1Id)
    val exercise1Instance: LfThinContractInst = contractInstance()

    val exercise10Id: LfContractId = suffixedId(-1, 10)
    val exercise10: LfNodeExercises = genExercise1X(exercise10Id, 3)
    val exercise10Instance: LfThinContractInst = contractInstance()

    val create100Info: CreateInfo = fromLocalContractId(
      subViewIndex(0, 1) +: subViewIndex(0, 2) +: rootViewPosition(1, 3),
      3,
      0,
      create100Inst,
      create100local,
      signatories = Set(signatory),
      observers = Set(observer),
    )
    val create100Relative: LfNodeCreate =
      genCreate3X(create100Info.relativeContractId, create100Inst)

    val create11InstR: LfThinContractInst = genCreate11Inst(create100Info.relativeContractId)
    val create11Info: CreateInfo = fromLocalContractId(
      rootViewPosition(1, 3),
      1,
      0,
      create11InstR,
      create11local,
      signatories = stakeholdersXX,
    )
    val create11Relative: LfNodeCreate = genCreateXX(create11Info.relativeContractId, create11InstR)

    val exercise12Id: LfContractId = suffixedId(-1, 12)
    val exercise12: LfNodeExercises = genExercise1X(exercise12Id, 6)
    val exercise12Instance: LfThinContractInst = contractInstance()

    val create120Inst: LfThinContractInst = genCreate120Inst(create100Info.relativeContractId)
    val create120Info: CreateInfo = fromLocalContractId(
      subViewIndex(0, 1) +: subViewIndex(1, 2) +: rootViewPosition(1, 3),
      5,
      0,
      create120Inst,
      create120local,
      signatories = Set(signatory),
      observers = Set(observer),
    )
    val create120Relative: LfNodeCreate =
      genCreate3X(create120Info.relativeContractId, create120Inst)

    val create13Inst: LfThinContractInst = genCreate13Inst(create120Info.relativeContractId)
    val create13Info: CreateInfo = fromLocalContractId(
      rootViewPosition(1, 3),
      1,
      1,
      create13Inst,
      create13local,
      signatories = stakeholdersXX,
    )
    val create13Relative: LfNodeCreate = genCreateXX(create13Info.relativeContractId, create13Inst)

    val create2Info: CreateInfo = fromLocalContractId(
      rootViewPosition(2, 3),
      6,
      0,
      create2Inst,
      create2local,
      signatories = Set(submitter),
      observers = Set(observer),
    )
    val create2Relative: LfNodeCreate = genCreateX(create2Info.relativeContractId, create2Inst)

    val view0: TransactionView =
      view(
        create0Relative,
        0,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create0Relative, create0Info.relativeAuthenticationData)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )

    val view100: TransactionView =
      view(
        create100Relative,
        3,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create100Relative, create100Info.relativeAuthenticationData)),
        Map.empty,
        Some(create100seed),
        isRoot = false,
        Set.empty,
      )

    val exercise10Input = asContractInstance(
      exercise10Id,
      exercise10Instance,
      metadataFromExercise(exercise10),
      CreationTime.CreatedAt(ledgerTime.toLf),
    )()

    val view10: TransactionView = view(
      exercise10,
      2,
      Set(exercise10Id),
      Seq(exercise10Input),
      Seq.empty,
      Map.empty,
      Some(deriveNodeSeed(1, 0)),
      isRoot = false,
      Set.empty,
      view100,
    )

    val view110: TransactionView =
      view(
        create120Relative,
        5,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create120Relative, create120Info.relativeAuthenticationData)),
        Map.empty,
        Some(create120seed),
        isRoot = false,
        Set.empty,
      )

    val exercise12Input = asContractInstance(
      exercise12Id,
      exercise12Instance,
      metadataFromExercise(exercise12),
      CreationTime.CreatedAt(ledgerTime.toLf),
    )()

    val view11: TransactionView =
      view(
        exercise12,
        4,
        Set(exercise12Id),
        Seq(exercise12Input),
        Seq.empty,
        Map.empty,
        Some(deriveNodeSeed(1, 2)),
        isRoot = false,
        Set.empty,
        view110,
      )

    private val exercise1Input = asContractInstance(
      exercise1Id,
      exercise1Instance,
      metadataFromExercise(exercise1),
      CreationTime.CreatedAt(ledgerTime.toLf),
    )()

    val view1: TransactionView =
      view(
        exercise1,
        1,
        Set.empty,
        Seq(exercise1Input),
        Seq(
          instanceFromCreate(create11Relative, create11Info.relativeAuthenticationData),
          instanceFromCreate(create13Relative, create13Info.relativeAuthenticationData),
        ),
        Map.empty,
        Some(deriveNodeSeed(1)),
        isRoot = true,
        Set.empty,
        view10,
        view11,
      )

    val view2: TransactionView =
      view(
        create2Relative,
        6,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create2Relative, create2Info.relativeAuthenticationData)),
        Map.empty,
        Some(create2seed),
        isRoot = true,
        Set.empty,
      )

    override lazy val rootViews: Seq[TransactionView] = Seq(view0, view1, view2)

    override lazy val viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])] =
      Seq(
        view0 -> Seq(view0),
        view1 -> Seq(view1, view10, view100, view11, view110),
        view10 -> Seq(view10, view100),
        view100 -> Seq(view100),
        view11 -> Seq(view11, view110),
        view110 -> Seq(view110),
        view2 -> Seq(view2),
      )

    override lazy val transactionTree: GenTransactionTree = genTransactionTree(view0, view1, view2)

    override lazy val fullInformeeTree: FullInformeeTree =
      mkFullInformeeTree(
        blindedForInformeeTree(view0),
        blindedForInformeeTree(
          view1,
          blindedForInformeeTree(view10, blindedForInformeeTree(view100)),
          blindedForInformeeTree(view11, blindedForInformeeTree(view110)),
        ),
        blindedForInformeeTree(view2),
      )

    val transactionViewTree0: FullTransactionViewTree =
      rootTransactionViewTree(view0, blinded(view1), blinded(view2))

    val transactionViewTree1: FullTransactionViewTree =
      rootTransactionViewTree(blinded(view0), view1, blinded(view2))

    val transactionViewTree10: FullTransactionViewTree =
      nonRootTransactionViewTree(
        blinded(view0),
        leafsBlinded(view1, view10, blinded(view11)),
        blinded(view2),
      )

    val transactionViewTree100: FullTransactionViewTree = nonRootTransactionViewTree(
      blinded(view0),
      leafsBlinded(view1, leafsBlinded(view10, view100), blinded(view11)),
      blinded(view2),
    )

    val transactionViewTree11: FullTransactionViewTree =
      nonRootTransactionViewTree(
        blinded(view0),
        leafsBlinded(view1, blinded(view10), view11),
        blinded(view2),
      )

    val transactionViewTree110: FullTransactionViewTree = nonRootTransactionViewTree(
      blinded(view0),
      leafsBlinded(view1, blinded(view10), leafsBlinded(view11, view110)),
      blinded(view2),
    )

    val transactionViewTree2: FullTransactionViewTree =
      rootTransactionViewTree(blinded(view0), blinded(view1), view2)

    val create120reinterpret: LfNodeCreate =
      genCreate3X(create120local, genCreate120Inst(create100Info.relativeContractId))

    override lazy val reinterpretedSubtransactions: Seq[
      (
          FullTransactionViewTree,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
          Witnesses,
      )
    ] =
      Seq(
        (
          transactionViewTree0,
          (
            transaction(Seq(0), lfCreate0),
            mkMetadata(seeds.filter(_._1 == LfNodeId(0))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree0.informees)),
        ),
        (
          transactionViewTree1,
          (
            transactionFrom(
              Seq(1),
              1,
              exercise1,
              exercise10,
              lfCreate100,
              lfCreate11,
              exercise12,
              lfCreate120,
              lfCreate13,
            ),
            mkMetadata(
              seeds.filter(seed => Seq(1, 2, 3, 4, 5, 6, 7).map(LfNodeId.apply).contains(seed._1))
            ),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree10,
          (
            transactionFrom(Seq(2), 2, exercise10, lfCreate100),
            mkMetadata(seeds.filter(seed => Seq(2, 3).map(LfNodeId.apply).contains(seed._1))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree10.informees, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree100,
          (
            transaction(Seq(0), lfCreate100),
            mkMetadata(Map(LfNodeId(0) -> create100seed)),
            Map.empty,
          ),
          Witnesses(
            NonEmpty(
              List,
              transactionViewTree100.informees,
              transactionViewTree10.informees,
              transactionViewTree1.informees,
            )
          ),
        ),
        (
          transactionViewTree11,
          (
            transactionFrom(Seq(5), 5, exercise12, create120reinterpret),
            mkMetadata(seeds.filter(seed => Seq(5, 6).map(LfNodeId.apply).contains(seed._1))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree11.informees, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree110,
          (
            transaction(Seq(0), create120reinterpret),
            mkMetadata(Map(LfNodeId(0) -> create120seed)),
            Map.empty,
          ),
          Witnesses(
            NonEmpty(
              List,
              transactionViewTree110.informees,
              transactionViewTree11.informees,
              transactionViewTree1.informees,
            )
          ),
        ),
        (
          transactionViewTree2,
          (transaction(Seq(0), lfCreate2), mkMetadata(Map(LfNodeId(0) -> create2seed)), Map.empty),
          Witnesses(NonEmpty(List, transactionViewTree2.informees)),
        ),
      )

    override lazy val rootTransactionViewTrees: Seq[FullTransactionViewTree] =
      Seq(transactionViewTree0, transactionViewTree1, transactionViewTree2)

    val (create0auth, create0Absolute) = toAbsolute(transactionId, create0Info.relativeFci)
    val create0: LfNodeCreate = genCreateX(create0Absolute, create0Inst)
    val (create100auth, create100Absolute) = toAbsolute(transactionId, create100Info.relativeFci)
    val create100: LfNodeCreate = genCreate3X(create100Absolute, create100Inst)
    val (create11auth, create11Absolute) = toAbsolute(transactionId, create11Info.relativeFci)
    val create11: LfNodeCreate = genCreateXX(create11Absolute, genCreate11Inst(create100Absolute))
    val (create120auth, create120Absolute) = toAbsolute(transactionId, create120Info.relativeFci)
    val create120: LfNodeCreate =
      genCreate3X(create120Absolute, genCreate120Inst(create100Absolute))
    val (create13auth, create13Absolute) = toAbsolute(transactionId, create13Info.relativeFci)
    val create13: LfNodeCreate = genCreateXX(create13Absolute, genCreate13Inst(create120Absolute))
    val (create2auth, create2Absolute) = toAbsolute(transactionId, create2Info.relativeFci)
    val create2: LfNodeCreate = genCreateX(create2Absolute, create2Inst)

    override lazy val versionedSuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1, 8),
        create0,
        exercise1,
        exercise10,
        create100,
        create11,
        exercise12,
        create120,
        create13,
        create2,
      )

    override def usedAndCreated: UsedAndCreatedContracts = {
      val create0Instance =
        asContractInstance(create0Absolute, create0.versionedCoinst, metadataFromCreate(create0))(
          create0auth
        )
      val create100Instance = asContractInstance(
        create100Absolute,
        create100.versionedCoinst,
        metadataFromCreate(create100),
      )(create100auth)
      val create11Instance = asContractInstance(
        create11Absolute,
        create11.versionedCoinst,
        metadataFromCreate(create11),
      )(create11auth)
      val create120Instance = asContractInstance(
        create120Absolute,
        create120.versionedCoinst,
        metadataFromCreate(create120),
      )(create120auth)
      val create13Instance = asContractInstance(
        create13Absolute,
        create13.versionedCoinst,
        metadataFromCreate(create13),
      )(create13auth)
      val create2Instance = asContractInstance(
        create2Absolute,
        create2.versionedCoinst,
        metadataFromCreate(create2),
      )(create2auth)

      UsedAndCreatedContracts.empty.copy(
        checkActivenessTxInputs = Set(exercise1Id, exercise10Id, exercise12Id),
        consumedInputsOfHostedStakeholders = Map(
          exercise10Id -> exercise10.stakeholders,
          exercise12Id -> exercise12.stakeholders,
        ),
        used = Map(
          exercise1Id -> exercise1Input,
          exercise10Id -> exercise10Input,
          exercise12Id -> exercise12Input,
        ),
        maybeCreated = Map(
          create0Absolute -> Some(create0Instance),
          create100Absolute -> Some(create100Instance),
          create11Absolute -> Some(create11Instance),
          create120Absolute -> Some(create120Instance),
          create13Absolute -> Some(create13Instance),
          create2Absolute -> Some(create2Instance),
        ),
      )
    }
  }

  /** Transaction structure:
    *   - 0. create
    *   - \1. exercise(0)
    *     - 1.0. create
    *     - 1.1. exerciseN(1.0)
    *       - 1.1.0. create
    *     - 1.2. exercise(1.1.0)
    *     - 1.3. exercise(1.0)
    *
    * View structure:
    *   - 0. view0
    *   - \1. view1
    *     - 1.1. view10
    */
  case object TransientContracts extends ExampleTransaction {

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with transient contracts"

    def stakeholders: Set[LfPartyId] = Set(submitter, observer)
    def genCreate(
        cid: LfContractId,
        contractInst: LfThinContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = Set(submitter),
        observers = Set(observer),
      )

    val create0Inst: LfThinContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0local: LfContractId = localContractId(discriminator(create0seed, stakeholders))
    val lfCreate0: LfNodeCreate = genCreate(create0local, create0Inst)

    def genExercise(cid: LfContractId, childIndices: List[Int]): LfNodeExercises =
      exerciseNode(
        cid,
        actingParties = Set(submitter),
        signatories = Set(submitter),
        observers = Set(observer),
        children = childIndices.map(nodeId),
      )
    val lfExercise1: LfNodeExercises = genExercise(create0local, List(2, 3, 5, 6))

    val create10Inst: LfThinContractInst = contractInstance()
    val create10seed: LfHash = deriveNodeSeed(1, 0)
    val create10local: LfContractId = localContractId(discriminator(create10seed, stakeholders))
    val lfCreate10: LfNodeCreate = genCreate(create10local, create10Inst)

    def genExerciseN(cid: LfContractId, childIndex: Int): LfNodeExercises =
      exerciseNode(
        cid,
        consuming = false,
        actingParties = Set(submitter),
        signatories = Set(submitter, signatory),
        observers = Set(observer),
        children = List(nodeId(childIndex)),
      )
    val lfExercise11: LfNodeExercises = genExerciseN(create10local, 4)

    val create110seed: LfHash = deriveNodeSeed(1, 1, 0)
    val create110local: LfContractId = localContractId(
      discriminator(create110seed, Set(submitter, signatory))
    )
    val create110Inst: LfThinContractInst = contractInstance()
    def genCreate110(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create110Inst,
        signatories = Set(submitter, signatory),
        observers = Set.empty,
      )
    val lfCreate110: LfNodeCreate = genCreate110(create110local)

    val lfExercise12: LfNodeExercises = genExercise(create110local, List.empty)

    val lfExercise13: LfNodeExercises = genExercise(create10local, List.empty)

    override def versionedUnsuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1),
        lfCreate0,
        lfExercise1,
        lfCreate10,
        lfExercise11,
        lfCreate110,
        lfExercise12,
        lfExercise13,
      )

    val exercise1seed: LfHash = deriveNodeSeed(1)
    val exercise11seed: LfHash = deriveNodeSeed(1, 1)
    val exercise12seed: LfHash = deriveNodeSeed(1, 2)
    val exercise13seed: LfHash = deriveNodeSeed(1, 3)

    override lazy val metadata: TransactionMetadata = mkMetadata(
      Map(
        LfNodeId(0) -> create0seed,
        LfNodeId(1) -> exercise1seed,
        LfNodeId(2) -> create10seed,
        LfNodeId(3) -> exercise11seed,
        LfNodeId(4) -> create110seed,
        LfNodeId(5) -> exercise12seed,
        LfNodeId(6) -> exercise13seed,
      )
    )

    override def keyResolver: LfKeyResolver = Map.empty // No keys involved here

    override def rootViewDecompositions: Seq[TransactionViewDecomposition.NewView] = {
      val v0 = awaitCreateNewView(
        lfCreate0,
        Some(create0seed),
        LfNodeId(0),
        Seq.empty,
        isRoot = true,
      )

      val v10 = awaitCreateNewView(
        LfTransactionUtil.lightWeight(lfExercise11),
        Some(exercise11seed),
        LfNodeId(3),
        Seq(SameView(lfCreate110, LfNodeId(4), RollbackContext.empty)),
        isRoot = false,
      )

      val v1 = awaitCreateNewView(
        LfTransactionUtil.lightWeight(lfExercise1),
        Some(exercise1seed),
        LfNodeId(1),
        Seq(
          SameView(lfCreate10, LfNodeId(2), RollbackContext.empty),
          v10,
          SameView(LfTransactionUtil.lightWeight(lfExercise12), LfNodeId(5), RollbackContext.empty),
          SameView(LfTransactionUtil.lightWeight(lfExercise13), LfNodeId(6), RollbackContext.empty),
        ),
        isRoot = true,
      )

      Seq(v0, v1)
    }

    val create0Info: CreateInfo =
      fromLocalContractId(
        rootViewPosition(0, 2),
        0,
        0,
        create0Inst,
        create0local,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val create0Relative: LfNodeCreate = genCreate(create0Info.relativeContractId, create0Inst)

    val exercise1: LfNodeExercises = genExercise(create0Info.relativeContractId, List(2, 3, 5, 6))

    val create10Info: CreateInfo = fromLocalContractId(
      rootViewPosition(1, 2),
      1,
      0,
      create10Inst,
      create10local,
      signatories = Set(submitter),
      observers = Set(observer),
    )
    val create10Relative: LfNodeCreate = genCreate(create10Info.relativeContractId, create10Inst)

    val exercise11: LfNodeExercises = genExerciseN(create10Info.relativeContractId, 4)

    val create110Info: CreateInfo = fromLocalContractId(
      subViewIndex(0, 1) +: rootViewPosition(1, 2),
      2,
      0,
      create110Inst,
      create110local,
      signatories = Set(submitter, signatory),
    )
    val create110Relative: LfNodeCreate = genCreate110(create110Info.relativeContractId)

    val exercise12: LfNodeExercises = genExercise(create110Info.relativeContractId, List.empty)

    val exercise13: LfNodeExercises = genExercise(create10Info.relativeContractId, List.empty)

    val view0: TransactionView =
      view(
        create0Relative,
        0,
        Set.empty,
        Seq.empty,
        Seq(instanceFromCreate(create0Relative, create0Info.relativeAuthenticationData)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )

    val view10: TransactionView = view(
      exercise11,
      2,
      Set.empty,
      Seq(instanceFromCreate(create10Relative, create10Info.relativeAuthenticationData)),
      Seq(instanceFromCreate(create110Relative, create110Info.relativeAuthenticationData)),
      Map.empty,
      Some(deriveNodeSeed(1, 1)),
      isRoot = false,
      Set.empty,
    )

    val view1: TransactionView = view(
      exercise1,
      1,
      Set(
        create0Info.relativeContractId,
        create10Info.relativeContractId,
        create110Info.relativeContractId,
      ),
      Seq(instanceFromCreate(create0Relative, create0Info.relativeAuthenticationData)),
      Seq(instanceFromCreate(create10Relative, create10Info.relativeAuthenticationData)),
      Map.empty,
      Some(deriveNodeSeed(1)),
      isRoot = true,
      Set.empty,
      view10,
    )

    override def rootViews: Seq[TransactionView] = Seq(view0, view1)

    override def viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])] =
      Seq(view0 -> Seq(view0), view1 -> Seq(view1, view10), view10 -> Seq(view10))

    override def transactionTree: GenTransactionTree =
      genTransactionTree(view0, view1)

    override def fullInformeeTree: FullInformeeTree =
      mkFullInformeeTree(
        blindedForInformeeTree(view0),
        blindedForInformeeTree(view1, blindedForInformeeTree(view10)),
      )

    val transactionViewTree0: FullTransactionViewTree =
      rootTransactionViewTree(view0, blinded(view1))
    val transactionViewTree1: FullTransactionViewTree =
      rootTransactionViewTree(blinded(view0), view1)
    val transactionViewTree10: FullTransactionViewTree =
      nonRootTransactionViewTree(blinded(view0), leafsBlinded(view1, view10))

    override def reinterpretedSubtransactions: Seq[
      (
          FullTransactionViewTree,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
          Witnesses,
      )
    ] =
      Seq(
        (
          transactionViewTree0,
          (
            transaction(Seq(0), lfCreate0),
            mkMetadata(seeds.filter(_._1 == LfNodeId(0))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree0.informees)),
        ),
        (
          transactionViewTree1,
          (
            transactionFrom(
              Seq(1),
              1,
              exercise1,
              lfCreate10,
              lfExercise11,
              lfCreate110,
              lfExercise12,
              lfExercise13,
            ),
            mkMetadata(
              seeds.filter(seed => Seq(1, 2, 3, 4, 5, 6).map(LfNodeId.apply).contains(seed._1))
            ),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree1.informees)),
        ),
        (
          transactionViewTree10,
          (
            transactionFrom(Seq(3), 3, exercise11, lfCreate110),
            mkMetadata(seeds.filter(seed => Seq(3, 4).map(LfNodeId.apply).contains(seed._1))),
            Map.empty,
          ),
          Witnesses(NonEmpty(List, transactionViewTree10.informees, transactionViewTree1.informees)),
        ),
      )

    override def rootTransactionViewTrees: Seq[FullTransactionViewTree] =
      Seq(transactionViewTree0, transactionViewTree1)

    val (create0auth, create0Absolute) = toAbsolute(transactionId, create0Info.relativeFci)
    val create0: LfNodeCreate = genCreate(create0Absolute, create0Inst)
    val exercise1Abs: LfNodeExercises = genExercise(create0Absolute, List(2, 3, 5, 6))
    val (create10auth, create10Absolute) = toAbsolute(transactionId, create10Info.relativeFci)
    val create10: LfNodeCreate = genCreate(create10Absolute, create10Inst)
    val exercise11Abs: LfNodeExercises = genExerciseN(create10Absolute, 4)
    val (create110auth, create110Absolute) = toAbsolute(transactionId, create110Info.relativeFci)
    val create110: LfNodeCreate = genCreate110(create110Absolute)
    val exercise12Abs: LfNodeExercises = genExercise(create110Absolute, List.empty)
    val exercise13Abs: LfNodeExercises = genExercise(create10Absolute, List.empty)

    override def versionedSuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1),
        create0,
        exercise1Abs,
        create10,
        exercise11Abs,
        create110,
        exercise12Abs,
        exercise13Abs,
      )

    override def usedAndCreated: UsedAndCreatedContracts = {
      val create0Instance =
        asContractInstance(create0Absolute, create0.versionedCoinst, metadataFromCreate(create0))(
          create0auth
        )
      val create10Instance = asContractInstance(
        create10Absolute,
        create10.versionedCoinst,
        metadataFromCreate(create10),
      )(create10auth)
      val create110Instance = asContractInstance(
        create110Absolute,
        create110.versionedCoinst,
        metadataFromCreate(create110),
      )(create110auth)

      UsedAndCreatedContracts.empty.copy(
        checkActivenessTxInputs = Set.empty,
        consumedInputsOfHostedStakeholders = Map.empty,
        // create110Absolute does not appear as used because it is archived by a parent view of the creating view.
        used = Map(create0Absolute -> create0Instance, create10Absolute -> create10Instance),
        maybeCreated = Map(
          create0Absolute -> Some(create0Instance),
          create10Absolute -> Some(create10Instance),
          create110Absolute -> Some(create110Instance),
        ),
        transient = Map(
          create0Absolute -> create0.stakeholders,
          create10Absolute -> create10.stakeholders,
          create110Absolute -> create110.stakeholders,
        ),
      )
    }
  }
}
