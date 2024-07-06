// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{
  DomainId,
  ParticipantId,
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
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.Versioned
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{
  ValueContractId,
  ValueOptional,
  ValueRecord,
  ValueUnit,
  VersionedValue,
}
import org.scalatest.EitherValues

import java.time.Duration as JDuration
import java.util.UUID
import scala.collection.immutable.HashMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

import DeduplicationPeriod.DeduplicationDuration

/** Provides convenience methods for creating [[ExampleTransaction]]s and parts thereof.
  */
object ExampleTransactionFactory {
  val hkdfOps: HkdfOps = new SymbolicPureCrypto()
  // Helper methods for Daml-LF types
  val languageVersion = LfTransactionBuilder.defaultLanguageVersion
  val packageId = LfTransactionBuilder.defaultPackageId
  val upgradePackageId = LfPackageId.assertFromString("upgraded-pkg-id")
  val templateId = LfTransactionBuilder.defaultTemplateId
  val packageName = LfTransactionBuilder.defaultPackageName
  val someOptUsedPackages = Some(Set(packageId))
  val defaultGlobalKey = LfTransactionBuilder.defaultGlobalKey
  val transactionVersion = LfTransactionBuilder.defaultTransactionVersion

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
  ): LfContractInst =
    LfContractInst(
      packageName = packageName,
      template = templateId,
      packageVersion = None,
      arg = versionedValueCapturing(capturedIds.toList),
    )

  val veryDeepValue: Value = {
    def deepValue(depth: Int): Value =
      if (depth <= 0) ValueUnit else ValueOptional(Some(deepValue(depth - 1)))

    deepValue(Value.MAXIMUM_NESTING + 10)
  }
  val veryDeepVersionedValue: VersionedValue =
    LfVersioned(transactionVersion, veryDeepValue)

  val veryDeepContractInstance: LfContractInst =
    LfContractInst(
      packageName = packageName,
      template = templateId,
      packageVersion = None,
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
      maintainers: Set[LfPartyId] = Set.empty,
  ): Versioned[LfGlobalKeyWithMaintainers] =
    LfVersioned(transactionVersion, LfGlobalKeyWithMaintainers(key, maintainers))

  def fetchNode(
      cid: LfContractId,
      actingParties: Set[LfPartyId] = Set.empty,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      key: Option[LfGlobalKeyWithMaintainers] = None,
      byKey: Boolean = false,
      version: LfTransactionVersion = transactionVersion,
      templateId: LfTemplateId = templateId,
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
    )

  def createNode(
      cid: LfContractId,
      contractInstance: LfContractInst = this.contractInstance(),
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
      // TODO(#16362): This should be taken from the LfGlobalKey which currently does not have it
      packageName = packageName,
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

  def unicum(index: Int) = Unicum(TestHash.digest(s"unicum$index"))

  def lfHash(index: Int): LfHash =
    LfHash.assertFromBytes(
      Bytes.assertFromString(f"$index%04x".padTo(LfHash.underlyingHashLength * 2, '0'))
    )

  def suffixedId(discriminator: Int, suffix: Int): LfContractId =
    LfContractId.V1(lfHash(discriminator), Bytes.assertFromString(f"$suffix%04x"))

  def unsuffixedId(index: Int): LfContractId.V1 = LfContractId.V1(lfHash(index))

  def rootViewPosition(index: Int, total: Int): ViewPosition =
    ViewPosition(List(MerkleSeq.indicesFromSeq(total)(index)))

  def asSerializableRaw(contractInstance: LfContractInst): SerializableRawContractInstance =
    SerializableRawContractInstance
      .create(contractInstance)
      .fold(err => throw new IllegalArgumentException(err.toString), Predef.identity)

  def asSerializable(
      contractId: LfContractId,
      contractInstance: LfContractInst = this.contractInstance(),
      metadata: ContractMetadata = ContractMetadata.tryCreate(Set.empty, Set(this.signatory), None),
      ledgerTime: CantonTimestamp = CantonTimestamp.Epoch,
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
  ): SerializableContract =
    SerializableContract(
      contractId,
      asSerializableRaw(contractInstance),
      metadata,
      LedgerCreateTime(ledgerTime),
      Some(salt),
    )

  private def serializableFromCreate(
      node: LfNodeCreate,
      salt: Salt,
  ): SerializableContract = {
    asSerializable(
      node.coid,
      node.versionedCoinst,
      metadataFromCreate(node),
      salt = salt,
    )
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

  val applicationId: ApplicationId = DefaultDamlValues.applicationId()
  val commandId: CommandId = DefaultDamlValues.commandId()
  val workflowId: WorkflowId = WorkflowId.assertFromString("testWorkflowId")

  val defaultTestingTopology: TestingTopology =
    TestingTopology(
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
            _ -> Seq(ExampleTransactionFactory.packageId, upgradePackageId)
          )
          .toMap,
    )

  def defaultTestingIdentityFactory: TestingIdentityFactory =
    defaultTestingTopology.build()

  // Topology
  def defaultTopologySnapshot: TopologySnapshot =
    defaultTestingIdentityFactory.topologySnapshot()

  def defaultPackageInfoService: PackageInfoService = new PackageInfoService {
    override def getDescription(packageId: PackageId)(implicit
        traceContext: TraceContext
    ): Future[Option[PackageDescription]] = Future.successful(None)
  }

  // Merkle trees
  def blinded[A](tree: MerkleTree[A]): MerkleTree[A] = BlindedNode(tree.rootHash)

}

/** Factory for [[ExampleTransaction]].
  * Also contains a number of predefined example transactions.
  * Also provides convenience methods for creating [[ExampleTransaction]]s and parts thereof.
  */
class ExampleTransactionFactory(
    val cryptoOps: HashOps with HmacOps with HkdfOps with RandomOps = new SymbolicPureCrypto,
    versionOverride: Option[ProtocolVersion] = None,
)(
    val transactionSalt: Salt = TestSalt.generateSalt(0),
    val transactionSeed: SaltSeed = TestSalt.generateSeed(0),
    val transactionUuid: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555"),
    val domainId: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("example::default")),
    val mediatorGroup: MediatorGroupRecipient = MediatorGroupRecipient(MediatorGroupIndex.zero),
    val ledgerTime: CantonTimestamp = CantonTimestamp.Epoch,
    val ledgerTimeUsed: CantonTimestamp = CantonTimestamp.Epoch.minusSeconds(1),
    val submissionTime: CantonTimestamp = CantonTimestamp.Epoch.minusMillis(9),
    val topologySnapshot: TopologySnapshot = defaultTopologySnapshot,
)(implicit ec: ExecutionContext, tc: TraceContext)
    extends EitherValues {

  private val protocolVersion = versionOverride.getOrElse(BaseTest.testedProtocolVersion)
  private val cantonContractIdVersion = AuthenticatedContractIdVersionV10
  private val random = new Random(0)

  private def createNewView(
      rootNode: LfActionNode,
      rootSeed: Option[LfHash],
      rootNodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
      isRoot: Boolean,
  ): Future[NewView] = {

    val rootRbContext = RollbackContext.empty

    val submittingAdminPartyO =
      Option.when(isRoot)(submitterMetadata.submittingParticipant.adminParty.toLf)
    TransactionViewDecompositionFactory
      .informeesParticipantsAndThreshold(rootNode, topologySnapshot)
      .map { case (viewInformeesWithParticipantData, viewThreshold) =>
        val viewInformees = viewInformeesWithParticipantData.fmap(_._2)
        val result = NewView(
          rootNode,
          ViewConfirmationParameters.create(viewInformees, viewThreshold),
          rootSeed,
          rootNodeId,
          tailNodes,
          rootRbContext,
        )

        result.withSubmittingAdminParty(submittingAdminPartyO)
      }
  }

  private def awaitCreateNewView(
      rootNode: LfActionNode,
      rootSeed: Option[LfHash],
      rootNodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
      isRoot: Boolean,
  ): NewView =
    Await.result(
      createNewView(
        rootNode,
        rootSeed,
        rootNodeId,
        tailNodes,
        isRoot,
      ),
      10.seconds,
    )

  /** Yields standard test cases that the sync-protocol must be able to handle.
    * Yields only "happy" cases, i.e., the sync-protocol must not emit an error.
    */
  lazy val standardHappyCases: Seq[ExampleTransaction] =
    Seq[ExampleTransaction](
      EmptyTransaction,
      SingleCreate(seed = deriveNodeSeed(0)),
      SingleCreate(
        seed = deriveNodeSeed(0),
        capturedContractIds = Seq(suffixedId(-1, 0), suffixedId(-1, 1)),
        unsuffixedCapturedContractIds = Seq(suffixedId(-1, 0), suffixedId(-1, 1)),
      ),
      SingleFetch(version = LfTransactionVersion.V31),
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
    submissionTime.toLf,
  )

  def deriveNodeSeed(path: Int*): LfHash =
    path.foldLeft(lfTransactionSeed)((seed, i) => LfHash.deriveNodeSeed(seed, i))

  def discriminator(nodeSeed: LfHash, stakeholders: Set[LfPartyId]): LfHash =
    LfHash.deriveContractDiscriminator(nodeSeed, submissionTime.toLf, stakeholders)

  val unicumGenerator = new UnicumGenerator(cryptoOps)

  def saltAndUnicum(
      viewPosition: ViewPosition,
      viewIndex: Int,
      createIndex: Int,
      suffixedContractInstance: LfContractInst,
      metadata: ContractMetadata,
  ): (Salt, Unicum) = {
    val viewParticipantDataSalt = participantDataSalt(viewIndex)
    val (contractSalt, unicum) = unicumGenerator
      .generateSaltAndUnicum(
        domainId,
        mediatorGroup,
        transactionUuid,
        viewPosition,
        viewParticipantDataSalt,
        createIndex,
        LedgerCreateTime(ledgerTime),
        metadata,
        asSerializableRaw(suffixedContractInstance),
        cantonContractIdVersion,
      )

    contractSalt.unwrap -> unicum
  }

  def fromDiscriminator(
      viewPosition: ViewPosition,
      viewIndex: Int,
      createIndex: Int,
      suffixedContractInstance: LfContractInst,
      discriminator: LfHash,
      signatories: Set[LfPartyId] = Set.empty,
      observers: Set[LfPartyId] = Set.empty,
      maybeKeyWithMaintainers: Option[protocol.LfGlobalKeyWithMaintainers] = None,
  ): (Salt, LfContractId) = {
    val metadata = ContractMetadata.tryCreate(
      signatories,
      signatories ++ observers,
      maybeKeyWithMaintainers.map(LfVersioned(transactionVersion, _)),
    )
    val (salt, unicum) =
      saltAndUnicum(
        viewPosition,
        viewIndex,
        createIndex,
        suffixedContractInstance,
        metadata,
      )
    salt -> cantonContractIdVersion.fromDiscriminator(discriminator, unicum)
  }

  def rootViewPosition(index: Int, total: Int): ViewPosition =
    ViewPosition(List(MerkleSeq.indicesFromSeq(total)(index)))

  def subViewIndex(index: Int, total: Int): MerklePathElement =
    TransactionSubviews.indices(total)(index)

  private def viewInternal(
      node: LfActionNode,
      viewConfirmationParameters: ViewConfirmationParameters,
      viewIndex: Int,
      consumed: Set[LfContractId],
      coreInputs: Seq[SerializableContract],
      created: Seq[SerializableContract],
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
      CreatedContract.tryCreate(contract, consumed.contains(coid), rolledBack = false)
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
      coreInputs: Seq[SerializableContract],
      created: Seq[SerializableContract],
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
          .informeesParticipantsAndThreshold(node, topologySnapshot),
        10.seconds,
      )
    val rawInformees = rawInformeesWithParticipantData.fmap { case (_, weight) => weight }
    val viewConfirmationParameters =
      TransactionViewDecompositionFactory.withSubmittingAdminParty(submittingAdminPartyO)(
        ViewConfirmationParameters.create(rawInformees, rawThreshold)
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

  def viewWithInformeesMerge(
      node: LfActionNode,
      nodesToMerge: Seq[LfActionNode],
      viewIndex: Int,
      consumed: Set[LfContractId],
      coreInputs: Seq[SerializableContract],
      created: Seq[SerializableContract],
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
            .informeesParticipantsAndThreshold(nodeToMerge, topologySnapshot),
          10.seconds,
        )
      val rawInformees = rawInformeesWithParticipantData.fmap { case (_, weight) => weight }
      ViewConfirmationParameters.create(rawInformees, rawThreshold)
    }

    val submittingAdminPartyO =
      Option.when(isRoot)(submitterMetadata.submittingParticipant.adminParty.toLf)

    val viewConfirmationParameters =
      TransactionViewDecompositionFactory.withSubmittingAdminParty(submittingAdminPartyO)(
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
    TransactionMetadata(ledgerTime, submissionTime, seeds)

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
      applicationId,
      commandId,
      submittingParticipant,
      Salt.tryDeriveSalt(transactionSeed, 0, cryptoOps),
      DefaultDamlValues.submissionId().some,
      DeduplicationDuration(JDuration.ofSeconds(100)),
      ledgerTime.plusSeconds(100),
      cryptoOps,
      protocolVersion,
    )

  val commonMetadata: CommonMetadata =
    CommonMetadata
      .create(cryptoOps, protocolVersion)(
        domainId,
        mediatorGroup,
        Salt.tryDeriveSalt(transactionSeed, 1, cryptoOps),
        transactionUuid,
      )

  val participantMetadata: ParticipantMetadata =
    ParticipantMetadata(cryptoOps)(
      ledgerTime,
      submissionTime,
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
  }

  abstract class SingleNode(val nodeSeed: Option[LfHash]) extends ExampleTransaction {
    override def cryptoOps: HashOps & RandomOps = ExampleTransactionFactory.this.cryptoOps

    def lfContractId: LfContractId

    def contractId: LfContractId

    def salt: Salt

    def nodeId: LfNodeId

    protected def contractInstance: LfContractInst

    def lfNode: LfActionNode

    def node: LfActionNode

    def reinterpretedNode: LfActionNode

    def consuming: Boolean

    def created: Seq[SerializableContract] = node match {
      case n: LfNodeCreate =>
        Seq(
          asSerializable(
            n.coid,
            contractInstance,
            metadataFromCreate(n),
            salt = salt,
          )
        )
      case _ => Seq.empty
    }

    def used: Seq[SerializableContract] = node match {
      case n: LfNodeExercises =>
        Seq(
          asSerializable(
            n.targetCoid,
            contractInstance,
            metadataFromExercise(n),
            salt = salt,
          )
        )
      case n: LfNodeFetch =>
        Seq(
          asSerializable(
            n.coid,
            contractInstance,
            metadataFromFetch(n),
            salt = salt,
          )
        )
      case _ => Seq.empty
    }

    def consumed: Set[LfContractId] = if (consuming) used.map(_.contractId).toSet else Set.empty

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
      view(node, 0, consumed, used, created, Map.empty, nodeSeed, isRoot = true, Set.empty)

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
  }

  /** Single create.
    * By default, [[submitter]] is the only signatory and [[observer]] the only observer.
    *
    * @param seed the node seed for the create node, used to derive the contract id
    * @param capturedContractIds contract ids captured by the contract instance
    * @throws IllegalArgumentException if [[unsuffixedCapturedContractIds]] and [[capturedContractIds]] have different sizes
    */
  case class SingleCreate(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      viewPosition: ViewPosition = rootViewPosition(0, 1),
      viewIndex: Int = 0,
      capturedContractIds: Seq[LfContractId] = Seq.empty,
      unsuffixedCapturedContractIds: Seq[LfContractId] = Seq.empty,
      signatories: Set[LfPartyId] = Set(submitter),
      observers: Set[LfPartyId] = Set(observer),
      key: Option[LfGlobalKeyWithMaintainers] = None,
  ) extends SingleNode(Some(seed)) {

    require(
      capturedContractIds.lengthCompare(unsuffixedCapturedContractIds) == 0,
      "captured contract IDs must have the same length",
    )

    override val contractInstance: LfContractInst =
      ExampleTransactionFactory.contractInstance(capturedContractIds)

    val serializableContractInstance: SerializableRawContractInstance = asSerializableRaw(
      contractInstance
    )

    val lfContractId: LfContractId = LfContractId.V1(discriminator, Bytes.Empty)

    val (salt, contractId) =
      fromDiscriminator(
        viewPosition,
        viewIndex,
        0,
        contractInstance,
        discriminator,
        signatories,
        observers,
        key,
      )

    private def discriminator: LfHash =
      ExampleTransactionFactory.this.discriminator(seed, signatories union observers)

    override def toString: String = {
      val captured =
        if (capturedContractIds.nonEmpty) s", capturing ${capturedContractIds.size} ids" else ""

      s"single create$captured"
    }

    override def lfNode: LfActionNode =
      createNode(
        lfContractId,
        ExampleTransactionFactory.contractInstance(unsuffixedCapturedContractIds),
        signatories,
        observers,
        key,
      )

    override def node: LfActionNode =
      createNode(contractId, contractInstance, signatories, observers, key)

    override def reinterpretedNode: LfActionNode =
      createNode(lfContractId, contractInstance, signatories, observers, key)

    override def consuming: Boolean = false

  }

  /** Single fetch with [[submitter]] as signatory and [[observer]] as observer and acting party.
    *
    * @param lfContractId id of the fetched contract
    * @param contractId id of the fetched contract
    * @param fetchedContractInstance instance of the used contract.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleFetch(
      override val nodeId: LfNodeId = LfNodeId(0),
      lfContractId: LfContractId = suffixedId(-1, 0),
      contractId: LfContractId = suffixedId(-1, 0),
      fetchedContractInstance: LfContractInst = contractInstance(),
      version: LfTransactionVersion = transactionVersion,
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
  ) extends SingleNode(None) {
    override def created: Seq[SerializableContract] = Seq.empty

    override val contractInstance: LfContractInst = fetchedContractInstance

    override def toString: String = "single fetch"

    private def genNode(id: LfContractId) =
      fetchNode(
        id,
        actingParties = Set(observer),
        signatories = Set(submitter),
        observers = Set(observer),
        version = version,
      )

    override def node: LfActionNode = genNode(contractId)
    override def lfNode: LfActionNode = genNode(lfContractId)
    override def reinterpretedNode: LfActionNode = node

    override def consuming: Boolean = false
  }

  /** Single consuming exercise without children with [[submitter]] as signatory, acting party and controller, and
    * [[observer]] as observer.
    *
    * @param lfContractId id of the exercised contract
    * @param contractId id of the exercised contract
    * @param inputContractInstance instance of the used contract.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleExercise(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      lfContractId: LfContractId = suffixedId(-1, 0),
      contractId: LfContractId = suffixedId(-1, 0),
      inputContractInstance: LfContractInst = contractInstance(),
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
  ) extends SingleNode(Some(seed)) {
    override def toString: String = "single exercise"

    override val contractInstance: LfContractInst = inputContractInstance

    private def genNode(id: LfContractId): LfNodeExercises =
      exerciseNodeWithoutChildren(
        targetCoid = id,
        actingParties = Set(submitter),
        signatories = Set(submitter),
        observers = Set(observer),
      )

    override def node: LfNodeExercises = genNode(contractId)
    override def lfNode: LfNodeExercises = genNode(lfContractId)
    override def reinterpretedNode: LfNodeExercises = node

    override def consuming: Boolean = true
  }

  /** Single consuming exercise without children without any acting party or signatory, and
    * [[observer]] as observer.
    *
    * @param lfContractId id of the exercised contract
    * @param contractId id of the exercised contract
    * @param inputContractInstance instance of the used contract.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleExerciseWithoutConfirmingParties(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      lfContractId: LfContractId = suffixedId(-1, 0),
      contractId: LfContractId = suffixedId(-1, 0),
      inputContractInstance: LfContractInst = contractInstance(),
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
  ) extends SingleNode(Some(seed)) {
    override def toString: String = "single exercise"

    override val contractInstance: LfContractInst = inputContractInstance

    private def genNode(id: LfContractId): LfNodeExercises =
      exerciseNodeWithoutChildren(
        targetCoid = id,
        actingParties = Set.empty,
        signatories = Set.empty,
        observers = Set(observer),
      )

    override def node: LfNodeExercises = genNode(contractId)
    override def lfNode: LfNodeExercises = genNode(lfContractId)
    override def reinterpretedNode: LfNodeExercises = node

    override def consuming: Boolean = true
  }

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class UpgradedSingleExercise(
      seed: LfHash,
      nodeId: LfNodeId = LfNodeId(0),
      lfContractId: LfContractId = suffixedId(-1, 0),
      contractId: LfContractId = suffixedId(-1, 0),
      contractInstance: LfContractInst = ExampleTransactionFactory.contractInstance(),
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
      consuming: Boolean = true,
  ) extends SingleNode(Some(seed)) {
    val upgradedTemplateId: canton.protocol.LfTemplateId =
      templateId.copy(packageId = upgradePackageId)
    private def genNode(id: LfContractId): LfNodeExercises =
      exerciseNode(targetCoid = id, templateId = upgradedTemplateId, signatories = Set(submitter))
    override def node: LfNodeExercises = genNode(contractId)
    override def lfNode: LfNodeExercises = genNode(lfContractId)
    override def reinterpretedNode: LfNodeExercises = node
  }

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  case class SingleExerciseWithNonstakeholderActor(
      seed: LfHash,
      override val nodeId: LfNodeId = LfNodeId(0),
      lfContractId: LfContractId = suffixedId(-1, 0),
      contractId: LfContractId = suffixedId(-1, 0),
      inputContractInstance: LfContractInst = contractInstance(),
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
  ) extends SingleNode(Some(seed)) {

    override val contractInstance: LfContractInst = inputContractInstance

    private def genNode(id: LfContractId): LfActionNode =
      exerciseNodeWithoutChildren(
        id,
        actingParties = Set(submitter),
        signatories = Set(signatory),
        observers = Set(observer),
      )
    override def node: LfActionNode = genNode(contractId)
    override def lfNode: LfActionNode = genNode(lfContractId)
    override def reinterpretedNode: LfActionNode = node

    override def consuming: Boolean = true

    override def toString: String = "single exercise with a non-stakeholder actor"

  }

  /** Transaction structure:
    * 0. create
    * 1. create capturing 0.
    * 2. fetch
    * 3. fetch 0.
    * 4. exercise
    * 5. exercise 1.
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
      capturedContractIds = Seq(suffixedId(-1, 1), create0.contractId),
      unsuffixedCapturedContractIds = Seq(suffixedId(-1, 1), create0.lfContractId),
    )
    private val fetch2: SingleFetch = SingleFetch(LfNodeId(2), suffixedId(-1, 2), suffixedId(-1, 2))
    private val fetch3: SingleFetch =
      SingleFetch(
        nodeId = LfNodeId(3),
        lfContractId = create0.lfContractId,
        contractId = create0.contractId,
        fetchedContractInstance = create0.contractInstance,
        version =
          LfTransactionVersion.V31, // ensure we test merging transactions with different versions
        salt = create0.salt,
      )
    private val exercise4: SingleExercise =
      SingleExercise(deriveNodeSeed(4), LfNodeId(4), suffixedId(-1, 4), suffixedId(-1, 4))
    private val exercise5: SingleExercise = SingleExercise(
      seed = deriveNodeSeed(5),
      nodeId = LfNodeId(5),
      lfContractId = create1.lfContractId,
      contractId = create1.contractId,
      inputContractInstance = create1.contractInstance,
      salt = create1.salt,
    )

    private val examples: List[SingleNode] =
      List[SingleNode](create0, create1, fetch2, fetch3, exercise4, exercise5)
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
          ex.node,
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

    override def versionedSuffixedTransaction: LfVersionedTransaction =
      transaction(0 until rootViewCount, examples.map(_.node)*)
  }

  /** Transaction structure:
    * 0. create
    * 1. exercise absolute
    * 1.0. create
    * 1.1. fetch 1.0.
    * 1.2. create
    * 1.3. exercise 1.2.
    *
    * In this specific
    * scenario we make sure informees and quorums for action nodes 1.0, 1.1. and 1.3 are correctly merged
    * to the parent view (v1):
    * 0. View0
    * 1. View1
    * 1.2 View10
    */
  case object MultipleRootsAndSimpleViewNesting extends ExampleTransaction {
    override def cryptoOps: HashOps & RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with multiple roots and a simple view nesting"

    val create0Agreement = "create0"
    def create0Inst: LfContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0disc: LfHash = discriminator(create0seed, Set(submitter, observer))
    def genCreate0(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create0Inst,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val lfCreate0: LfNodeCreate = genCreate0(LfContractId.V1(create0disc))

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

    def create10Inst: LfContractInst = contractInstance()
    def create12Inst: LfContractInst = contractInstance()
    def genCreate10(
        cid: LfContractId,
        contractInstance: LfContractInst,
        agreementText: String,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInstance,
        signatories = Set(submitter, signatory, signatoryReplica),
      )

    def genCreate12(
        cid: LfContractId,
        contractInstance: LfContractInst,
        agreementText: String,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInstance,
        signatories = Set(submitter, signatory, extra),
      )

    val create10Agreement = "create10"
    val create10seed: LfHash = deriveNodeSeed(1, 0)
    val create10disc: LfHash =
      discriminator(create10seed, Set(submitter, signatory, signatoryReplica))

    val lfCreate10: LfNodeCreate =
      genCreate10(LfContractId.V1(create10disc), create10Inst, create10Agreement)
    val create12Agreement = "create12"
    val create12seed: LfHash = deriveNodeSeed(1, 2)
    val create12disc: LfHash = discriminator(create12seed, Set(submitter, signatory, extra))
    val lfCreate12: LfNodeCreate =
      genCreate12(LfContractId.V1(create12disc), create12Inst, create12Agreement)

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

        val quorumsAux = {
          val (quorumSubmittingParticipant, quorumOther) =
            v1Pre.viewConfirmationParameters.quorums.partition(q =>
              q == Quorum(
                confirmers = Map(submitter -> PositiveInt.one),
                threshold = NonNegativeInt.one,
              )
            )

          /* the submitting participant quorum should only be added at the end after the other quorums have been merged
           * to mimic what happens during view decomposition.
           */
          (quorumOther ++ v1TailNodes.mapFilter {
            case SameView(lfNode, nodeId, _) if !nodesNotChildren.contains(nodeId) =>
              Some(
                Quorum(
                  confirmers = lfNode.requiredAuthorizers.map(pId => pId -> PositiveInt.one).toMap,
                  threshold = NonNegativeInt.tryCreate(lfNode.requiredAuthorizers.size),
                )
              )
            case _ => None
          } ++ quorumSubmittingParticipant).distinct
        }

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
    val (salt0Id, create0Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(0, 2),
        0,
        0,
        create0Inst,
        create0disc,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val create0: LfNodeCreate = genCreate0(create0Id)

    val exercise1Agreement = "exercise1"
    val exercise1Id: LfContractId = suffixedId(-1, 0)
    val exercise1: LfNodeExercises = genExercise1(exercise1Id)
    val exercise1Instance: LfContractInst = contractInstance()

    val create10SerInst: SerializableRawContractInstance =
      asSerializableRaw(create10Inst)
    val (salt10Id, create10Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(1, 2),
        1,
        0,
        create10Inst,
        create10disc,
        signatories = Set(submitter, signatory, signatoryReplica),
      )
    val create10: LfNodeCreate = genCreate10(create10Id, create10Inst, create10Agreement)

    val fetch11: LfNodeFetch = lfFetch11

    val (salt12Id, create12Id): (Salt, LfContractId) =
      fromDiscriminator(
        subViewIndex(0, 1) +: rootViewPosition(1, 2),
        2,
        0,
        create12Inst,
        create12disc,
        signatories = Set(submitter, signatory, extra),
      )
    val create12: LfNodeCreate = genCreate12(create12Id, create12Inst, create12Agreement)

    val exercise13Id: LfContractId = suffixedId(-1, 0)
    val exercise13: LfNodeExercises = genExercise13(exercise13Id)

    // Views
    val view0: TransactionView =
      view(
        create0,
        0,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create0, salt0Id)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )

    val view10: TransactionView =
      view(
        create12,
        2,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create12, salt12Id)),
        Map.empty,
        Some(create12seed),
        isRoot = false,
        Set.empty,
      )

    val view1: TransactionView =
      viewWithInformeesMerge(
        exercise1,
        Seq[LfActionNode](create10, fetch11, exercise13),
        1,
        Set(exercise1Id, exercise13Id),
        Seq(
          asSerializable(
            exercise1Id,
            exercise1Instance,
            metadataFromExercise(exercise1),
            ledgerTime,
          )
        ),
        Seq(serializableFromCreate(create10, salt10Id)),
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

    val fetch11Abs: LfNodeFetch = genFetch11(create10.coid)

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

  }

  /** Transaction structure:
    * 0. create
    * 1. exercise absolute
    * 1.0. create
    * 1.1. fetch 1.0.
    * 1.2. create
    * 1.3. exercise 1.2.
    * 1.3.0. create
    * 1.3.1. exercise absolute
    * 1.3.1.0 create
    *
    * View structure:
    * 0. View0
    * 1. View1
    * 1.3.0. View10
    * 1.3.1. View11
    * 1.3.1.0 View110
    */
  case object MultipleRootsAndViewNestings extends ExampleTransaction {

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with multiple roots and view nestings"

    def create0Inst: LfContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0disc: LfHash = discriminator(deriveNodeSeed(0), Set(submitter, observer))
    def genCreate0(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create0Inst,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val lfCreate0: LfNodeCreate = genCreate0(LfContractId.V1(create0disc))

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

    def create10Inst: LfContractInst = contractInstance()
    def create12Inst: LfContractInst = contractInstance()
    def genCreate1x(cid: LfContractId, contractInstance: LfContractInst): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInstance,
        signatories = Set(submitter, signatory),
      )

    val create10seed: LfHash = deriveNodeSeed(1, 0)
    val create10disc: LfHash = discriminator(create10seed, Set(submitter, signatory))
    val lfCreate10: LfNodeCreate = genCreate1x(LfContractId.V1(create10disc), create10Inst)
    val create12seed: LfHash = deriveNodeSeed(1, 2)
    val create12disc: LfHash = discriminator(create12seed, Set(submitter, signatory))
    val lfCreate12: LfNodeCreate = genCreate1x(LfContractId.V1(create12disc), create12Inst)

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

    def create130Inst: LfContractInst = contractInstance()
    val create130seed: LfHash = deriveNodeSeed(1, 3, 0)
    def genCreate130(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create130Inst,
        signatories = Set(signatory),
        observers = Set(extra),
      )
    val create130disc: LfHash = discriminator(create130seed, Set(signatory, extra))
    val lfCreate130: LfNodeCreate = genCreate130(LfContractId.V1(create130disc))

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

    def create1310Inst: LfContractInst = contractInstance()
    val create1310seed: LfHash = deriveNodeSeed(1, 3, 1, 0)
    def genCreate1310(cid: LfContractId): LfNodeCreate =
      createNode(
        cid,
        contractInstance = create1310Inst,
        signatories = Set(submitter),
        observers = Set(extra),
      )
    val create1310disc: LfHash = discriminator(create1310seed, Set(submitter, extra))
    val lfCreate1310: LfNodeCreate = genCreate1310(LfContractId.V1(create1310disc))

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
    val create0SerInst: SerializableRawContractInstance =
      asSerializableRaw(create0Inst)
    val (salt0Id, create0Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(0, 2),
        0,
        0,
        create0Inst,
        create0disc,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val create0: LfNodeCreate = genCreate0(create0Id)

    val exercise1Id: LfContractId = suffixedId(-1, 0)
    val exercise1: LfNodeExercises = genExercise1(exercise1Id)
    val exercise1Instance: LfContractInst = contractInstance()

    val create10SerInst: SerializableRawContractInstance = asSerializableRaw(create10Inst)
    val (salt10Id, create10Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(1, 2),
        1,
        0,
        create10Inst,
        create10disc,
        signatories = Set(submitter, signatory),
      )
    val create10: LfNodeCreate = genCreate1x(create10Id, create10Inst)

    val fetch11: LfNodeFetch = lfFetch11

    val create12SerInst: SerializableRawContractInstance =
      asSerializableRaw(create12Inst)
    val (salt12Id, create12Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(1, 2),
        1,
        1,
        create12Inst,
        create12disc,
        signatories = Set(submitter, signatory),
      )
    val create12: LfNodeCreate = genCreate1x(create12Id, create12Inst)

    val create130SerInst: SerializableRawContractInstance = asSerializableRaw(create130Inst)
    val (salt130Id, create130Id): (Salt, LfContractId) =
      fromDiscriminator(
        subViewIndex(0, 2) +: rootViewPosition(1, 2),
        2,
        0,
        create130Inst,
        create130disc,
        signatories = Set(signatory),
        observers = Set(extra),
      )
    val create130: LfNodeCreate = genCreate130(create130Id)

    val exercise131Id: LfContractId = suffixedId(-1, 1)
    val exercise131: LfNodeExercises = genExercise131(exercise131Id)
    val exercise131Instance: LfContractInst = contractInstance()

    val create1310SerInst: SerializableRawContractInstance = asSerializableRaw(create1310Inst)
    val (salt1310Id, create1310Id): (Salt, LfContractId) =
      fromDiscriminator(
        subViewIndex(0, 1) +: subViewIndex(1, 2) +: rootViewPosition(1, 2),
        4,
        0,
        create1310Inst,
        create1310disc,
        signatories = Set(submitter),
        observers = Set(extra),
      )
    val create1310: LfNodeCreate = genCreate1310(create1310Id)

    // Views
    val view0: TransactionView =
      view(
        create0,
        0,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create0, salt0Id)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )
    val view10: TransactionView =
      view(
        create130,
        2,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create130, salt130Id)),
        Map.empty,
        Some(create130seed),
        isRoot = false,
        Set.empty,
      )
    val view110: TransactionView =
      view(
        create1310,
        4,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create1310, salt1310Id)),
        Map.empty,
        Some(create1310seed),
        isRoot = false,
        Set.empty,
      )

    val view11: TransactionView =
      view(
        exercise131,
        3,
        Set(exercise131Id),
        Seq(
          asSerializable(
            contractId = exercise131Id,
            contractInstance = exercise131Instance,
            metadata = metadataFromExercise(exercise131),
            ledgerTime = ledgerTime,
          )
        ),
        Seq.empty,
        Map.empty,
        Some(deriveNodeSeed(1, 3, 1)),
        isRoot = false,
        Set.empty,
        view110,
      )

    val view1: TransactionView =
      view(
        exercise1,
        1,
        Set(exercise1Id, create12Id),
        Seq(
          asSerializable(
            exercise1Id,
            exercise1Instance,
            metadataFromExercise(exercise1),
            ledgerTime,
          )
        ),
        Seq(
          serializableFromCreate(create10, salt10Id),
          serializableFromCreate(create12, salt12Id),
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

    val fetch11Abs: LfNodeFetch = genFetch11(create10.coid)
    val exercise13Abs: LfNodeExercises = genExercise13(create12.coid)

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

  }

  /** Transaction structure:
    * 0. create
    * 1. exerciseN
    * 1.0. exercise
    * 1.0.0. create
    * 1.1. create(capturing 1.0.0)
    * 1.2. exercise
    * 1.2.0. create(capturing 1.0.0)
    * 1.3. create(capturing 1.2.0)
    * 2. create
    *
    * View structure:
    * 0. View0
    * 1. View1
    * 1.0. View10
    * 1.0.0. View100
    * 1.2. View11
    * 1.2.0. View110
    * 2. View2
    */
  case object ViewInterleavings extends ExampleTransaction {

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with subviews and core nodes interleaved"

    def stakeholdersX: Set[LfPartyId] = Set(submitter, observer)
    def genCreateX(
        cid: LfContractId,
        contractInst: LfContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = Set(submitter),
        observers = Set(observer),
      )

    val create0Inst: LfContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0disc: LfHash = discriminator(create0seed, stakeholdersX)
    val lfCreate0: LfNodeCreate =
      genCreateX(LfContractId.V1(create0disc), create0Inst)

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
        contractInst: LfContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = Set(signatory),
        observers = Set(observer),
      )

    val create100Inst: LfContractInst = contractInstance()
    val create100seed: LfHash = deriveNodeSeed(1, 0, 0)
    val create100disc: LfHash = discriminator(create100seed, stakeholders3X)
    val lfCreate100Id: LfContractId = LfContractId.V1(create100disc)
    val lfCreate100: LfNodeCreate = genCreate3X(lfCreate100Id, create100Inst)

    def stakeholdersXX: Set[LfPartyId] = Set(signatory, submitter)
    def genCreateXX(
        cid: LfContractId,
        contractInst: LfContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = stakeholdersXX,
        observers = Set.empty,
      )

    def genCreate11Inst(capturedId: LfContractId): LfContractInst = contractInstance(
      Seq(capturedId)
    )
    val create11seed: LfHash = deriveNodeSeed(1, 1)
    val create11disc: LfHash = discriminator(create11seed, stakeholdersXX)
    val lfCreate11: LfNodeCreate =
      genCreateXX(LfContractId.V1(create11disc), genCreate11Inst(lfCreate100Id))

    val lfExercise12: LfNodeExercises = genExercise1X(suffixedId(-1, 12), 6)

    def genCreate120Inst(capturedId: LfContractId): LfContractInst = contractInstance(
      Seq(capturedId)
    )
    val lfCreate120Inst: LfContractInst = genCreate120Inst(lfCreate100Id)
    val create120seed: LfHash = deriveNodeSeed(1, 2, 0)
    val create120disc: LfHash = discriminator(create120seed, stakeholders3X)
    val lfCreate120Id: LfContractId = LfContractId.V1(create120disc)
    val lfCreate120: LfNodeCreate = genCreate3X(lfCreate120Id, lfCreate120Inst)

    def genCreate13Inst(capturedId: LfContractId): LfContractInst = contractInstance(
      Seq(capturedId)
    )
    val create13seed: LfHash = deriveNodeSeed(1, 3)
    val create13disc: LfHash = discriminator(create13seed, stakeholdersXX)
    val lfCreate13Id: LfContractId = LfContractId.V1(create13disc)
    val lfCreate13: LfNodeCreate =
      genCreateXX(lfCreate13Id, genCreate13Inst(lfCreate120Id))

    val create2Inst: LfContractInst = contractInstance()
    val create2seed: LfHash = deriveNodeSeed(2)
    val create2disc: LfHash = discriminator(create2seed, stakeholdersX)
    val lfCreate2: LfNodeCreate =
      genCreateX(LfContractId.V1(create2disc), create2Inst)

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

    val create0SerInst: SerializableRawContractInstance =
      asSerializableRaw(create0Inst)
    val (salt0Id, create0Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(0, 3),
        0,
        0,
        create0Inst,
        create0disc,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val create0: LfNodeCreate = genCreateX(create0Id, create0Inst)

    val exercise1Id: LfContractId = suffixedId(-1, 1)
    val exercise1: LfNodeExercises = genExercise1(exercise1Id)
    val exercise1Instance: LfContractInst = contractInstance()

    val exercise10Id: LfContractId = suffixedId(-1, 10)
    val exercise10: LfNodeExercises = genExercise1X(exercise10Id, 3)
    val exercise10Instance: LfContractInst = contractInstance()

    val create100SerInst: SerializableRawContractInstance = asSerializableRaw(create100Inst)
    val (salt100Id, create100Id): (Salt, LfContractId) =
      fromDiscriminator(
        subViewIndex(0, 1) +: subViewIndex(0, 2) +: rootViewPosition(1, 3),
        3,
        0,
        create100Inst,
        create100disc,
        signatories = Set(signatory),
        observers = Set(observer),
      )
    val create100: LfNodeCreate = genCreate3X(create100Id, create100Inst)

    val create11Inst: LfContractInst = genCreate11Inst(create100Id)
    val create11SerInst: SerializableRawContractInstance =
      asSerializableRaw(create11Inst)
    val (salt11Id, create11Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(1, 3),
        1,
        0,
        create11Inst,
        create11disc,
        signatories = stakeholdersXX,
      )
    val create11: LfNodeCreate = genCreateXX(create11Id, create11Inst)

    val exercise12Id: LfContractId = suffixedId(-1, 12)
    val exercise12: LfNodeExercises = genExercise1X(exercise12Id, 6)
    val exercise12Instance: LfContractInst = contractInstance()

    val create120Inst: LfContractInst = genCreate120Inst(create100Id)
    val create120SerInst: SerializableRawContractInstance = asSerializableRaw(create120Inst)
    val (salt120Id, create120Id): (Salt, LfContractId) =
      fromDiscriminator(
        subViewIndex(0, 1) +: subViewIndex(1, 2) +: rootViewPosition(1, 3),
        5,
        0,
        create120Inst,
        create120disc,
        signatories = Set(signatory),
        observers = Set(observer),
      )
    val create120: LfNodeCreate = genCreate3X(create120Id, create120Inst)

    val create13Inst: LfContractInst = genCreate13Inst(create120Id)
    val create13SerInst: SerializableRawContractInstance = asSerializableRaw(create13Inst)
    val (salt13Id, create13Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(1, 3),
        1,
        1,
        create13Inst,
        create13disc,
        signatories = stakeholdersXX,
      )
    val create13: LfNodeCreate = genCreateXX(create13Id, create13Inst)

    val create2SerInst: SerializableRawContractInstance = asSerializableRaw(create2Inst)
    val (salt2Id, create2Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(2, 3),
        6,
        0,
        create2Inst,
        create2disc,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val create2: LfNodeCreate = genCreateX(create2Id, create2Inst)

    val view0: TransactionView =
      view(
        create0,
        0,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create0, salt0Id)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )

    val view100: TransactionView =
      view(
        create100,
        3,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create100, salt100Id)),
        Map.empty,
        Some(create100seed),
        isRoot = false,
        Set.empty,
      )

    val view10: TransactionView = view(
      exercise10,
      2,
      Set(exercise10Id),
      Seq(
        asSerializable(
          exercise10Id,
          exercise10Instance,
          metadataFromExercise(exercise10),
          ledgerTime,
        )
      ),
      Seq.empty,
      Map.empty,
      Some(deriveNodeSeed(1, 0)),
      isRoot = false,
      Set.empty,
      view100,
    )

    val view110: TransactionView =
      view(
        create120,
        5,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create120, salt120Id)),
        Map.empty,
        Some(create120seed),
        isRoot = false,
        Set.empty,
      )

    val view11: TransactionView =
      view(
        exercise12,
        4,
        Set(exercise12Id),
        Seq(
          asSerializable(
            exercise12Id,
            exercise12Instance,
            metadataFromExercise(exercise12),
            ledgerTime,
          )
        ),
        Seq.empty,
        Map.empty,
        Some(deriveNodeSeed(1, 2)),
        isRoot = false,
        Set.empty,
        view110,
      )

    val view1: TransactionView =
      view(
        exercise1,
        1,
        Set.empty,
        Seq(
          asSerializable(
            exercise1Id,
            exercise1Instance,
            metadataFromExercise(exercise1),
            ledgerTime,
          )
        ),
        Seq(
          serializableFromCreate(create11, salt11Id),
          serializableFromCreate(create13, salt13Id),
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
        create2,
        6,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create2, salt2Id)),
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
      genCreate3X(lfCreate120Id, genCreate120Inst(create100Id))

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
  }

  /** Transaction structure:
    * 0. create
    * 1. exercise(0)
    * 1.0. create
    * 1.1. exerciseN(1.0)
    * 1.1.0. create
    * 1.2. exercise(1.1.0)
    * 1.3. exercise(1.0)
    *
    * View structure:
    * 0. view0
    * 1. view1
    * 1.1. view10
    */
  case object TransientContracts extends ExampleTransaction {

    override def cryptoOps: HashOps with RandomOps = ExampleTransactionFactory.this.cryptoOps

    override def toString: String = "transaction with transient contracts"

    def stakeholders: Set[LfPartyId] = Set(submitter, observer)
    def genCreate(
        cid: LfContractId,
        contractInst: LfContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = Set(submitter),
        observers = Set(observer),
      )

    val create0Inst: LfContractInst = contractInstance()
    val create0seed: LfHash = deriveNodeSeed(0)
    val create0disc: LfHash = discriminator(create0seed, stakeholders)
    val lfCreate0Id: LfContractId = LfContractId.V1(create0disc)
    val lfCreate0: LfNodeCreate = genCreate(lfCreate0Id, create0Inst)

    def genExercise(cid: LfContractId, childIndices: List[Int]): LfNodeExercises =
      exerciseNode(
        cid,
        actingParties = Set(submitter),
        signatories = Set(submitter),
        observers = Set(observer),
        children = childIndices.map(nodeId),
      )
    val lfExercise1: LfNodeExercises = genExercise(lfCreate0Id, List(2, 3, 5, 6))

    val create10Inst: LfContractInst = contractInstance()
    val create10seed: LfHash = deriveNodeSeed(1, 0)
    val create10disc: LfHash = discriminator(create10seed, stakeholders)
    val lfCreate10Id: LfContractId = LfContractId.V1(create10disc)
    val lfCreate10: LfNodeCreate = genCreate(lfCreate10Id, create10Inst)

    def genExerciseN(cid: LfContractId, childIndex: Int): LfNodeExercises =
      exerciseNode(
        cid,
        consuming = false,
        actingParties = Set(submitter),
        signatories = Set(submitter, signatory),
        observers = Set(observer),
        children = List(nodeId(childIndex)),
      )
    val lfExercise11: LfNodeExercises = genExerciseN(lfCreate10Id, 4)

    val create110seed: LfHash = deriveNodeSeed(1, 1, 0)
    val create110disc: LfHash = discriminator(create110seed, Set(submitter, signatory))
    def genCreate110(
        cid: LfContractId,
        contractInst: LfContractInst,
    ): LfNodeCreate =
      createNode(
        cid,
        contractInstance = contractInst,
        signatories = Set(submitter, signatory),
        observers = Set.empty,
      )

    val create110Inst: LfContractInst = contractInstance()
    val lfCreate110Id: LfContractId = LfContractId.V1(create110disc)
    val lfCreate110: LfNodeCreate = genCreate110(lfCreate110Id, create110Inst)

    val lfExercise12: LfNodeExercises = genExercise(lfCreate110Id, List.empty)

    val lfExercise13: LfNodeExercises = genExercise(lfCreate10Id, List.empty)

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

    val create0SerInst: SerializableRawContractInstance =
      asSerializableRaw(create0Inst)
    val (salt0Id, create0Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(0, 2),
        0,
        0,
        create0Inst,
        create0disc,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val create0: LfNodeCreate = genCreate(create0Id, create0Inst)

    val exercise1: LfNodeExercises = genExercise(create0Id, List(2, 3, 5, 6))

    val create10SerInst: SerializableRawContractInstance =
      asSerializableRaw(create10Inst)
    val (salt10Id, create10Id): (Salt, LfContractId) =
      fromDiscriminator(
        rootViewPosition(1, 2),
        1,
        0,
        create10Inst,
        create10disc,
        signatories = Set(submitter),
        observers = Set(observer),
      )
    val create10: LfNodeCreate = genCreate(create10Id, create10Inst)

    val exercise11: LfNodeExercises = genExerciseN(create10Id, 4)

    val create110SerInst: SerializableRawContractInstance = asSerializableRaw(create110Inst)
    val (salt110Id, create110Id): (Salt, LfContractId) =
      fromDiscriminator(
        subViewIndex(0, 1) +: rootViewPosition(1, 2),
        2,
        0,
        create110Inst,
        create110disc,
        signatories = Set(submitter, signatory),
      )
    val create110: LfNodeCreate = genCreate110(create110Id, create110Inst)

    val exercise12: LfNodeExercises = genExercise(create110Id, List.empty)

    val exercise13: LfNodeExercises = genExercise(create10Id, List.empty)

    val view0: TransactionView =
      view(
        create0,
        0,
        Set.empty,
        Seq.empty,
        Seq(serializableFromCreate(create0, salt0Id)),
        Map.empty,
        Some(create0seed),
        isRoot = true,
        Set.empty,
      )

    val view10: TransactionView = view(
      exercise11,
      2,
      Set.empty,
      Seq(
        asSerializable(
          create10Id,
          create10Inst,
          ContractMetadata.tryCreate(create10.signatories, create10.stakeholders, None),
          salt = salt10Id,
        )
      ),
      Seq(serializableFromCreate(create110, salt110Id)),
      Map.empty,
      Some(deriveNodeSeed(1, 1)),
      isRoot = false,
      Set.empty,
    )

    val view1: TransactionView = view(
      exercise1,
      1,
      Set(create0Id, create10Id, create110Id),
      Seq(
        asSerializable(
          create0Id,
          create0Inst,
          ContractMetadata.tryCreate(create0.signatories, create0.stakeholders, None),
          salt = salt0Id,
        )
      ),
      Seq(serializableFromCreate(create10, salt10Id)),
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

    override def versionedSuffixedTransaction: LfVersionedTransaction =
      transaction(
        Seq(0, 1),
        create0,
        exercise1,
        create10,
        exercise11,
        create110,
        exercise12,
        exercise13,
      )
  }
}
