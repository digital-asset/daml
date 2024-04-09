// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.ViewConfirmationParameters.InvalidViewConfirmationParameters
import com.digitalasset.canton.data.*
import com.digitalasset.canton.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.*
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, LfTransactionUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*

import java.util.UUID
import scala.annotation.{nowarn, tailrec}
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Factory class that can create the [[com.digitalasset.canton.data.GenTransactionTree]]s from a
  * [[com.digitalasset.canton.protocol.WellFormedTransaction]].
  *
  * @param contractSerializer used to serialize contract instances for contract ids
  *                           Will only be used to serialize contract instances contained in well-formed transactions.
  * @param cryptoOps is used to derive Merkle hashes and contract ids [[com.digitalasset.canton.crypto.HashOps]]
  *                  as well as salts and contract ids [[com.digitalasset.canton.crypto.HmacOps]]
  */
abstract class TransactionTreeFactoryImpl(
    participantId: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    contractSerializer: (LfContractInst, AgreementText) => SerializableRawContractInstance,
    cryptoOps: HashOps & HmacOps,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TransactionTreeFactory
    with NamedLogging {

  private val unicumGenerator = new UnicumGenerator(cryptoOps)
  private val cantonContractIdVersion = CantonContractIdVersion.fromProtocolVersion(protocolVersion)
  private val transactionViewDecompositionFactory = TransactionViewDecompositionFactory(
    protocolVersion
  )
  private val contractEnrichmentFactory = ContractEnrichmentFactory(protocolVersion)

  protected type State <: TransactionTreeFactoryImpl.State

  protected def stateForSubmission(
      transactionSeed: SaltSeed,
      mediator: MediatorRef,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      keyResolver: LfKeyResolver,
      nextSaltIndex: Int,
  ): State

  protected def stateForValidation(
      mediator: MediatorRef,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      salts: Iterable[Salt],
      keyResolver: LfKeyResolver,
  ): State

  override def createTransactionTree(
      transaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      confirmationPolicy: ConfirmationPolicy,
      workflowId: Option[WorkflowId],
      mediator: MediatorRef,
      transactionSeed: SaltSeed,
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: SerializableContractOfId,
      keyResolver: LfKeyResolver,
      maxSequencingTime: CantonTimestamp,
      validatePackageVettings: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, GenTransactionTree] = {
    val metadata = transaction.metadata
    val state = stateForSubmission(
      transactionSeed,
      mediator,
      transactionUuid,
      metadata.ledgerTime,
      keyResolver,
      nextSaltIndex = 0,
    )

    // Create salts
    val submitterMetadataSalt = checked(state.tryNextSalt())
    val commonMetadataSalt = checked(state.tryNextSalt())
    val participantMetadataSalt = checked(state.tryNextSalt())

    // Create fields
    val participantMetadata = ParticipantMetadata(cryptoOps)(
      metadata.ledgerTime,
      metadata.submissionTime,
      workflowId,
      participantMetadataSalt,
      protocolVersion,
    )

    val rootViewDecompositionsF =
      transactionViewDecompositionFactory.fromTransaction(
        confirmationPolicy,
        topologySnapshot,
        transaction,
        RollbackContext.empty,
        Some(participantId.adminParty.toLf),
      )

    val commonMetadata = CommonMetadata(cryptoOps, protocolVersion)(
      confirmationPolicy,
      domainId,
      mediator,
      commonMetadataSalt,
      transactionUuid,
    )

    for {
      submitterMetadata <- EitherT.fromEither[Future](
        SubmitterMetadata
          .fromSubmitterInfo(cryptoOps)(
            submitterActAs = submitterInfo.actAs,
            submitterApplicationId = submitterInfo.applicationId,
            submitterCommandId = submitterInfo.commandId,
            submitterSubmissionId = submitterInfo.submissionId,
            submitterDeduplicationPeriod = submitterInfo.deduplicationPeriod,
            submitterParticipant = participantId,
            salt = submitterMetadataSalt,
            maxSequencingTime,
            protocolVersion = protocolVersion,
          )
          .leftMap(SubmitterMetadataError)
      )
      rootViewDecompositions <- EitherT.liftF(rootViewDecompositionsF)

      _ = if (logger.underlying.isDebugEnabled) {
        val numRootViews = rootViewDecompositions.length
        val numViews = TransactionViewDecomposition.countNestedViews(rootViewDecompositions)
        logger.debug(
          s"Computed transaction tree with total=$numViews for #root-nodes=$numRootViews"
        )
      }

      _ <-
        if (validatePackageVettings)
          UsableDomain
            .resolveParticipantsAndCheckPackagesVetted(
              domainId = domainId,
              snapshot = topologySnapshot,
              requiredPackagesByParty = requiredPackagesByParty(rootViewDecompositions),
            )
            .leftMap(_.transformInto[UnknownPackageError])
        else EitherT.rightT[Future, TransactionTreeConversionError](())

      rootViews <- createRootViews(rootViewDecompositions, state, contractOfId)
        .map(rootViews =>
          GenTransactionTree.tryCreate(cryptoOps)(
            submitterMetadata,
            commonMetadata,
            participantMetadata,
            MerkleSeq.fromSeq(cryptoOps, protocolVersion)(rootViews),
          )
        )
    } yield rootViews
  }

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  override def tryReconstruct(
      subaction: WellFormedTransaction[WithoutSuffixes],
      rootPosition: ViewPosition,
      confirmationPolicy: ConfirmationPolicy,
      mediator: MediatorRef,
      submittingParticipantO: Option[ParticipantId],
      viewSalts: Iterable[Salt],
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: SerializableContractOfId,
      rbContext: RollbackContext,
      keyResolver: LfKeyResolver,
  )(implicit traceContext: TraceContext): EitherT[
    Future,
    TransactionTreeConversionError,
    (TransactionView, WellFormedTransaction[WithSuffixes]),
  ] = {
    /* We ship the root node of the view with suffixed contract IDs.
     * If this is a create node, reinterpretation will allocate an unsuffixed contract id instead of the one given in the node.
     * If this is an exercise node, the exercise result may contain unsuffixed contract ids created in the body of the exercise.
     * Accordingly, the reinterpreted transaction must first be suffixed before we can compare it against
     * the shipped views.
     * Ideally we'd ship only the inputs needed to reconstruct the transaction rather than computed data
     * such as exercise results and created contract IDs.
     */

    ErrorUtil.requireArgument(
      subaction.unwrap.roots.length == 1,
      s"Subaction must have a single root node, but has ${subaction.unwrap.roots.iterator.mkString(", ")}",
    )

    val metadata = subaction.metadata
    val state = stateForValidation(
      mediator,
      transactionUuid,
      metadata.ledgerTime,
      viewSalts,
      keyResolver,
    )

    val decompositionsF =
      transactionViewDecompositionFactory.fromTransaction(
        confirmationPolicy,
        topologySnapshot,
        subaction,
        rbContext,
        submittingParticipantO.map(_.adminParty.toLf),
      )
    for {
      decompositions <- EitherT.liftF(decompositionsF)
      decomposition = checked(decompositions.head)
      view <- createView(decomposition, rootPosition, state, contractOfId)
    } yield {
      val suffixedNodes = state.suffixedNodes() transform {
        // Recover the children
        case (nodeId, ne: LfNodeExercises) =>
          checked(subaction.unwrap.nodes(nodeId)) match {
            case ne2: LfNodeExercises =>
              ne.copy(children = ne2.children)
            case _: LfNode =>
              throw new IllegalStateException(
                "Node type changed while constructing the transaction tree"
              )
          }
        case (_, nl: LfLeafOnlyActionNode) => nl
      }

      // keep around the rollback nodes (not suffixed as they don't have a contract id), so that we don't orphan suffixed nodes.
      val rollbackNodes = subaction.unwrap.nodes.collect { case tuple @ (_, _: LfNodeRollback) =>
        tuple
      }

      val suffixedTx = LfVersionedTransaction(
        subaction.unwrap.version,
        suffixedNodes ++ rollbackNodes,
        subaction.unwrap.roots,
      )
      view -> checked(WellFormedTransaction.normalizeAndAssert(suffixedTx, metadata, WithSuffixes))
    }
  }

  override def saltsFromView(view: TransactionView): Iterable[Salt] = {
    val salts = Iterable.newBuilder[Salt]

    def addSaltsFrom(subview: TransactionView): Unit = {
      // Salts must be added in the same order as they are requested by checkView
      salts += checked(subview.viewCommonData.tryUnwrap).salt
      salts += checked(subview.viewParticipantData.tryUnwrap).salt
    }

    @tailrec
    @nowarn("msg=match may not be exhaustive")
    def go(stack: Seq[TransactionView]): Unit = stack match {
      case Seq() =>
      case subview +: toVisit =>
        addSaltsFrom(subview)
        subview.subviews.assertAllUnblinded(hash =>
          s"View ${subview.viewHash} contains an unexpected blinded subview $hash"
        )
        go(subview.subviews.unblindedElements ++ toVisit)
    }

    go(Seq(view))

    salts.result()
  }

  /** compute set of required packages for each party */
  private def requiredPackagesByParty(
      rootViewDecompositions: Seq[TransactionViewDecomposition.NewView]
  ): Map[LfPartyId, Set[PackageId]] = {
    def requiredPackagesByParty(
        rootViewDecomposition: TransactionViewDecomposition.NewView,
        parentInformee: Set[LfPartyId],
    ): Map[LfPartyId, Set[PackageId]] = {
      val allInformees =
        parentInformee ++ rootViewDecomposition.viewConfirmationParameters.informeesIds
      val childRequirements =
        rootViewDecomposition.tailNodes.foldLeft(Map.empty[LfPartyId, Set[PackageId]]) {
          case (acc, newView: TransactionViewDecomposition.NewView) =>
            MapsUtil.mergeMapsOfSets(acc, requiredPackagesByParty(newView, allInformees))
          case (acc, _) => acc
        }
      val rootPackages = rootViewDecomposition.allNodes
        .collect { case sameView: TransactionViewDecomposition.SameView =>
          LfTransactionUtil.nodeTemplates(sameView.lfNode).map(_.packageId).toSet
        }
        .flatten
        .toSet

      allInformees.foldLeft(childRequirements) { case (acc, party) =>
        acc.updated(party, acc.getOrElse(party, Set()).union(rootPackages))
      }
    }

    rootViewDecompositions.foldLeft(Map.empty[LfPartyId, Set[PackageId]]) { case (acc, view) =>
      MapsUtil.mergeMapsOfSets(acc, requiredPackagesByParty(view, Set.empty))
    }
  }

  private def createRootViews(
      decompositions: Seq[TransactionViewDecomposition.NewView],
      state: State,
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, Seq[TransactionView]] = {

    // collect all contract ids referenced
    val preloadCids = Set.newBuilder[LfContractId]
    def go(view: TransactionViewDecomposition.NewView): Unit = {
      preloadCids.addAll(
        LfTransactionUtil.usedContractId(view.rootNode).toList
      )
      preloadCids.addAll(
        view.tailNodes
          .flatMap(x => LfTransactionUtil.usedContractId(x.lfNode))
          .toList
      )
      view.childViews.foreach(go)
    }
    decompositions.foreach(go)

    // prefetch contracts using resolver. while we execute this on each contract individually,
    // we know that the contract loader will batch these requests together at the level of the contract
    // store
    EitherT
      .right(
        preloadCids
          .result()
          .toList
          .parTraverse(cid => contractOfId(cid).value.map((cid, _)))
          .map(_.toMap)
      )
      .flatMap { preloaded =>
        def fromPreloaded(
            cid: LfContractId
        ): EitherT[Future, ContractLookupError, SerializableContract] = {
          preloaded.get(cid) match {
            case Some(value) => EitherT.fromEither(value)
            case None =>
              // if we ever missed a contract during prefetching due to mistake, then we can
              // fallback to the original loader
              logger.warn(s"Prefetch missed $cid")
              contractOfId(cid)
          }
        }
        // Creating the views sequentially
        MonadUtil.sequentialTraverse(
          decompositions.zip(MerkleSeq.indicesFromSeq(decompositions.size))
        ) { case (rootView, index) =>
          createView(rootView, index +: ViewPosition.root, state, fromPreloaded)
        }
      }
  }

  protected def createView(
      view: TransactionViewDecomposition.NewView,
      viewPosition: ViewPosition,
      state: State,
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, TransactionView]

  protected def updateStateWithContractCreation(
      nodeId: LfNodeId,
      createNode: LfNodeCreate,
      viewParticipantDataSalt: Salt,
      viewPosition: ViewPosition,
      createIndex: Int,
      state: State,
  )(implicit traceContext: TraceContext): LfNodeCreate = {
    val cantonContractInst = checked(
      LfTransactionUtil
        .suffixContractInst(state.unicumOfCreatedContract, cantonContractIdVersion)(
          createNode.versionedCoinst
        )
        .fold(
          cid =>
            throw new IllegalStateException(
              s"Invalid contract id $cid found in contract instance of create node"
            ),
          Predef.identity,
        )
    )
    val serializedCantonContractInst =
      contractSerializer(cantonContractInst, AgreementText(createNode.agreementText))

    val discriminator = createNode.coid match {
      case LfContractId.V1(discriminator, suffix) if suffix.isEmpty =>
        discriminator
      case _: LfContractId =>
        throw new IllegalStateException(
          s"Invalid contract id for created contract ${createNode.coid}"
        )
    }
    val contractMetadata = LfTransactionUtil.metadataFromCreate(createNode)
    val (contractSalt, unicum) = unicumGenerator
      .generateSaltAndUnicum(
        domainId,
        state.mediator,
        state.transactionUUID,
        viewPosition,
        viewParticipantDataSalt,
        createIndex,
        LedgerCreateTime(state.ledgerTime),
        contractMetadata,
        serializedCantonContractInst,
        cantonContractIdVersion,
      )
      .valueOr(err =>
        throw new IllegalArgumentException(s"Error generating contract salt and unicum: $err")
      )

    val contractId = cantonContractIdVersion.fromDiscriminator(discriminator, unicum)

    state.setUnicumFor(discriminator, unicum)

    val createdInfo = SerializableContract(
      contractId = contractId,
      rawContractInstance = serializedCantonContractInst,
      metadata = contractMetadata,
      ledgerCreateTime = LedgerCreateTime(state.ledgerTime),
      contractSalt = Some(contractSalt.unwrap),
    )
    state.setCreatedContractInfo(contractId, createdInfo)

    // No need to update the key because the key does not contain contract ids
    val suffixedNode =
      createNode.copy(coid = contractId, arg = cantonContractInst.unversioned.arg)
    state.addSuffixedNode(nodeId, suffixedNode)
    suffixedNode
  }

  protected def trySuffixNode(
      state: State
  )(idAndNode: (LfNodeId, LfActionNode)): LfActionNode = {
    val (nodeId, node) = idAndNode
    val suffixedNode = LfTransactionUtil
      .suffixNode(state.unicumOfCreatedContract, cantonContractIdVersion)(node)
      .fold(
        cid => throw new IllegalArgumentException(s"Invalid contract id $cid found"),
        Predef.identity,
      )
    state.addSuffixedNode(nodeId, suffixedNode)
    suffixedNode
  }

  protected def createActionDescription(
      actionNode: LfActionNode,
      seed: Option[LfHash],
      packagePreference: Set[LfPackageId],
  ): ActionDescription =
    checked(
      ActionDescription.tryFromLfActionNode(actionNode, seed, packagePreference, protocolVersion)
    )

  protected def createViewCommonData(
      rootView: TransactionViewDecomposition.NewView,
      salt: Salt,
  ): Either[InvalidViewConfirmationParameters, ViewCommonData] =
    ViewCommonData.create(cryptoOps)(
      rootView.viewConfirmationParameters,
      salt,
      protocolVersion,
    )

  protected def createViewParticipantData(
      coreCreatedNodes: List[(LfNodeCreate, RollbackScope)],
      coreOtherNodes: List[(LfActionNode, RollbackScope)],
      childViews: Seq[TransactionView],
      createdContractInfo: collection.Map[LfContractId, SerializableContract],
      resolvedKeys: collection.Map[LfGlobalKey, SerializableKeyResolution],
      actionDescription: ActionDescription,
      salt: Salt,
      contractOfId: SerializableContractOfId,
      rbContextCore: RollbackContext,
  ): EitherT[Future, TransactionTreeConversionError, ViewParticipantData] = {

    val consumedInCore =
      coreOtherNodes.mapFilter { case (an, rbScopeOther) =>
        // to be considered consumed, archival has to happen in the same rollback scope,
        if (rbScopeOther == rbContextCore.rollbackScope)
          LfTransactionUtil.consumedContractId(an)
        else None
      }.toSet
    val created = coreCreatedNodes.map { case (n, rbScopeCreate) =>
      val cid = n.coid
      // The preconditions of tryCreate are met as we have created all contract IDs of created contracts in this class.
      checked(
        CreatedContract
          .tryCreate(
            createdContractInfo(cid),
            consumedInCore = consumedInCore.contains(cid),
            rolledBack = rbScopeCreate != rbContextCore.rollbackScope,
          )
      )
    }
    // "Allowing for" duplicate contract creations under different rollback nodes is not necessary as contracts in
    // different positions receive different contract_ids as ensured by WellformednessCheck "Create nodes have unique
    // contract ids".

    val createdInSubviewsSeq = for {
      childView <- childViews
      subView <- childView.flatten
      createdContract <- subView.viewParticipantData.tryUnwrap.createdCore
    } yield createdContract.contract.contractId

    val createdInSubviews = createdInSubviewsSeq.toSet
    val createdInSameViewOrSubviews = createdInSubviewsSeq ++ created.map(_.contract.contractId)

    val usedCore = SortedSet.from(coreOtherNodes.flatMap({ case (node, _) =>
      LfTransactionUtil.usedContractId(node)
    }))
    val coreInputs = usedCore -- createdInSameViewOrSubviews
    val createdInSubviewArchivedInCore = consumedInCore intersect createdInSubviews

    val contractEnricher = contractEnrichmentFactory(coreOtherNodes)

    def withInstance(
        contractId: LfContractId
    ): EitherT[Future, ContractLookupError, InputContract] = {
      val cons = consumedInCore.contains(contractId)
      createdContractInfo.get(contractId) match {
        case Some(info) =>
          EitherT.pure(InputContract(info, cons))
        case None =>
          contractOfId(contractId).map { c => InputContract(contractEnricher(c), cons) }
      }
    }

    for {
      coreInputsWithInstances <- coreInputs.toSeq
        .parTraverse(cid => withInstance(cid).map(cid -> _))
        .leftWiden[TransactionTreeConversionError]
        .map(_.toMap)
      viewParticipantData <- EitherT
        .fromEither[Future](
          ViewParticipantData.create(cryptoOps)(
            coreInputs = coreInputsWithInstances,
            createdCore = created,
            createdInSubviewArchivedInCore = createdInSubviewArchivedInCore,
            resolvedKeys = resolvedKeys.toMap,
            actionDescription = actionDescription,
            rollbackContext = rbContextCore,
            salt = salt,
            protocolVersion = protocolVersion,
          )
        )
        .leftMap[TransactionTreeConversionError](ViewParticipantDataError)
    } yield viewParticipantData
  }
}

object TransactionTreeFactoryImpl {

  /** Creates a `TransactionTreeFactory`. */
  def apply(
      submitterParticipant: ParticipantId,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      cryptoOps: HashOps & HmacOps,
      uniqueContractKeys: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit ex: ExecutionContext): TransactionTreeFactoryImpl =
    new TransactionTreeFactoryImplV3(
      submitterParticipant,
      domainId,
      protocolVersion,
      contractSerializer,
      cryptoOps,
      uniqueContractKeys,
      loggerFactory,
    )

  private[submission] def contractSerializer(
      contractInst: LfContractInst,
      agreementText: AgreementText,
  ): SerializableRawContractInstance =
    SerializableRawContractInstance
      .create(contractInst, agreementText)
      .leftMap { err =>
        throw new IllegalArgumentException(
          s"Unable to serialize contract instance, although it is contained in a well-formed transaction.\n$err\n$contractInst"
        )
      }
      .merge

  trait State {
    def mediator: MediatorRef
    def transactionUUID: UUID
    def ledgerTime: CantonTimestamp

    protected def salts: Iterator[Salt]

    def nextSalt(): Either[TransactionTreeFactory.TooFewSalts, Salt] =
      Either.cond(salts.hasNext, salts.next(), TooFewSalts)

    def tryNextSalt()(implicit loggingContext: ErrorLoggingContext): Salt =
      nextSalt().valueOr { case TooFewSalts =>
        ErrorUtil.internalError(new IllegalStateException("No more salts available"))
      }

    private val suffixedNodesBuilder
        : mutable.Builder[(LfNodeId, LfActionNode), Map[LfNodeId, LfActionNode]] =
      Map.newBuilder[LfNodeId, LfActionNode]

    def suffixedNodes(): Map[LfNodeId, LfActionNode] = suffixedNodesBuilder.result()

    def addSuffixedNode(nodeId: LfNodeId, suffixedNode: LfActionNode): Unit = {
      suffixedNodesBuilder += nodeId -> suffixedNode
    }

    private val unicumOfCreatedContractMap: mutable.Map[LfHash, Unicum] = mutable.Map.empty

    def unicumOfCreatedContract: LfHash => Option[Unicum] = unicumOfCreatedContractMap.get

    def setUnicumFor(discriminator: LfHash, unicum: Unicum)(implicit
        loggingContext: ErrorLoggingContext
    ): Unit =
      unicumOfCreatedContractMap.put(discriminator, unicum).foreach { _ =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Two contracts have the same discriminator: $discriminator"
          )
        )
      }

    /** All contracts created by a node that has already been processed. */
    val createdContractInfo: mutable.Map[LfContractId, SerializableContract] =
      mutable.Map.empty

    def setCreatedContractInfo(
        contractId: LfContractId,
        createdInfo: SerializableContract,
    )(implicit loggingContext: ErrorLoggingContext): Unit =
      createdContractInfo.put(contractId, createdInfo).foreach { _ =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Two created contracts have the same contract id: $contractId"
          )
        )
      }

    /** Out parameter for contracts created in the view (including subviews). */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createdContractsInView: collection.Set[LfContractId] = Set.empty
  }
}
