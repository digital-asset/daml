// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.data.ViewConfirmationParameters.InvalidViewConfirmationParameters
import com.digitalasset.canton.data.*
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.*
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactoryImpl.*
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, LfTransactionUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.transaction.ContractStateMachine.KeyInactive
import com.digitalasset.daml.lf.transaction.Transaction.{
  KeyActive,
  KeyCreate,
  KeyInput,
  NegativeKeyLookup,
}
import com.digitalasset.daml.lf.transaction.{ContractKeyUniquenessMode, ContractStateMachine}
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
class TransactionTreeFactoryImpl(
    participantId: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    contractSerializer: LfContractInst => SerializableRawContractInstance,
    cryptoOps: HashOps & HmacOps,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TransactionTreeFactory
    with NamedLogging {

  private val unicumGenerator = new UnicumGenerator(cryptoOps)
  private val cantonContractIdVersion = AuthenticatedContractIdVersionV10
  private val transactionViewDecompositionFactory = TransactionViewDecompositionFactory

  override def createTransactionTree(
      transaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      workflowId: Option[WorkflowId],
      mediator: MediatorGroupRecipient,
      transactionSeed: SaltSeed,
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: SerializableContractOfId,
      keyResolver: LfKeyResolver,
      maxSequencingTime: CantonTimestamp,
      validatePackageVettings: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, GenTransactionTree] = {
    val metadata = transaction.metadata
    val state = stateForSubmission(
      transactionSeed,
      mediator,
      transactionUuid,
      metadata.ledgerTime,
      keyResolver,
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
        topologySnapshot,
        transaction,
        RollbackContext.empty,
        Some(participantId.adminParty.toLf),
      )

    val commonMetadata = CommonMetadata
      .create(cryptoOps, protocolVersion)(
        domainId,
        mediator,
        commonMetadataSalt,
        transactionUuid,
      )

    for {
      submitterMetadata <- SubmitterMetadata
        .fromSubmitterInfo(cryptoOps)(
          submitterActAs = submitterInfo.actAs,
          submitterApplicationId = submitterInfo.applicationId,
          submitterCommandId = submitterInfo.commandId,
          submitterSubmissionId = submitterInfo.submissionId,
          submitterDeduplicationPeriod = submitterInfo.deduplicationPeriod,
          submittingParticipant = participantId,
          salt = submitterMetadataSalt,
          maxSequencingTime,
          protocolVersion = protocolVersion,
        )
        .leftMap(SubmitterMetadataError)
        .toEitherT[FutureUnlessShutdown]

      rootViewDecompositions <- EitherT
        .liftF(rootViewDecompositionsF)
        .mapK(FutureUnlessShutdown.outcomeK)

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
        else EitherT.rightT[FutureUnlessShutdown, TransactionTreeConversionError](())

      rootViews <- createRootViews(rootViewDecompositions, state, contractOfId)
        .map(rootViews =>
          GenTransactionTree.tryCreate(cryptoOps)(
            submitterMetadata,
            commonMetadata,
            participantMetadata,
            MerkleSeq.fromSeq(cryptoOps, protocolVersion)(rootViews),
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield rootViews
  }

  private def stateForSubmission(
      transactionSeed: SaltSeed,
      mediator: MediatorGroupRecipient,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      keyResolver: LfKeyResolver,
  ): State = {
    val salts = LazyList
      .from(0)
      .map(index => Salt.tryDeriveSalt(transactionSeed, index, cryptoOps))
    new State(mediator, transactionUUID, ledgerTime, salts.iterator, keyResolver)
  }

  /** compute set of required packages for each party */
  private def requiredPackagesByParty(
      rootViewDecompositions: Seq[TransactionViewDecomposition.NewView]
  ): Map[LfPartyId, Set[PackageId]] = {
    def requiredPackagesByParty(
        rootViewDecomposition: TransactionViewDecomposition.NewView,
        parentInformees: Set[LfPartyId],
    ): Map[LfPartyId, Set[PackageId]] = {
      val allInformees =
        parentInformees ++ rootViewDecomposition.viewConfirmationParameters.informees
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

  private def createView(
      view: TransactionViewDecomposition.NewView,
      viewPosition: ViewPosition,
      state: State,
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, TransactionView] = {
    state.signalRollbackScope(view.rbContext.rollbackScope)

    // reset to a fresh state with projected resolver before visiting the subtree
    val previousCsmState = state.csmState
    val previousResolver = state.currentResolver
    state.currentResolver = state.csmState.projectKeyResolver(previousResolver)
    state.csmState = initialCsmState

    // Process core nodes and subviews
    val coreCreatedBuilder =
      List.newBuilder[(LfNodeCreate, RollbackScope)] // contract IDs have already been suffixed
    val coreOtherBuilder = // contract IDs have not yet been suffixed
      List.newBuilder[((LfNodeId, LfActionNode), RollbackScope)]
    val childViewsBuilder = Seq.newBuilder[TransactionView]
    val subViewKeyResolutions =
      mutable.Map.empty[LfGlobalKey, LfVersioned[SerializableKeyResolution]]

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createIndex = 0

    val nbSubViews = view.allNodes.count {
      case _: TransactionViewDecomposition.NewView => true
      case _ => false
    }
    val subviewIndex = TransactionSubviews.indices(nbSubViews).iterator
    val createdInView = mutable.Set.empty[LfContractId]

    def fromEither[A <: TransactionTreeConversionError, B](
        either: Either[A, B]
    ): EitherT[Future, TransactionTreeConversionError, B] =
      EitherT.fromEither(either.leftWiden[TransactionTreeConversionError])

    for {
      // Compute salts
      viewCommonDataSalt <- fromEither(state.nextSalt())
      viewParticipantDataSalt <- fromEither(state.nextSalt())
      _ <- MonadUtil.sequentialTraverse_(view.allNodes) {
        case childView: TransactionViewDecomposition.NewView =>
          // Compute subviews, recursively
          createView(childView, subviewIndex.next() +: viewPosition, state, contractOfId)
            .map { v =>
              childViewsBuilder += v
              val createdInSubview = state.createdContractsInView
              createdInView ++= createdInSubview
              val keyResolutionsInSubview = v.globalKeyInputs.fmap(_.map(_.asSerializable))
              MapsUtil.extendMapWith(subViewKeyResolutions, keyResolutionsInSubview) {
                (accRes, _) => accRes
              }
            }

        case TransactionViewDecomposition.SameView(lfActionNode, nodeId, rbContext) =>
          val rbScope = rbContext.rollbackScope
          val suffixedNode = lfActionNode match {
            case createNode: LfNodeCreate =>
              val suffixedNode = updateStateWithContractCreation(
                nodeId,
                createNode,
                viewParticipantDataSalt,
                viewPosition,
                createIndex,
                state,
              )
              coreCreatedBuilder += (suffixedNode -> rbScope)
              createdInView += suffixedNode.coid
              createIndex += 1
              suffixedNode
            case lfNode: LfActionNode =>
              val suffixedNode = trySuffixNode(state)(nodeId -> lfNode)
              coreOtherBuilder += ((nodeId, lfNode) -> rbScope)
              suffixedNode
          }

          suffixedNode.keyOpt.foreach { case LfGlobalKeyWithMaintainers(gkey, maintainers) =>
            state.keyVersionAndMaintainers += (gkey -> (suffixedNode.version -> maintainers))
          }

          state.signalRollbackScope(rbScope)

          EitherT.fromEither[Future]({
            for {
              resolutionForModeOff <- suffixedNode match {
                case lookupByKey: LfNodeLookupByKey
                    if state.csmState.mode == ContractKeyUniquenessMode.Off =>
                  val gkey = lookupByKey.key.globalKey
                  state.currentResolver.get(gkey).toRight(MissingContractKeyLookupError(gkey))
                case _ => Right(KeyInactive) // dummy value, as resolution is not used
              }
              nextState <- state.csmState
                .handleNode((), suffixedNode, resolutionForModeOff)
                .leftMap(ContractKeyResolutionError)
            } yield {
              state.csmState = nextState
            }
          })
      }
      _ = state.signalRollbackScope(view.rbContext.rollbackScope)

      coreCreatedNodes = coreCreatedBuilder.result()
      // Translate contract ids in untranslated core nodes
      // This must be done only after visiting the whole action (rather than just the node)
      // because an Exercise result may contain an unsuffixed contract ID of a contract
      // that was created in the consequences of the exercise, i.e., we know the suffix only
      // after we have visited the create node.
      coreOtherNodes = coreOtherBuilder.result().map { case (nodeInfo, rbc) =>
        (checked(trySuffixNode(state)(nodeInfo)), rbc)
      }
      childViews = childViewsBuilder.result()

      suffixedRootNode = coreOtherNodes.headOption
        .orElse(coreCreatedNodes.headOption)
        .map { case (node, _) => node }
        .getOrElse(
          throw new IllegalArgumentException(s"The received view has no core nodes. $view")
        )

      // Compute the parameters of the view
      seed = view.rootSeed
      packagePreference <- EitherT.fromEither[Future](buildPackagePreference(view))
      actionDescription = createActionDescription(suffixedRootNode, seed, packagePreference)
      viewCommonData = createViewCommonData(view, viewCommonDataSalt).fold(
        ErrorUtil.internalError,
        identity,
      )
      viewKeyInputs = state.csmState.globalKeyInputs
      resolvedK <- EitherT.fromEither[Future](
        resolvedKeys(
          viewKeyInputs,
          state.keyVersionAndMaintainers,
          subViewKeyResolutions,
        )
      )
      viewParticipantData <- createViewParticipantData(
        coreCreatedNodes,
        coreOtherNodes,
        childViews,
        state.createdContractInfo,
        resolvedK,
        actionDescription,
        viewParticipantDataSalt,
        contractOfId,
        view.rbContext,
      )

      // fast-forward the former state over the subtree
      nextCsmState <- EitherT.fromEither[Future](
        previousCsmState
          .advance(
            // advance ignores the resolver in mode Strict
            if (state.csmState.mode == ContractKeyUniquenessMode.Strict) Map.empty
            else previousResolver,
            state.csmState,
          )
          .leftMap(ContractKeyResolutionError(_): TransactionTreeConversionError)
      )
    } yield {
      // Compute the result
      val subviews = TransactionSubviews(childViews)(protocolVersion, cryptoOps)
      val transactionView =
        TransactionView.tryCreate(cryptoOps)(
          viewCommonData,
          viewParticipantData,
          subviews,
          protocolVersion,
        )

      checkCsmStateMatchesView(state.csmState, transactionView, viewPosition)

      // Update the out parameters in the `State`
      state.createdContractsInView = createdInView
      state.csmState = nextCsmState
      state.currentResolver = previousResolver

      transactionView
    }
  }

  private def updateStateWithContractCreation(
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
    val serializedCantonContractInst = contractSerializer(cantonContractInst)

    val discriminator = createNode.coid match {
      case LfContractId.V1(discriminator, suffix) if suffix.isEmpty =>
        discriminator
      case _: LfContractId =>
        throw new IllegalStateException(
          s"Invalid contract id for created contract ${createNode.coid}"
        )
    }
    val contractMetadata = LfTransactionUtil.metadataFromCreate(createNode)
    val (contractSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
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

  private def trySuffixNode(
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

  private[submission] def buildPackagePreference(
      decomposition: TransactionViewDecomposition
  ): Either[ConflictingPackagePreferenceError, Set[LfPackageId]] = {

    def nodePref(n: LfActionNode): Set[(LfPackageName, LfPackageId)] = n match {
      case ex: LfNodeExercises if ex.interfaceId.isDefined =>
        Set(ex.packageName -> ex.templateId.packageId)
      case _ => Set.empty
    }

    @tailrec
    def go(
        decompositions: List[TransactionViewDecomposition],
        resolved: Set[(LfPackageName, LfPackageId)],
    ): Set[(LfPackageName, LfPackageId)] = {
      decompositions match {
        case Nil =>
          resolved
        case (v: SameView) :: others =>
          go(others, resolved ++ nodePref(v.lfNode))
        case (v: NewView) :: others =>
          go(v.tailNodes.toList ::: others, resolved ++ nodePref(v.lfNode))
      }
    }

    val preferences = go(List(decomposition), Set.empty)
    MapsUtil
      .toNonConflictingMap(preferences)
      .bimap(
        conflicts => ConflictingPackagePreferenceError(conflicts),
        map => map.values.toSet,
      )
  }

  private def createActionDescription(
      actionNode: LfActionNode,
      seed: Option[LfHash],
      packagePreference: Set[LfPackageId],
  ): ActionDescription =
    checked(
      ActionDescription.tryFromLfActionNode(actionNode, seed, packagePreference, protocolVersion)
    )

  private def createViewCommonData(
      rootView: TransactionViewDecomposition.NewView,
      salt: Salt,
  ): Either[InvalidViewConfirmationParameters, ViewCommonData] =
    ViewCommonData.create(cryptoOps)(
      rootView.viewConfirmationParameters,
      salt,
      protocolVersion,
    )

  /** The difference between `viewKeyInputs: Map[LfGlobalKey, KeyInput]` and
    * `subviewKeyResolutions: Map[LfGlobalKey, SerializableKeyResolution]`, computed as follows:
    * <ul>
    * <li>First, `keyVersionAndMaintainers` is used to compute
    *     `viewKeyResolutions: Map[LfGlobalKey, SerializableKeyResolution]` from `viewKeyInputs`.</li>
    * <li>Second, the result consists of all key-resolution pairs that are in `viewKeyResolutions`,
    *     but not in `subviewKeyResolutions`.</li>
    * </ul>
    *
    * Note: The following argument depends on how this method is used.
    * It just sits here because we can then use scaladoc referencing.
    *
    * All resolved contract IDs in the map difference are core input contracts by the following argument:
    * Suppose that the map difference resolves a key `k` to a contract ID `cid`.
    * - In mode [[com.digitalasset.daml.lf.transaction.ContractKeyUniquenessMode.Strict]],
    *   the first node (in execution order) involving the key `k` determines the key's resolution for the view.
    *   So the first node `n` in execution order involving `k` is an Exercise, Fetch, or positive LookupByKey node.
    * - In mode [[com.digitalasset.daml.lf.transaction.ContractKeyUniquenessMode.Off]],
    *   the first by-key node (in execution order, including Creates) determines the global key input of the view.
    *   So the first by-key node `n` is an ExerciseByKey, FetchByKey, or positive LookupByKey node.
    * In particular, `n` cannot be a Create node because then the resolution for the view
    * would be [[com.digitalasset.daml.lf.transaction.ContractStateMachine.KeyInactive]].
    * If this node `n` is in the core of the view, then `cid` is a core input and we are done.
    * If this node `n` is in a proper subview, then the aggregated global key inputs
    * [[com.digitalasset.canton.data.TransactionView.globalKeyInputs]]
    * of the subviews resolve `k` to `cid` (as resolutions from earlier subviews are preferred)
    * and therefore the map difference does not resolve `k` at all.
    *
    * @return `Left(...)` if `viewKeyInputs` contains a key not in the `keyVersionAndMaintainers.keySet`
    * @throws java.lang.IllegalArgumentException if `subviewKeyResolutions.keySet` is not a subset of `viewKeyInputs.keySet`
    */
  private def resolvedKeys(
      viewKeyInputs: Map[LfGlobalKey, KeyInput],
      keyVersionAndMaintainers: collection.Map[LfGlobalKey, (LfTransactionVersion, Set[LfPartyId])],
      subviewKeyResolutions: collection.Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]],
  )(implicit
      traceContext: TraceContext
  ): Either[TransactionTreeConversionError, Map[LfGlobalKey, LfVersioned[
    SerializableKeyResolution
  ]]] = {
    ErrorUtil.requireArgument(
      subviewKeyResolutions.keySet.subsetOf(viewKeyInputs.keySet),
      s"Global key inputs of subview not part of the global key inputs of the parent view. Missing keys: ${subviewKeyResolutions.keySet
          .diff(viewKeyInputs.keySet)}",
    )

    def resolutionFor(
        key: LfGlobalKey,
        keyInput: KeyInput,
    ): Either[MissingContractKeyLookupError, LfVersioned[SerializableKeyResolution]] = {
      keyVersionAndMaintainers.get(key).toRight(MissingContractKeyLookupError(key)).map {
        case (lfVersion, maintainers) =>
          val resolution = keyInput match {
            case KeyActive(cid) => AssignedKey(cid)
            case KeyCreate | NegativeKeyLookup => FreeKey(maintainers)
          }
          LfVersioned(lfVersion, resolution)
      }
    }

    for {
      viewKeyResolutionSeq <- viewKeyInputs.toSeq
        .traverse { case (gkey, keyInput) =>
          resolutionFor(gkey, keyInput).map(gkey -> _)
        }
    } yield {
      MapsUtil.mapDiff(viewKeyResolutionSeq.toMap, subviewKeyResolutions)
    }
  }

  private def createViewParticipantData(
      coreCreatedNodes: List[(LfNodeCreate, RollbackScope)],
      coreOtherNodes: List[(LfActionNode, RollbackScope)],
      childViews: Seq[TransactionView],
      createdContractInfo: collection.Map[LfContractId, SerializableContract],
      resolvedKeys: collection.Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]],
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

    def withInstance(
        contractId: LfContractId
    ): EitherT[Future, ContractLookupError, InputContract] = {
      val cons = consumedInCore.contains(contractId)
      createdContractInfo.get(contractId) match {
        case Some(info) =>
          EitherT.pure(InputContract(info, cons))
        case None =>
          contractOfId(contractId).map { c => InputContract(c, cons) }
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

  /** Check that we correctly reconstruct the csm state machine
    * Canton does not distinguish between the different com.digitalasset.daml.lf.transaction.Transaction.KeyInactive forms right now
    */
  private def checkCsmStateMatchesView(
      csmState: ContractStateMachine.State[Unit],
      transactionView: TransactionView,
      viewPosition: ViewPosition,
  )(implicit traceContext: TraceContext): Unit = {
    val viewGki = transactionView.globalKeyInputs.fmap(_.unversioned.resolution)
    val stateGki = csmState.globalKeyInputs.fmap(_.toKeyMapping)
    ErrorUtil.requireState(
      viewGki == stateGki,
      show"""Failed to reconstruct the global key inputs for the view at position $viewPosition.
            |  Reconstructed: $viewGki
            |  Expected: $stateGki""".stripMargin,
    )
    val viewLocallyCreated = transactionView.createdContracts.keySet
    val stateLocallyCreated = csmState.locallyCreated
    ErrorUtil.requireState(
      viewLocallyCreated == stateLocallyCreated,
      show"Failed to reconstruct created contracts for the view at position $viewPosition.\n  Reconstructed: $viewLocallyCreated\n  Expected: $stateLocallyCreated",
    )
    val viewInputContractIds = transactionView.inputContracts.keySet
    val stateInputContractIds = csmState.inputContractIds
    ErrorUtil.requireState(
      viewInputContractIds == stateInputContractIds,
      s"Failed to reconstruct input contracts for the view at position $viewPosition.\n  Reconstructed: $viewInputContractIds\n  Expected: $stateInputContractIds",
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  override def tryReconstruct(
      subaction: WellFormedTransaction[WithoutSuffixes],
      rootPosition: ViewPosition,
      mediator: MediatorGroupRecipient,
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

  private def stateForValidation(
      mediator: MediatorGroupRecipient,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      salts: Iterable[Salt],
      keyResolver: LfKeyResolver,
  ): State = new State(mediator, transactionUUID, ledgerTime, salts.iterator, keyResolver)

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
}

object TransactionTreeFactoryImpl {

  /** Creates a `TransactionTreeFactory`. */
  def apply(
      submittingParticipant: ParticipantId,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      cryptoOps: HashOps & HmacOps,
      loggerFactory: NamedLoggerFactory,
  )(implicit ex: ExecutionContext): TransactionTreeFactoryImpl =
    new TransactionTreeFactoryImpl(
      submittingParticipant,
      domainId,
      protocolVersion,
      contractSerializer,
      cryptoOps,
      loggerFactory,
    )

  private[submission] def contractSerializer(
      contractInst: LfContractInst
  ): SerializableRawContractInstance =
    SerializableRawContractInstance
      .create(contractInst)
      .leftMap { err =>
        throw new IllegalArgumentException(
          s"Unable to serialize contract instance, although it is contained in a well-formed transaction.\n$err\n$contractInst"
        )
      }
      .merge

  private val initialCsmState: ContractStateMachine.State[Unit] =
    ContractStateMachine.initial[Unit](ContractKeyUniquenessMode.Off)

  private class State(
      val mediator: MediatorGroupRecipient,
      val transactionUUID: UUID,
      val ledgerTime: CantonTimestamp,
      val salts: Iterator[Salt],
      initialResolver: LfKeyResolver,
  ) {

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

    /** An [[com.digitalasset.canton.protocol.LfGlobalKey]] stores neither the
      * [[com.digitalasset.canton.protocol.LfTransactionVersion]] to be used during serialization
      * nor the maintainers, which we need to cache in case no contract is found.
      *
      * Out parameter that stores version and maintainers for all keys
      * that have been referenced by an already-processed node.
      */
    val keyVersionAndMaintainers: mutable.Map[LfGlobalKey, (LfTransactionVersion, Set[LfPartyId])] =
      mutable.Map.empty

    /** Out parameter for the [[com.digitalasset.daml.lf.transaction.ContractStateMachine.State]]
      *
      * The state of the [[com.digitalasset.daml.lf.transaction.ContractStateMachine]]
      * after iterating over the following nodes in execution order:
      * 1. The iteration starts at the root node of the current view.
      * 2. The iteration includes all processed nodes of the view. This includes the nodes of fully processed subviews.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var csmState: ContractStateMachine.State[Unit] = initialCsmState

    /** This resolver is used to feed [[com.digitalasset.daml.lf.transaction.ContractStateMachine.State.handleLookupWith]].
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var currentResolver: LfKeyResolver = initialResolver

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var rollbackScope: RollbackScope = RollbackScope.empty

    def signalRollbackScope(target: RollbackScope): Unit = {
      val (pops, pushes) = RollbackScope.popsAndPushes(rollbackScope, target)
      for (_ <- 1 to pops) { csmState = csmState.endRollback() }
      for (_ <- 1 to pushes) { csmState = csmState.beginRollback() }
      rollbackScope = target
    }
  }
}
