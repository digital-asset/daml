// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.lf.transaction.ContractStateMachine.KeyInactive
import com.daml.lf.transaction.Transaction.{KeyActive, KeyCreate, KeyInput, NegativeKeyLookup}
import com.daml.lf.transaction.{ContractKeyUniquenessMode, ContractStateMachine, Util}
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractKeyResolutionError,
  MissingContractKeyLookupError,
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfKeyResolver, LfPackageId, LfPartyId, checked}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Generate transaction trees as used from protocol version [[com.digitalasset.canton.version.ProtocolVersion.v5]] on
  */
class TransactionTreeFactoryImplV3(
    submitterParticipant: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    contractSerializer: (LfContractInst, AgreementText) => SerializableRawContractInstance,
    cryptoOps: HashOps & HmacOps,
    uniqueContractKeys: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TransactionTreeFactoryImpl(
      submitterParticipant,
      domainId,
      protocolVersion,
      contractSerializer,
      cryptoOps,
      loggerFactory,
    ) {

  private val initialCsmState: ContractStateMachine.State[Unit] =
    ContractStateMachine.initial[Unit](
      if (uniqueContractKeys) ContractKeyUniquenessMode.Strict else ContractKeyUniquenessMode.Off
    )

  private[submission] def buildPackagePreference(
      decomposition: TransactionViewDecomposition
  ): Set[LfPackageId] = {

    def nodePref(n: LfActionNode): Set[(LfPackageName, LfPackageId)] = n match {
      case ex: LfNodeExercises if ex.interfaceId.isDefined =>
        ex.packageName match {
          case Some(packageName) => Set((packageName, ex.templateId.packageId))
          case None =>
            throw new IllegalStateException(s"LF 1.16 nodes must have package name [$ex]")
        }
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

    /* LF 1.16 will be enabled as part of ProtocolVersion.v6 */
    if (Util.sharedKey(decomposition.lfNode.version)) {
      val preferences = go(List(decomposition), Set.empty)
      MapsUtil.toNonConflictingMap(preferences) match {
        case Right(map) => map.values.toSet
        case Left(conflicts) =>
          throw new IllegalArgumentException(
            s"Detected conflicting package preferences [$conflicts]"
          )
      }
    } else {
      Set.empty[LfPackageId]
    }
  }

  protected[submission] class State private (
      override val mediator: MediatorRef,
      override val transactionUUID: UUID,
      override val ledgerTime: CantonTimestamp,
      override protected val salts: Iterator[Salt],
      initialResolver: LfKeyResolver,
  ) extends TransactionTreeFactoryImpl.State {

    /** An [[com.digitalasset.canton.protocol.LfGlobalKey]] stores neither the
      * [[com.digitalasset.canton.protocol.LfTransactionVersion]] to be used during serialization
      * nor the maintainers, which we need to cache in case no contract is found.
      *
      * Out parameter that stores version and maintainers for all keys
      * that have been referenced by an already-processed node.
      */
    val keyVersionAndMaintainers: mutable.Map[LfGlobalKey, (LfTransactionVersion, Set[LfPartyId])] =
      mutable.Map.empty

    /** Out parameter for the [[com.daml.lf.transaction.ContractStateMachine.State]]
      *
      * The state of the [[com.daml.lf.transaction.ContractStateMachine]]
      * after iterating over the following nodes in execution order:
      * 1. The iteration starts at the root node of the current view.
      * 2. The iteration includes all processed nodes of the view. This includes the nodes of fully processed subviews.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var csmState: ContractStateMachine.State[Unit] = initialCsmState

    /** This resolver is used to feed [[com.daml.lf.transaction.ContractStateMachine.State.handleLookupWith]]
      * if `uniqueContractKeys` is false.
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

  private[submission] object State {
    private[submission] def submission(
        transactionSeed: SaltSeed,
        mediator: MediatorRef,
        transactionUUID: UUID,
        ledgerTime: CantonTimestamp,
        nextSaltIndex: Int,
        keyResolver: LfKeyResolver,
    ): State = {
      val salts = LazyList
        .from(nextSaltIndex)
        .map(index => Salt.tryDeriveSalt(transactionSeed, index, cryptoOps))
      new State(mediator, transactionUUID, ledgerTime, salts.iterator, keyResolver)
    }

    private[submission] def validation(
        mediator: MediatorRef,
        transactionUUID: UUID,
        ledgerTime: CantonTimestamp,
        salts: Iterable[Salt],
        keyResolver: LfKeyResolver,
    ): State = new State(mediator, transactionUUID, ledgerTime, salts.iterator, keyResolver)
  }

  override protected def stateForSubmission(
      transactionSeed: SaltSeed,
      mediator: MediatorRef,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      keyResolver: LfKeyResolver,
      nextSaltIndex: Int,
  ): State =
    State.submission(
      transactionSeed,
      mediator,
      transactionUUID,
      ledgerTime,
      nextSaltIndex,
      keyResolver,
    )

  override protected def stateForValidation(
      mediator: MediatorRef,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      salts: Iterable[Salt],
      keyResolver: LfKeyResolver,
  ): State = State.validation(mediator, transactionUUID, ledgerTime, salts, keyResolver)

  override protected def createView(
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
    val subViewKeyResolutions = mutable.Map.empty[LfGlobalKey, SerializableKeyResolution]

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createIndex = 0

    val nbSubViews = view.allNodes.count {
      case _: TransactionViewDecomposition.NewView => true
      case _ => false
    }
    val subviewIndex = TransactionSubviews.indices(protocolVersion, nbSubViews).iterator
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
              val keyResolutionsInSubview = v.globalKeyInputs.fmap(_.asSerializable)
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
      packagePreference = buildPackagePreference(view)
      actionDescription = createActionDescription(suffixedRootNode, seed, packagePreference)
      viewCommonData = createViewCommonData(view, viewCommonDataSalt)
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

  /** Check that we correctly reconstruct the csm state machine
    * Canton does not distinguish between the different com.daml.lf.transaction.Transaction.KeyInactive forms right now
    */
  private def checkCsmStateMatchesView(
      csmState: ContractStateMachine.State[Unit],
      transactionView: TransactionView,
      viewPosition: ViewPosition,
  )(implicit traceContext: TraceContext): Unit = {
    val viewGki = transactionView.globalKeyInputs.fmap(_.resolution)
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
    // Reconstruction of the active ledger state works currently only in UCK mode
    if (uniqueContractKeys) {
      // The locally created contracts should also be computable in non-UCK mode from the view data.
      // However, `activeLedgerState` as a whole can be reconstructed only in UCK mode,
      // and therefore we check this only in UCK mode.
      ErrorUtil.requireState(
        transactionView.activeLedgerState.isEquivalent(csmState.activeState),
        show"Failed to reconstruct ActiveLedgerState $viewPosition.\n  Reconstructed: ${transactionView.activeLedgerState}\n  Expected: ${csmState.activeState}",
      )
    }
  }

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
    * - In mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]],
    *   the first node (in execution order) involving the key `k` determines the key's resolution for the view.
    *   So the first node `n` in execution order involving `k` is an Exercise, Fetch, or positive LookupByKey node.
    * - In mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Off]],
    *   the first by-key node (in execution order, including Creates) determines the global key input of the view.
    *   So the first by-key node `n` is an ExerciseByKey, FetchByKey, or positive LookupByKey node.
    * In particular, `n` cannot be a Create node because then the resolution for the view
    * would be [[com.daml.lf.transaction.ContractStateMachine.KeyInactive]].
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
      subviewKeyResolutions: collection.Map[LfGlobalKey, SerializableKeyResolution],
  )(implicit
      traceContext: TraceContext
  ): Either[TransactionTreeConversionError, Map[LfGlobalKey, SerializableKeyResolution]] = {
    ErrorUtil.requireArgument(
      subviewKeyResolutions.keySet.subsetOf(viewKeyInputs.keySet),
      s"Global key inputs of subview not part of the global key inputs of the parent view. Missing keys: ${subviewKeyResolutions.keySet
          .diff(viewKeyInputs.keySet)}",
    )

    def resolutionFor(
        key: LfGlobalKey,
        keyInput: KeyInput,
    ): Either[MissingContractKeyLookupError, SerializableKeyResolution] = {
      keyVersionAndMaintainers.get(key).toRight(MissingContractKeyLookupError(key)).map {
        case (lfVersion, maintainers) =>
          val resolution = keyInput match {
            case KeyActive(cid) => AssignedKey(cid)(lfVersion)
            case KeyCreate | NegativeKeyLookup => FreeKey(maintainers)(lfVersion)
          }
          resolution
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
}
