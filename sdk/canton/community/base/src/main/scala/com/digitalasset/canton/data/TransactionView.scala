// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.lf.transaction.ContractStateMachine.{ActiveLedgerState, KeyMapping}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.TransactionView.InvalidView
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.{v0, v1, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil, NamedLoggingLazyVal}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import monocle.Lens
import monocle.macros.GenLens

/** Encapsulates a subaction of the underlying transaction.
  *
  * @param subviews the top-most subviews of this view
  * @throws TransactionView$.InvalidView if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
  */
final case class TransactionView private (
    viewCommonData: MerkleTree[ViewCommonData],
    viewParticipantData: MerkleTree[ViewParticipantData],
    subviews: TransactionSubviews,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransactionView.type],
) extends MerkleTreeInnerNode[TransactionView](hashOps)
    with HasProtocolVersionedWrapper[TransactionView]
    with HasLoggerName {

  @transient override protected lazy val companionObj: TransactionView.type = TransactionView

  if (viewCommonData.unwrap.isRight) {
    subviews.unblindedElementsWithIndex
      .find { case (view, _path) => view.viewCommonData == viewCommonData }
      .foreach { case (_view, path) =>
        throw InvalidView(
          s"The subview with index $path has an equal viewCommonData."
        )
      }
  }

  def subviewHashesConsistentWith(subviewHashes: Seq[ViewHash]): Boolean = {
    subviews.hashesConsistentWith(hashOps)(subviewHashes)
  }

  override def subtrees: Seq[MerkleTree[_]] =
    Seq[MerkleTree[_]](viewCommonData, viewParticipantData) ++ subviews.trees

  def tryUnblindViewParticipantData(
      fieldName: String
  )(implicit loggingContext: NamedLoggingContext): ViewParticipantData =
    viewParticipantData.unwrap.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          s"$fieldName of view $viewHash can be computed only if the view participant data is unblinded"
        )
      )
    )

  private def tryUnblindSubview(subview: MerkleTree[TransactionView], fieldName: String)(implicit
      loggingContext: NamedLoggingContext
  ): TransactionView =
    subview.unwrap.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          s"$fieldName of view $viewHash can be computed only if all subviews are unblinded, but ${subview.rootHash} is blinded"
        )
      )
    )

  override private[data] def withBlindedSubtrees(
      blindingCommandPerNode: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransactionView] =
    TransactionView.tryCreate(
      viewCommonData.doBlind(blindingCommandPerNode), // O(1)
      viewParticipantData.doBlind(blindingCommandPerNode), // O(1)
      subviews.doBlind(blindingCommandPerNode), // O(#subviews)
      representativeProtocolVersion,
    )(hashOps)

  private[data] def tryBlindForTransactionViewTree(
      viewPos: ViewPositionFromRoot
  ): TransactionView = {
    val isMainView = viewPos.isEmpty

    if (isMainView) this
    else {
      TransactionView.tryCreate(
        viewCommonData.blindFully,
        viewParticipantData.blindFully,
        subviews.tryBlindForTransactionViewTree(viewPos),
        representativeProtocolVersion,
      )(hashOps)
    }
  }

  val viewHash: ViewHash = ViewHash.fromRootHash(rootHash)

  /** Traverses all unblinded subviews `v1, v2, v3, ...` in pre-order and yields
    * `f(...f(f(z, v1), v2)..., vn)`
    */
  def foldLeft[A](z: A)(f: (A, TransactionView) => A): A =
    subviews.unblindedElements
      .to(LazyList)
      .foldLeft(f(z, this))((acc, subView) => subView.foldLeft(acc)(f))

  /** Yields all (direct and indirect) subviews of this view in pre-order.
    * The first element is this view.
    */
  lazy val flatten: Seq[TransactionView] =
    foldLeft(Seq.newBuilder[TransactionView])((acc, v) => acc += v).result()

  lazy val tryFlattenToParticipantViews: Seq[ParticipantTransactionView] =
    flatten.map(ParticipantTransactionView.tryCreate)

  /** Yields all (direct and indirect) subviews of this view in pre-order, along with the subview position
    * under the root view position `rootPos`. The first element is this view.
    */
  def allSubviewsWithPosition(rootPos: ViewPosition): Seq[(TransactionView, ViewPosition)] = {
    def helper(
        view: TransactionView,
        viewPos: ViewPosition,
    ): Seq[(TransactionView, ViewPosition)] = {
      (view, viewPos) +: view.subviews.unblindedElementsWithIndex.flatMap {
        case (view, viewIndex) => helper(view, viewIndex +: viewPos)
      }
    }

    helper(this, rootPos)
  }

  override def pretty: Pretty[TransactionView] = prettyOfClass(
    param("root hash", _.rootHash),
    param("view common data", _.viewCommonData),
    param("view participant data", _.viewParticipantData),
    param("subviews", _.subviews),
  )

  @VisibleForTesting
  private[data] def copy(
      viewCommonData: MerkleTree[ViewCommonData] = this.viewCommonData,
      viewParticipantData: MerkleTree[ViewParticipantData] = this.viewParticipantData,
      subviews: TransactionSubviews = this.subviews,
  ) =
    new TransactionView(viewCommonData, viewParticipantData, subviews)(
      hashOps,
      representativeProtocolVersion,
    )

  /** If the view with the given hash appears either as this view or one of its unblinded descendants,
    * replace it by the given view.
    * TODO(i12900): not stack safe unless we have limits on the depths of views.
    */
  def replace(h: ViewHash, v: TransactionView): TransactionView =
    if (viewHash == h) v
    else this.copy(subviews = subviews.mapUnblinded(_.replace(h, v)))

  protected def toProtoV0: v0.ViewNode = v0.ViewNode(
    viewCommonData = Some(MerkleTree.toBlindableNodeV0(viewCommonData)),
    viewParticipantData = Some(MerkleTree.toBlindableNodeV0(viewParticipantData)),
    subviews = subviews.toProtoV0,
  )

  protected def toProtoV1: v1.ViewNode = v1.ViewNode(
    viewCommonData = Some(MerkleTree.toBlindableNodeV1(viewCommonData)),
    viewParticipantData = Some(MerkleTree.toBlindableNodeV1(viewParticipantData)),
    subviews = Some(subviews.toProtoV1),
  )

  /** The global key inputs that the [[com.daml.lf.transaction.ContractStateMachine]] computes
    * while interpreting the root action of the view, enriched with the maintainers of the key and the
    * [[com.digitalasset.canton.protocol.LfTransactionVersion]] to be used for serializing the key.
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded
    */
  def globalKeyInputs(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfGlobalKey, KeyResolutionWithMaintainers] =
    _globalKeyInputs.get

  private[this] val _globalKeyInputs
      : NamedLoggingLazyVal[Map[LfGlobalKey, KeyResolutionWithMaintainers]] =
    NamedLoggingLazyVal[Map[LfGlobalKey, KeyResolutionWithMaintainers]] { implicit loggingContext =>
      val viewParticipantData = tryUnblindViewParticipantData("Global key inputs")

      subviews.assertAllUnblinded(hash =>
        s"Global key inputs of view $viewHash can be computed only if all subviews are unblinded, but ${hash} is blinded"
      )

      subviews.unblindedElements.foldLeft(viewParticipantData.resolvedKeysWithMaintainers) {
        (acc, subview) =>
          val subviewGki = subview.globalKeyInputs
          MapsUtil.mergeWith(acc, subviewGki) { (accRes, _subviewRes) => accRes }
      }
    }

  /** The input contracts of the view (including subviews).
    *
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded
    */
  def inputContracts(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfContractId, InputContract] = _inputsAndCreated.get._1

  /** The contracts appearing in create nodes in the view (including subviews).
    *
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded
    */
  def createdContracts(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfContractId, CreatedContractInView] = _inputsAndCreated.get._2

  private[this] val _inputsAndCreated: NamedLoggingLazyVal[
    (Map[LfContractId, InputContract], Map[LfContractId, CreatedContractInView])
  ] = NamedLoggingLazyVal[
    (Map[LfContractId, InputContract], Map[LfContractId, CreatedContractInView])
  ] { implicit loggingContext =>
    val vpd = viewParticipantData.unwrap.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          s"Inputs and created contracts of view $viewHash can be computed only if the view participant data is unblinded"
        )
      )
    )
    val currentRollbackScope = vpd.rollbackContext.rollbackScope
    subviews.assertAllUnblinded(hash =>
      s"Inputs and created contracts of view $viewHash can be computed only if all subviews are unblinded, but ${hash} is blinded"
    )
    val subviewInputsAndCreated = subviews.unblindedElements.map { subview =>
      val subviewVpd =
        subview.tryUnblindViewParticipantData("Inputs and created contracts")
      val created = subview.createdContracts
      val inputs = subview.inputContracts
      val subviewRollbackScope = subviewVpd.rollbackContext.rollbackScope
      // If the subview sits under a Rollback node in the view's core,
      // then the created contracts of the subview are all rolled back,
      // and all consuming inputs become non-consuming inputs.
      if (subviewRollbackScope != currentRollbackScope) {
        (
          inputs.fmap(_.copy(consumed = false)),
          created.fmap(_.copy(rolledBack = true)),
        )
      } else (inputs, created)
    }

    val createdCore = vpd.createdCore.map { contract =>
      contract.contract.contractId -> CreatedContractInView.fromCreatedContract(contract)
    }.toMap
    subviewInputsAndCreated.foldLeft((vpd.coreInputs, createdCore)) {
      case ((accInputs, accCreated), (subviewInputs, subviewCreated)) =>
        val subviewCreatedUpdated = subviewCreated.fmap { contract =>
          if (vpd.createdInSubviewArchivedInCore.contains(contract.contract.contractId))
            contract.copy(consumedInView = true)
          else contract
        }
        val accCreatedUpdated = accCreated.fmap { contract =>
          if (subviewInputs.get(contract.contract.contractId).exists(_.consumed))
            contract.copy(consumedInView = true)
          else contract
        }
        val nextCreated = MapsUtil.mergeWith(accCreatedUpdated, subviewCreatedUpdated) {
          (fromAcc, _) =>
            // By the contract ID allocation scheme, the contract IDs in the subviews are pairwise distinct
            // and distinct from `createdCore`
            // TODO(i12901) Check this invariant somewhere
            ErrorUtil.internalError(
              new IllegalStateException(
                s"Contract ${fromAcc.contract.contractId} is created multiple times in view $viewHash"
              )
            )
        }

        val subviewNontransientInputs = subviewInputs.filter { case (cid, _) =>
          !accCreated.contains(cid)
        }
        val nextInputs = MapsUtil.mergeWith(accInputs, subviewNontransientInputs) {
          (fromAcc, fromSubview) =>
            fromAcc.copy(consumed = fromAcc.consumed || fromSubview.consumed)
        }
        (nextInputs, nextCreated)
    }
  }

  /** The [[com.daml.lf.transaction.ContractStateMachine.ActiveLedgerState]]
    * the [[com.daml.lf.transaction.ContractStateMachine]] reaches after interpreting the root action of the view.
    *
    * Must only be used in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]]
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded.
    */
  def activeLedgerState(implicit
      loggingContext: NamedLoggingContext
  ): ActiveLedgerState[Unit] =
    _activeLedgerStateAndUpdatedKeys.get._1

  /** The keys that this view updates (including reassigning the key), along with the maintainers of the key.
    *
    * Must only be used in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]]
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded.
    */
  def updatedKeys(implicit loggingContext: NamedLoggingContext): Map[LfGlobalKey, Set[LfPartyId]] =
    _activeLedgerStateAndUpdatedKeys.get._2

  /** The keys that this view updates (including reassigning the key), along with the assignment of that key at the end of the transaction.
    *
    * Must only be used in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]]
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded.
    */
  def updatedKeyValues(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfGlobalKey, KeyMapping] = {
    val localActiveKeys = activeLedgerState.localActiveKeys
    def resolveKey(key: LfGlobalKey): KeyMapping =
      localActiveKeys.get(key) match {
        case None =>
          globalKeyInputs.get(key).map(_.resolution).flatten.filterNot(consumed.contains(_))
        case Some(mapping) => mapping
      }
    (localActiveKeys.keys ++ globalKeyInputs.keys).map(k => k -> resolveKey(k)).toMap
  }

  private[this] val _activeLedgerStateAndUpdatedKeys
      : NamedLoggingLazyVal[(ActiveLedgerState[Unit], Map[LfGlobalKey, Set[LfPartyId]])] =
    NamedLoggingLazyVal[(ActiveLedgerState[Unit], Map[LfGlobalKey, Set[LfPartyId]])] {
      implicit loggingContext =>
        val updatedKeysB = Map.newBuilder[LfGlobalKey, Set[LfPartyId]]
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var localKeys: Map[LfGlobalKey, LfContractId] = Map.empty

        inputContracts.foreach { case (cid, inputContract) =>
          // Consuming exercises under a rollback node are rewritten to non-consuming exercises in the view inputs.
          // So here we are looking only at key usages that are outside of rollback nodes (inside the view).
          if (inputContract.consumed) {
            inputContract.contract.metadata.maybeKeyWithMaintainers.foreach { kWithM =>
              val key = kWithM.globalKey
              updatedKeysB += (key -> kWithM.maintainers)
            }
          }
        }
        createdContracts.foreach { case (cid, createdContract) =>
          if (!createdContract.rolledBack) {
            createdContract.contract.metadata.maybeKeyWithMaintainers.foreach { kWithM =>
              val key = kWithM.globalKey
              updatedKeysB += (key -> kWithM.maintainers)
              if (!createdContract.consumedInView) {
                // If we have an active contract, we use that mapping.
                localKeys += key -> cid
              } else {
                if (!localKeys.contains(key)) {
                  // If all contracts are inactive, we arbitrarily use the first in createdContracts
                  // (createdContracts is not ordered)
                  localKeys += key -> cid
                }
              }
            }
          }
        }

        val locallyCreatedThisTimeline = createdContracts.collect {
          case (contractId, createdContract) if !createdContract.rolledBack => contractId
        }.toSet

        ActiveLedgerState(
          locallyCreatedThisTimeline = locallyCreatedThisTimeline,
          consumedBy = consumed,
          localKeys = localKeys,
        ) ->
          updatedKeysB.result()
    }

  def consumed(implicit loggingContext: NamedLoggingContext): Map[LfContractId, Unit] = {
    // In strict mode, every node involving a key updates the active ledger state
    // unless it is under a rollback node.
    // So it suffices to look at the created and input contracts
    // Contract consumption under a rollback is ignored.

    val consumedInputs = inputContracts.collect {
      // No need to check for contract.rolledBack because consumption under a rollback does not set the consumed flag
      case (cid, contract) if contract.consumed => cid -> ()
    }
    val consumedCreates = createdContracts.collect {
      // If the creation is rolled back, then so are all archivals
      // because a rolled-back create can only be used in the same or deeper rollback scopes,
      // as ensured by `WellformedTransaction.checkCreatedContracts`.
      case (cid, contract) if !contract.rolledBack && contract.consumedInView => cid -> ()
    }
    consumedInputs ++ consumedCreates
  }
}

object TransactionView
    extends HasProtocolVersionedWithContextCompanion[
      TransactionView,
      (HashOps, ConfirmationPolicy),
    ] {
  override def name: String = "TransactionView"
  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(0) -> LegacyProtoConverter(ProtocolVersion.v3)(v0.ViewNode)(
        supportedProtoVersion(_)(fromProtoV0),
        _.toProtoV0.toByteString,
      ),
      ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.ViewNode)(
        supportedProtoVersion(_)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
    )

  private def tryCreate(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      representativeProtocolVersion: RepresentativeProtocolVersion[TransactionView.type],
  )(hashOps: HashOps): TransactionView = {
    // Check consistency between protocol version and subviews structure
    val isProtoV0 =
      representativeProtocolVersion == protocolVersionRepresentativeFor(ProtocolVersion.v3)
    val isProtoV1 =
      representativeProtocolVersion == protocolVersionRepresentativeFor(ProtocolVersion.v4)
    val (isSubviewsV0, isSubviewsV1) = subviews match {
      case _: TransactionSubviewsV0 => (true, false)
      case _: TransactionSubviewsV1 => (false, true)
    }
    require(
      !isSubviewsV0 || !isProtoV1,
      s"TransactionSubviewsV0 cannot be used with representativeProtocolVersion $representativeProtocolVersion",
    )
    require(
      !isSubviewsV1 || !isProtoV0,
      s"TransactionSubviewsV1 cannot be used with representativeProtocolVersion $representativeProtocolVersion",
    )

    new TransactionView(viewCommonData, viewParticipantData, subviews)(
      hashOps,
      representativeProtocolVersion,
    )
  }

  /** Creates a view.
    *
    * @throws InvalidView if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
    */
  def tryCreate(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      protocolVersion: ProtocolVersion,
  ): TransactionView =
    tryCreate(
      viewCommonData,
      viewParticipantData,
      subviews,
      protocolVersionRepresentativeFor(protocolVersion),
    )(hashOps)

  private def createFromRepresentativePV(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      representativeProtocolVersion: RepresentativeProtocolVersion[TransactionView.type],
  ): Either[String, TransactionView] =
    Either
      .catchOnly[InvalidView](
        TransactionView.tryCreate(
          viewCommonData,
          viewParticipantData,
          subviews,
          representativeProtocolVersion,
        )(hashOps)
      )
      .leftMap(_.message)

  /** Creates a view.
    *
    * Yields `Left(...)` if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
    */
  def create(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      protocolVersion: ProtocolVersion,
  ): Either[String, TransactionView] =
    Either
      .catchOnly[InvalidView](
        TransactionView.tryCreate(hashOps)(
          viewCommonData,
          viewParticipantData,
          subviews,
          protocolVersion,
        )
      )
      .leftMap(_.message)

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  val viewCommonDataUnsafe: Lens[TransactionView, MerkleTree[ViewCommonData]] =
    GenLens[TransactionView](_.viewCommonData)

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  val viewParticipantDataUnsafe: Lens[TransactionView, MerkleTree[ViewParticipantData]] =
    GenLens[TransactionView](_.viewParticipantData)

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  val subviewsUnsafe: Lens[TransactionView, TransactionSubviews] =
    GenLens[TransactionView](_.subviews)

  private def fromProtoV0(
      context: (HashOps, ConfirmationPolicy),
      protoView: v0.ViewNode,
  ): ParsingResult[TransactionView] = {
    val (hashOps, _) = context
    for {
      commonData <- MerkleTree.fromProtoOptionV0(
        protoView.viewCommonData,
        ViewCommonData.fromByteString(context),
      )
      participantData <- MerkleTree.fromProtoOptionV0(
        protoView.viewParticipantData,
        ViewParticipantData.fromByteString(hashOps),
      )
      subViews <- TransactionSubviews.fromProtoV0(context, protoView.subviews)
      view <- createFromRepresentativePV(hashOps)(
        commonData,
        participantData,
        subViews,
        protocolVersionRepresentativeFor(ProtoVersion(0)),
      ).leftMap(e =>
        ProtoDeserializationError.OtherError(s"Unable to create transaction views: $e")
      )
    } yield view
  }

  private def fromProtoV1(
      context: (HashOps, ConfirmationPolicy),
      protoView: v1.ViewNode,
  ): ParsingResult[TransactionView] = {
    val (hashOps, _) = context
    for {
      commonData <- MerkleTree.fromProtoOptionV1(
        protoView.viewCommonData,
        ViewCommonData.fromByteString(context),
      )
      participantData <- MerkleTree.fromProtoOptionV1(
        protoView.viewParticipantData,
        ViewParticipantData.fromByteString(hashOps),
      )
      subViews <- TransactionSubviews.fromProtoV1(context, protoView.subviews)
      view <- createFromRepresentativePV(hashOps)(
        commonData,
        participantData,
        subViews,
        protocolVersionRepresentativeFor(ProtoVersion(1)),
      ).leftMap(e =>
        ProtoDeserializationError.OtherError(s"Unable to create transaction views: $e")
      )
    } yield view
  }

  /** Indicates an attempt to create an invalid view. */
  final case class InvalidView(message: String) extends RuntimeException(message)
}
