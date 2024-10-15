// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription
import com.digitalasset.canton.data.TransactionView.{
  InvalidView,
  WithPath,
  validateViewCommonData,
  validateViewParticipantData,
}
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil, NamedLoggingLazyVal}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfVersioned, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import monocle.Lens
import monocle.macros.GenLens

/** A single view of a transaction, embedded in a Merkle tree.
  * Nodes of the Merkle tree may or may not be blinded.
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

  def subviewHashesConsistentWith(subviewHashes: Seq[ViewHash]): Boolean =
    subviews.hashesConsistentWith(hashOps)(subviewHashes)

  override def subtrees: Seq[MerkleTree[?]] =
    Seq[MerkleTree[?]](viewCommonData, viewParticipantData) ++ subviews.trees

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
    ): Seq[(TransactionView, ViewPosition)] =
      (view, viewPos) +: view.subviews.unblindedElementsWithIndex.flatMap {
        case (view, viewIndex) => helper(view, viewIndex +: viewPos)
      }

    helper(this, rootPos)
  }

  override protected def pretty: Pretty[TransactionView] = prettyOfClass(
    param("root hash", _.rootHash),
    param("view common data", _.viewCommonData),
    param("view participant data", _.viewParticipantData),
    param("subviews", _.subviews),
  )

  // This constructor is intended for monocle GenLens/test use where the intention is to bypass the validation
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

  private[data] def tryCopy(
      viewCommonData: MerkleTree[ViewCommonData] = this.viewCommonData,
      viewParticipantData: MerkleTree[ViewParticipantData] = this.viewParticipantData,
      subviews: TransactionSubviews = this.subviews,
  ): TransactionView =
    copy(viewCommonData, viewParticipantData, subviews).tryValidated()

  /** If the view with the given hash appears either as this view or one of its unblinded descendants,
    * replace it by the given view.
    * TODO(i12900): not stack safe unless we have limits on the depths of views.
    */
  def replace(h: ViewHash, v: TransactionView): TransactionView =
    if (viewHash == h) v
    else this.tryCopy(subviews = subviews.mapUnblinded(_.replace(h, v)))

  protected def toProtoV30: v30.ViewNode = v30.ViewNode(
    viewCommonData = Some(MerkleTree.toBlindableNodeV30(viewCommonData)),
    viewParticipantData = Some(MerkleTree.toBlindableNodeV30(viewParticipantData)),
    subviews = Some(subviews.toProtoV30),
  )

  /** The global key inputs that the [[com.digitalasset.daml.lf.transaction.ContractStateMachine]] computes
    * while interpreting the root action of the view, enriched with the maintainers of the key and the
    * [[com.digitalasset.canton.protocol.LfLanguageVersion]] to be used for serializing the key.
    *
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded
    */
  def globalKeyInputs(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]] =
    _globalKeyInputs.get

  private[this] val _globalKeyInputs
      : NamedLoggingLazyVal[Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]]] =
    NamedLoggingLazyVal[Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]]] {
      implicit loggingContext =>
        val viewParticipantData = tryUnblindViewParticipantData("Global key inputs")

        subviews.assertAllUnblinded(hash =>
          s"Global key inputs of view $viewHash can be computed only if all subviews are unblinded, but $hash is blinded"
        )

        subviews.unblindedElements.foldLeft(viewParticipantData.resolvedKeysWithMaintainers) {
          (acc, subview) =>
            val subviewGki = subview.globalKeyInputs
            MapsUtil.mergeWith(acc, subviewGki)((accRes, _subviewRes) => accRes)
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
      s"Inputs and created contracts of view $viewHash can be computed only if all subviews are unblinded, but $hash is blinded"
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
            throw InvalidView(
              s"Contract ${fromAcc.contract.contractId} is created multiple times in view $viewHash"
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

  def tryValidated(): TransactionView = validated.valueOr(e => throw InvalidView(e))

  def validated: Either[String, TransactionView] = {

    lazy val childParticipantData = subviews.unblindedElementsWithIndex.flatMap(t =>
      t._1.viewParticipantData.unwrap.toOption.toList.map(WithPath(t._2, _))
    )
    lazy val childCommonData = subviews.unblindedElementsWithIndex.flatMap(t =>
      t._1.viewCommonData.unwrap.toOption.toList.map(WithPath(t._2, _))
    )

    for {
      _ <- viewParticipantData.unwrap match {
        case Left(_) => Right(())
        case Right(d) => validateViewParticipantData(d, childParticipantData)
      }
      _ <- viewCommonData.unwrap match {
        case Left(_) => Right(())
        case Right(d) => validateViewCommonData(d, childCommonData)
      }
    } yield this
  }

}

object TransactionView
    extends HasProtocolVersionedWithContextCompanion[
      TransactionView,
      (HashOps, ProtocolVersion),
    ] {
  override def name: String = "TransactionView"
  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.ViewNode)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  private def tryCreate(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      representativeProtocolVersion: RepresentativeProtocolVersion[TransactionView.type],
  )(hashOps: HashOps): TransactionView =
    new TransactionView(viewCommonData, viewParticipantData, subviews)(
      hashOps,
      representativeProtocolVersion,
    ).tryValidated()

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

  private def fromProtoV30(
      context: (HashOps, ProtocolVersion),
      protoView: v30.ViewNode,
  ): ParsingResult[TransactionView] = {
    val (hashOps, expectedProtocolVersion) = context
    for {
      commonData <- MerkleTree.fromProtoOptionV30(
        protoView.viewCommonData,
        ViewCommonData.fromByteString(expectedProtocolVersion)(hashOps),
      )
      participantData <- MerkleTree.fromProtoOptionV30(
        protoView.viewParticipantData,
        ViewParticipantData.fromByteString(expectedProtocolVersion)(hashOps),
      )
      subViews <- TransactionSubviews.fromProtoV30(context, protoView.subviews)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      view <- createFromRepresentativePV(hashOps)(
        commonData,
        participantData,
        subViews,
        rpv,
      ).leftMap(e =>
        ProtoDeserializationError.OtherError(s"Unable to create transaction views: $e")
      )
    } yield view
  }

  final case class WithPath[X](path: MerklePathElement, value: X) {
    def map[Y](f: X => Y): WithPath[Y] = WithPath(path, f(value))
  }

  def validateViewParticipantData(
      parentData: ViewParticipantData,
      childData: Seq[WithPath[ViewParticipantData]],
  ): Either[String, Unit] = {
    def validateExercise(
        parentExercise: ExerciseActionDescription,
        childExercises: Seq[WithPath[ExerciseActionDescription]],
    ): Either[String, Unit] = {
      val parentPackages = parentExercise.packagePreference
      childExercises
        .map(_.map(_.packagePreference.removedAll(parentPackages).headOption))
        .collectFirst { case WithPath(p, Some(k)) =>
          s"Detected unexpected exercise package preference: $k at $p"
        }
        .toLeft(())
    }

    parentData.actionDescription match {
      case ead: ExerciseActionDescription =>
        validateExercise(
          ead,
          childData.map(_.map(_.actionDescription)).collect {
            case WithPath(p, e: ExerciseActionDescription) => WithPath(p, e)
          },
        )
      case _ => Right(())
    }

  }

  def validateViewCommonData(
      parentData: ViewCommonData,
      childData: Seq[WithPath[ViewCommonData]],
  ): Either[String, Unit] =
    childData
      .collectFirst {
        case d if d.value == parentData =>
          s"The subview with index ${d.path} has equal viewCommonData to a parent."
      }
      .toLeft(())

  /** Indicates an attempt to create an invalid view. */
  final case class InvalidView(message: String) extends RuntimeException(message)
}
