// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.*
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndexFromRoot
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{EitherUtil, MonadUtil}
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import monocle.Lens
import monocle.macros.GenLens

import scala.collection.mutable

/** Partially blinded version of a transaction tree.
  * This class is also used to represent transaction view trees and informee trees.
  */
// private constructor, because object invariants are checked by factory methods
final case class GenTransactionTree private (
    submitterMetadata: MerkleTree[SubmitterMetadata],
    commonMetadata: MerkleTree[CommonMetadata],
    participantMetadata: MerkleTree[ParticipantMetadata],
    rootViews: MerkleSeq[TransactionView],
)(hashOps: HashOps)
    extends MerkleTreeInnerNode[GenTransactionTree](hashOps) {

  def validated: Either[String, this.type] = for {
    _ <- checkUniqueHashes
    _ <- commonMetadata.unwrap.leftMap(_ => "commonMetadata is blinded")
  } yield this

  private def checkUniqueHashes: Either[String, this.type] = {
    // Check that every subtree has a unique root hash
    val usedHashes = mutable.Set[RootHash]()

    def go(tree: MerkleTree[_]): Either[String, this.type] = {
      val rootHash = tree.rootHash

      for {
        _ <- EitherUtil.condUnitE(
          !usedHashes.contains(rootHash),
          "A transaction tree must contain a hash at most once. " +
            s"Found the hash ${rootHash.toString} twice.",
        )
        _ = usedHashes.add(rootHash).discard
        _ <- MonadUtil.sequentialTraverse(tree.subtrees)(go)
      } yield this
    }

    go(this)
  }

  @VisibleForTesting
  // Private, because it does not check object invariants and is therefore unsafe.
  private[data] def copy(
      submitterMetadata: MerkleTree[SubmitterMetadata] = this.submitterMetadata,
      commonMetadata: MerkleTree[CommonMetadata] = this.commonMetadata,
      participantMetadata: MerkleTree[ParticipantMetadata] = this.participantMetadata,
      rootViews: MerkleSeq[TransactionView] = this.rootViews,
  ): GenTransactionTree =
    GenTransactionTree(submitterMetadata, commonMetadata, participantMetadata, rootViews)(hashOps)

  override def subtrees: Seq[MerkleTree[_]] =
    Seq[MerkleTree[_]](
      submitterMetadata,
      commonMetadata,
      participantMetadata,
    ) ++ rootViews.rootOrEmpty.toList

  override private[data] def withBlindedSubtrees(
      blindingCommandPerNode: PartialFunction[RootHash, BlindingCommand]
  ): MerkleTree[GenTransactionTree] =
    GenTransactionTree(
      submitterMetadata.doBlind(blindingCommandPerNode),
      commonMetadata.doBlind(blindingCommandPerNode),
      participantMetadata.doBlind(blindingCommandPerNode),
      rootViews.doBlind(blindingCommandPerNode),
    )(hashOps)

  /** Specialized blinding that addresses the case of blinding a Transaction Tree to obtain a
    * Transaction View Tree.
    * The view is identified by its position in the tree, specified by `viewPos`, directed from the root
    * to the leaf.
    * To ensure the path is valid, it should be obtained beforehand with a traversal method such as
    * [[TransactionView.allSubviewsWithPosition]]
    *
    * @param viewPos the position of the view from root to leaf
    * @throws java.lang.UnsupportedOperationException if the path does not lead to a view
    */
  private[data] def tryBlindForTransactionViewTree(
      viewPos: ViewPositionFromRoot
  ): GenTransactionTree = {
    viewPos.position match {
      case (head: MerkleSeqIndexFromRoot) +: tail =>
        val sm = if (viewPos.isTopLevel) submitterMetadata else submitterMetadata.blindFully
        val rv = rootViews.tryBlindAllButLeaf(
          head,
          _.tryBlindForTransactionViewTree(ViewPositionFromRoot(tail)),
        )
        GenTransactionTree(
          sm,
          commonMetadata,
          participantMetadata,
          rv,
        )(hashOps)
      case _ => throw new UnsupportedOperationException(s"Invalid view position: $viewPos")
    }
  }

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(rootHash)

  /** Yields the full informee tree corresponding to this transaction tree.
    * The resulting informee tree is full, only if every view common data is unblinded.
    */
  def tryFullInformeeTree(protocolVersion: ProtocolVersion): FullInformeeTree = {
    val tree = blind({
      case _: GenTransactionTree => RevealIfNeedBe
      case _: SubmitterMetadata => BlindSubtree
      case _: CommonMetadata => RevealSubtree
      case _: ParticipantMetadata => BlindSubtree
      case _: TransactionView => RevealIfNeedBe
      case _: ViewCommonData => RevealSubtree
      case _: ViewParticipantData => BlindSubtree
    }).tryUnwrap
    FullInformeeTree.tryCreate(tree, protocolVersion)
  }

  /** Finds the position of the view corresponding to the given hash.
    * Returns `None` if no such view exists in this tree.
    */
  def viewPosition(viewHash: RootHash): Option[ViewPosition] = {
    val pos = for {
      (rootView, index) <- rootViews.unblindedElementsWithIndex
      (view, viewPos) <- rootView.allSubviewsWithPosition(index +: ViewPosition.root)
      if view.rootHash == viewHash
    } yield viewPos

    pos.headOption
  }

  /** Yields the transaction view tree corresponding to a given view.
    * If some subtrees have already been blinded, they will remain blinded.
    *
    * @throws java.lang.IllegalArgumentException if there is no transaction view in this tree with `viewHash`
    */
  def transactionViewTree(viewHash: RootHash): FullTransactionViewTree =
    viewPosition(viewHash)
      .map(viewPos =>
        FullTransactionViewTree.tryCreate(tryBlindForTransactionViewTree(viewPos.reverse))
      )
      .getOrElse(
        throw new IllegalArgumentException(s"No transaction view found with hash $viewHash")
      )

  lazy val allTransactionViewTrees: Seq[FullTransactionViewTree] = for {
    (rootView, index) <- rootViews.unblindedElementsWithIndex
    (_view, viewPos) <- rootView.allSubviewsWithPosition(index +: ViewPosition.root)
    genTransactionTree = tryBlindForTransactionViewTree(viewPos.reverse)
  } yield FullTransactionViewTree.tryCreate(genTransactionTree)

  def allLightTransactionViewTrees(
  ): Seq[LightTransactionViewTree] =
    allTransactionViewTrees.map(LightTransactionViewTree.fromTransactionViewTree)

  /** All lightweight transaction trees in this [[GenTransactionTree]], accompanied by their witnesses and randomness
    * suitable for deriving encryption keys for encrypted view messages.
    *
    * The witnesses are useful for constructing the BCC-style recipient trees for the view messages.
    * The lightweight transaction + BCC scheme requires that, for every non top-level view,
    * the encryption key used to encrypt that view's lightweight transaction tree can be (deterministically) derived
    * from the randomness used for the parent view. This function returns suitable randomness that is derived using
    * a HKDF. For top-level views, the randomness is derived from the provided initial seed.
    *
    * All the returned random values have the same length as the provided initial seed. The caller should ensure that
    * the provided randomness is long enough to be used for the default HMAC implementation.
    */
  def allLightTransactionViewTreesWithWitnessesAndSeeds(
      initSeed: SecureRandomness,
      hkdfOps: HkdfOps,
  ): Either[HkdfError, Seq[(LightTransactionViewTree, Witnesses, SecureRandomness)]] = {
    val randomnessLength = initSeed.unwrap.size
    val witnessAndSeedMapE =
      allTransactionViewTrees.toList.foldLeftM(
        Map.empty[ViewPosition, (Witnesses, SecureRandomness)]
      ) { case (ws, tvt) =>
        val parentPosition = ViewPosition(tvt.viewPosition.position.drop(1))
        val (witnesses, parentSeed) = ws.get(parentPosition) match {
          case Some((parentWitnesses, parentSeed)) =>
            parentWitnesses.prepend(tvt.informees) -> parentSeed
          case None if (parentPosition.position.isEmpty) =>
            Witnesses(NonEmpty(Seq, tvt.informees)) -> initSeed
          case None =>
            throw new IllegalStateException(
              s"Can't find the parent witnesses for position ${tvt.viewPosition}"
            )
        }

        val viewIndex =
          tvt.viewPosition.position.headOption
            .getOrElse(throw new IllegalStateException("View with no position"))
        val seedE = hkdfOps.computeHkdf(
          parentSeed.unwrap,
          randomnessLength,
          HkdfInfo.subview(viewIndex),
        )
        seedE.map(seed => ws.updated(tvt.viewPosition, witnesses -> seed))
      }
    witnessAndSeedMapE.map { witnessAndSeedMap =>
      allTransactionViewTrees.map { tvt =>
        val (witnesses, seed) = witnessAndSeedMap(tvt.viewPosition)
        (LightTransactionViewTree.fromTransactionViewTree(tvt), witnesses, seed)
      }
    }
  }

  def toProtoV30: v30.GenTransactionTree =
    v30.GenTransactionTree(
      submitterMetadata = Some(MerkleTree.toBlindableNodeV30(submitterMetadata)),
      commonMetadata = Some(MerkleTree.toBlindableNodeV30(commonMetadata)),
      participantMetadata = Some(MerkleTree.toBlindableNodeV30(participantMetadata)),
      rootViews = Some(rootViews.toProtoV30),
    )

  def mapUnblindedRootViews(f: TransactionView => TransactionView): GenTransactionTree =
    this.copy(rootViews = rootViews.mapM(f))

  override def pretty: Pretty[GenTransactionTree] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("common metadata", _.commonMetadata),
    param("participant metadata", _.participantMetadata),
    param("roots", _.rootViews),
  )
}

object GenTransactionTree {

  /** @throws GenTransactionTree$.InvalidGenTransactionTree if two subtrees have the same root hash
    */
  def tryCreate(hashOps: HashOps)(
      submitterMetadata: MerkleTree[SubmitterMetadata],
      commonMetadata: MerkleTree[CommonMetadata],
      participantMetadata: MerkleTree[ParticipantMetadata],
      rootViews: MerkleSeq[TransactionView],
  ): GenTransactionTree =
    create(hashOps)(submitterMetadata, commonMetadata, participantMetadata, rootViews).valueOr(
      err => throw InvalidGenTransactionTree(err)
    )

  /** Creates a [[GenTransactionTree]].
    * Yields `Left(...)` if two subtrees have the same root hash.
    */
  def create(hashOps: HashOps)(
      submitterMetadata: MerkleTree[SubmitterMetadata],
      commonMetadata: MerkleTree[CommonMetadata],
      participantMetadata: MerkleTree[ParticipantMetadata],
      rootViews: MerkleSeq[TransactionView],
  ): Either[String, GenTransactionTree] =
    GenTransactionTree(submitterMetadata, commonMetadata, participantMetadata, rootViews)(
      hashOps
    ).validated

  /** Indicates an attempt to create an invalid [[GenTransactionTree]]. */
  final case class InvalidGenTransactionTree(message: String) extends RuntimeException(message) {}

  @VisibleForTesting
  val submitterMetadataUnsafe: Lens[GenTransactionTree, MerkleTree[SubmitterMetadata]] =
    GenLens[GenTransactionTree](_.submitterMetadata)

  @VisibleForTesting
  val rootViewsUnsafe: Lens[GenTransactionTree, MerkleSeq[TransactionView]] =
    GenLens[GenTransactionTree](_.rootViews)

  def fromProtoV30(
      context: (HashOps, ProtocolVersion),
      protoTransactionTree: v30.GenTransactionTree,
  ): ParsingResult[GenTransactionTree] = {
    val (hashOps, expectedProtocolVersion) = context
    for {
      submitterMetadata <- MerkleTree
        .fromProtoOptionV30(
          protoTransactionTree.submitterMetadata,
          SubmitterMetadata.fromByteString(expectedProtocolVersion)(hashOps),
        )
      commonMetadata <- MerkleTree
        .fromProtoOptionV30(
          protoTransactionTree.commonMetadata,
          CommonMetadata.fromByteString(expectedProtocolVersion)(hashOps),
        )
      commonMetadataUnblinded <- commonMetadata.unwrap.leftMap(_ =>
        InvariantViolation("GenTransactionTree.commonMetadata is blinded")
      )
      participantMetadata <- MerkleTree
        .fromProtoOptionV30(
          protoTransactionTree.participantMetadata,
          ParticipantMetadata.fromByteString(expectedProtocolVersion)(hashOps),
        )
      rootViewsP <- ProtoConverter
        .required("GenTransactionTree.rootViews", protoTransactionTree.rootViews)
      rootViews <- MerkleSeq.fromProtoV30(
        (
          hashOps,
          TransactionView.fromByteStringLegacy(ProtoVersion(1))(
            (hashOps, commonMetadataUnblinded.confirmationPolicy, expectedProtocolVersion)
          ),
        ),
        rootViewsP,
      )
      genTransactionTree <- createGenTransactionTreeV30(
        hashOps,
        submitterMetadata,
        commonMetadata,
        participantMetadata,
        rootViews,
      )
    } yield genTransactionTree
  }

  def createGenTransactionTreeV30(
      hashOps: HashOps,
      submitterMetadata: MerkleTree[SubmitterMetadata],
      commonMetadata: MerkleTree[CommonMetadata],
      participantMetadata: MerkleTree[ParticipantMetadata],
      rootViews: MerkleSeq[TransactionView],
  ): ParsingResult[GenTransactionTree] =
    GenTransactionTree
      .create(hashOps)(
        submitterMetadata,
        commonMetadata,
        participantMetadata,
        rootViews,
      )
      .leftMap(e => ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e"))
}
