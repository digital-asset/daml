// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.GenTransactionTree.ViewWithWitnessesAndRecipients
import com.digitalasset.canton.data.MerkleTree.*
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndexFromRoot
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import monocle.Lens
import monocle.macros.GenLens

import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** A DAML transaction, decomposed into views (cf. ViewDecomposition) and embedded in a Merkle tree.
  * Merkle tree nodes may or may not be blinded.
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
        _ <- Either.cond(
          !usedHashes.contains(rootHash),
          (),
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
  ): GenTransactionTree =
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

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(rootHash)

  /** Yields the full informee tree corresponding to this transaction tree.
    * The resulting informee tree is full, only if every view common data is unblinded.
    */
  def tryFullInformeeTree(protocolVersion: ProtocolVersion): FullInformeeTree = {
    val tree = blind {
      case _: GenTransactionTree => RevealIfNeedBe
      case _: SubmitterMetadata => RevealSubtree
      case _: CommonMetadata => RevealSubtree
      case _: ParticipantMetadata => BlindSubtree
      case _: TransactionView => RevealIfNeedBe
      case _: ViewCommonData => RevealSubtree
      case _: ViewParticipantData => BlindSubtree
    }.tryUnwrap
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

  /** Returns subviews in pre-order so, in the end, the tree is in pre-order.
    */
  lazy val allTransactionViewTrees: Seq[FullTransactionViewTree] = for {
    (rootView, index) <- rootViews.unblindedElementsWithIndex
    (_view, viewPos) <- rootView.allSubviewsWithPosition(index +: ViewPosition.root)
    genTransactionTree = tryBlindForTransactionViewTree(viewPos.reverse)
  } yield FullTransactionViewTree.tryCreate(genTransactionTree)

  /** All lightweight transaction trees in this [[GenTransactionTree]], accompanied by their recipients tree
    * that will serve to group session keys to encrypt view messages.
    *
    * The lightweight transaction + BCC scheme requires that each parent view contains the randomness for all
    * its direct children, allowing it to derive the encryption key and subsequently decrypt those views.
    * By default, the randomness is reused if views share the same recipients tree, and this information is stored in a
    * temporary cache to be used in multiple transactions.
    *
    * The caller should ensure that the provided randomness is long enough to be used for the default HMAC
    * implementation.
    */
  def allTransactionViewTreesWithRecipients(topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, Witnesses.InvalidWitnesses, Seq[
    ViewWithWitnessesAndRecipients
  ]] = {
    /* We first gather all witnesses for a view and then derive its recipients tree
     * and that of its direct children.
     * Later on, if a view shares the same recipients tree, we can use the same randomness/key.
     */
    val witnessMap =
      allTransactionViewTrees.foldLeft(
        Map.empty[ViewPosition, Witnesses]
      ) { case (ws, tvt) =>
        val parentPosition = ViewPosition(tvt.viewPosition.position.drop(1))
        val witnesses = ws.get(parentPosition) match {
          case Some(parentWitnesses) =>
            parentWitnesses.prepend(tvt.informees)
          case None if parentPosition.position.isEmpty =>
            Witnesses(NonEmpty(Seq, tvt.informees))
          case None =>
            throw new IllegalStateException(
              s"Can't find the parent witnesses for position ${tvt.viewPosition}"
            )
        }
        ws.updated(tvt.viewPosition, witnesses)
      }

    for {
      allViewsWithMetadata <- allTransactionViewTrees.parTraverse { tvt =>
        val viewWitnesses = witnessMap(tvt.viewPosition)
        val parentPosition = ViewPosition(tvt.viewPosition.position.drop(1))
        val parentWitnessesO = witnessMap.get(parentPosition)

        // We return the witnesses for testing purposes. We will use the recipients to derive our view encryption key.
        for {
          viewRecipients <- viewWitnesses
            .toRecipients(topologySnapshot)
          parentRecipients <- parentWitnessesO
            .traverse(_.toRecipients(topologySnapshot))
        } yield ViewWithWitnessesAndRecipients(tvt, viewWitnesses, viewRecipients, parentRecipients)
      }
    } yield allViewsWithMetadata
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

  override protected def pretty: Pretty[GenTransactionTree] = prettyOfClass(
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
        InvariantViolation(field = "GenTransactionTree.commonMetadata", error = "is blinded")
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
          (
            hashOps,
            TransactionView.fromByteString(expectedProtocolVersion)(
              (hashOps, expectedProtocolVersion)
            ),
          ),
          expectedProtocolVersion,
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

  final case class ViewWithWitnessesAndRecipients(
      view: FullTransactionViewTree,
      witnesses: Witnesses,
      recipients: Recipients,
      parentRecipients: Option[Recipients],
  )
}
