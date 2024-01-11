// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.MerkleTree.BlindingCommand
import com.digitalasset.canton.data.ViewPosition.{
  ListIndex,
  MerklePathElement,
  MerkleSeqIndexFromRoot,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RootHash, ViewHash, v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion}

/** Abstraction over the subviews of a [[TransactionView]]
  */
sealed trait TransactionSubviews extends Product with PrettyPrinting {
  def toProtoV0: Seq[v0.BlindableNode]

  def toProtoV1: v1.MerkleSeq

  val unblindedElementsWithIndex: Seq[(TransactionView, MerklePathElement)]

  val trees: Seq[MerkleTree[?]]

  def doBlind(policy: PartialFunction[RootHash, BlindingCommand]): TransactionSubviews

  def blindFully: TransactionSubviews

  val areFullyBlinded: Boolean

  val blindedElements: Seq[RootHash]

  /** Check that the provided subview hashes are consistent with the ones from the contained subviews. */
  def hashesConsistentWith(hashOps: HashOps)(subviewHashes: Seq[ViewHash]): Boolean

  private[data] def tryBlindForTransactionViewTree(
      viewPos: ViewPositionFromRoot
  ): TransactionSubviews

  lazy val unblindedElements: Seq[TransactionView] = unblindedElementsWithIndex.map(_._1)

  /** Apply `f` to all the unblinded contained subviews */
  def mapUnblinded(f: TransactionView => TransactionView): TransactionSubviews

  /** Return the view hashes of the contained subviews
    *
    * @throws java.lang.IllegalStateException if applied to a [[TransactionSubviewsV1]] with blinded elements
    */
  val trySubviewHashes: Seq[ViewHash]

  /** Assert that all contained subviews are unblinded
    *
    * @throws java.lang.IllegalStateException if there are blinded subviews, passing the first blinded subview hash
    *                                         to the provided function to generate the error message
    */
  def assertAllUnblinded(makeMessage: RootHash => String): Unit =
    blindedElements.headOption.foreach(hash => throw new IllegalStateException(makeMessage(hash)))
}

/** Implementation of [[TransactionSubviews]] where the subviews are a sequence
  *
  * @param subviews transaction views wrapped in this class
  */
final case class TransactionSubviewsV0 private[data] (
    subviews: Seq[MerkleTree[TransactionView]]
) extends TransactionSubviews {
  override def toProtoV0: Seq[v0.BlindableNode] = subviews.map(MerkleTree.toBlindableNodeV0(_))

  def toProtoV1: v1.MerkleSeq =
    throw new IllegalStateException("Attempting to serialize TransactionSubviewsV0 to proto v1")

  override lazy val unblindedElementsWithIndex: Seq[(TransactionView, MerklePathElement)] =
    subviewsWithIndex.collect { case (v: TransactionView, index) =>
      (v, index)
    }

  override lazy val trees: Seq[MerkleTree[?]] = subviews

  override def doBlind(policy: PartialFunction[RootHash, BlindingCommand]): TransactionSubviews =
    TransactionSubviewsV0(subviews.map(_.doBlind(policy)))

  override def blindFully: TransactionSubviews =
    TransactionSubviewsV0(subviews.map(_.blindFully))

  override lazy val areFullyBlinded: Boolean = subviews.forall(_.isBlinded)

  override lazy val blindedElements: Seq[RootHash] =
    subviews.flatMap(_.unwrap.left.toOption.toList)

  override def hashesConsistentWith(hashOps: HashOps)(subviewHashes: Seq[ViewHash]): Boolean =
    subviews.map(sv => ViewHash.fromRootHash(sv.rootHash)) == subviewHashes

  override private[data] def tryBlindForTransactionViewTree(
      viewPos: ViewPositionFromRoot
  ): TransactionSubviews = {
    viewPos match {
      case ViewPositionFromRoot((index: ListIndex) +: tail) =>
        if (index.index >= subviews.size)
          throw new UnsupportedOperationException(
            s"Invalid path: index $index does not exist (${subviews.size} subviews)"
          )

        val newSubviews = subviewsWithIndex.map {
          case (sv, i) if i == index =>
            sv.tryUnwrap.tryBlindForTransactionViewTree(ViewPositionFromRoot(tail))
          case (sv, _i) => sv.blindFully
        }
        TransactionSubviewsV0(newSubviews)
      case other =>
        throw new UnsupportedOperationException(s"Invalid path: $other")
    }
  }

  override def mapUnblinded(f: TransactionView => TransactionView): TransactionSubviews =
    TransactionSubviewsV0(subviews.map {
      case v: TransactionView => f(v)
      case v => v
    })

  override def pretty: Pretty[TransactionSubviewsV0.this.type] = prettyOfClass(
    unnamedParam(_.subviews)
  )

  override lazy val trySubviewHashes: Seq[ViewHash] =
    subviews.map(sv => ViewHash.fromRootHash(sv.rootHash))

  private lazy val subviewsWithIndex: Seq[(MerkleTree[TransactionView], ListIndex)] =
    subviews.zip(TransactionSubviews.indicesV0(subviews.size))
}

/** Implementation of [[TransactionSubviews]] where the subviews are a merkle tree
  *
  * @param subviews transaction views wrapped in this class
  */
final case class TransactionSubviewsV1 private[data] (
    subviews: MerkleSeq[TransactionView]
) extends TransactionSubviews {
  override def toProtoV0: Seq[v0.BlindableNode] =
    throw new IllegalStateException(
      "Attempting to serialize TransactionSubviewsV1 to proto v0"
    )

  override def toProtoV1: v1.MerkleSeq = subviews.toProtoV1

  override lazy val unblindedElementsWithIndex: Seq[(TransactionView, MerklePathElement)] =
    subviews.unblindedElementsWithIndex

  override lazy val trees: Seq[MerkleTree[?]] = subviews.rootOrEmpty.toList

  override def doBlind(policy: PartialFunction[RootHash, BlindingCommand]): TransactionSubviews =
    TransactionSubviewsV1(subviews.doBlind(policy))

  override def blindFully: TransactionSubviews =
    TransactionSubviewsV1(subviews.blindFully)

  override lazy val areFullyBlinded: Boolean = subviews.isFullyBlinded

  override lazy val blindedElements: Seq[RootHash] = subviews.blindedElements

  override def hashesConsistentWith(hashOps: HashOps)(subviewHashes: Seq[ViewHash]): Boolean = {
    val merkleSeqRepr = subviews.representativeProtocolVersion
    val merkleSeqElemRepr = subviews.tryMerkleSeqElementRepresentativeProtocolVersion

    val subviewsSeq = subviewHashes.map(h => BlindedNode(h.toRootHash))
    val subviewsToCheck = MerkleSeq.fromSeq(hashOps, merkleSeqRepr, merkleSeqElemRepr)(subviewsSeq)

    subviews.rootHashO == subviewsToCheck.rootHashO
  }

  override def tryBlindForTransactionViewTree(
      viewPos: ViewPositionFromRoot
  ): TransactionSubviews = {
    viewPos.position match {
      case (head: MerkleSeqIndexFromRoot) +: tail =>
        TransactionSubviewsV1(
          subviews.tryBlindAllButLeaf(
            head,
            _.tryBlindForTransactionViewTree(ViewPositionFromRoot(tail)),
          )
        )
      case other => throw new UnsupportedOperationException(s"Invalid path: $other")
    }
  }

  override def mapUnblinded(f: TransactionView => TransactionView): TransactionSubviews = {
    TransactionSubviewsV1(subviews.mapM(f))
  }

  override def pretty: Pretty[TransactionSubviewsV1.this.type] = prettyOfClass(
    unnamedParam(_.subviews)
  )

  override lazy val trySubviewHashes: Seq[ViewHash] = {
    if (blindedElements.isEmpty) unblindedElements.map(_.viewHash)
    else
      throw new IllegalStateException(
        "Attempting to get subviewHashes from a TransactionSubviewsV1 with blinded elements"
      )
  }
}

object TransactionSubviews {
  private[data] def fromProtoV0(
      context: (HashOps, ConfirmationPolicy),
      subviewsP: Seq[v0.BlindableNode],
  ): ParsingResult[TransactionSubviewsV0] =
    for {
      subviews <- subviewsP.traverse(subviewP =>
        MerkleTree.fromProtoOptionV0(
          Some(subviewP),
          TransactionView.fromByteStringLegacy(ProtoVersion(0))(context),
        )
      )
    } yield TransactionSubviewsV0(subviews)

  private[data] def fromProtoV1(
      context: (HashOps, ConfirmationPolicy),
      subviewsPO: Option[v1.MerkleSeq],
  ): ParsingResult[TransactionSubviewsV1] = {
    val (hashOps, _) = context
    for {
      subviewsP <- ProtoConverter.required("ViewNode.subviews", subviewsPO)
      tvParser = TransactionView.fromByteStringLegacy(ProtoVersion(1))(context)
      subviews <- MerkleSeq.fromProtoV1((hashOps, tvParser), subviewsP)
    } yield TransactionSubviewsV1(subviews)
  }

  def apply(
      subviewsSeq: Seq[MerkleTree[TransactionView]]
  )(protocolVersion: ProtocolVersion, hashOps: HashOps): TransactionSubviews = {
    if (protocolVersion >= ProtocolVersion.v4)
      TransactionSubviewsV1(MerkleSeq.fromSeq(hashOps, protocolVersion)(subviewsSeq))
    else
      TransactionSubviewsV0(subviewsSeq)
  }

  def empty(protocolVersion: ProtocolVersion, hashOps: HashOps): TransactionSubviews =
    apply(Seq.empty)(protocolVersion, hashOps)

  /** Produce a sequence of indices for subviews.
    * When subviews are stored in a sequence, it is essentially (0, ..., size - 1).
    * When subviews are stored in a merkle tree, it gives the view paths in the tree. For example, a
    * balanced tree with 4 subviews will produce (LL, LR, RL, RR).
    *
    * @param nbOfSubviews total number of subviews
    * @return the sequence of indices for the subviews
    */
  def indices(protocolVersion: ProtocolVersion, nbOfSubviews: Int): Seq[MerklePathElement] =
    if (protocolVersion >= ProtocolVersion.v4)
      MerkleSeq.indicesFromSeq(nbOfSubviews)
    else indicesV0(nbOfSubviews)

  private[data] def indicesV0(nbOfSubviews: Int): Seq[ListIndex] =
    (0 until nbOfSubviews).map(ListIndex)
}
