// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.MerkleTree.BlindingCommand
import com.digitalasset.canton.data.ViewPosition.{MerklePathElement, MerkleSeqIndexFromRoot}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RootHash, ViewHash, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion}

/** Abstraction over the subviews of a [[TransactionView]]
  * Implementation of [[TransactionSubviews]] where the subviews are a merkle tree
  *
  * @param subviews transaction views wrapped in this class
  */
final case class TransactionSubviews private[data] (
    subviews: MerkleSeq[TransactionView]
) extends PrettyPrinting {
  def toProtoV1: v1.MerkleSeq = subviews.toProtoV1

  lazy val unblindedElementsWithIndex: Seq[(TransactionView, MerklePathElement)] =
    subviews.unblindedElementsWithIndex

  lazy val trees: Seq[MerkleTree[?]] = subviews.rootOrEmpty.toList

  def doBlind(policy: PartialFunction[RootHash, BlindingCommand]): TransactionSubviews =
    TransactionSubviews(subviews.doBlind(policy))

  def blindFully: TransactionSubviews =
    TransactionSubviews(subviews.blindFully)

  lazy val areFullyBlinded: Boolean = subviews.isFullyBlinded

  lazy val blindedElements: Seq[RootHash] = subviews.blindedElements

  /** Check that the provided subview hashes are consistent with the ones from the contained subviews. */
  def hashesConsistentWith(hashOps: HashOps)(subviewHashes: Seq[ViewHash]): Boolean = {
    val merkleSeqRepr = subviews.representativeProtocolVersion
    val merkleSeqElemRepr = subviews.tryMerkleSeqElementRepresentativeProtocolVersion

    val subviewsSeq = subviewHashes.map(h => BlindedNode(h.toRootHash))
    val subviewsToCheck = MerkleSeq.fromSeq(hashOps, merkleSeqRepr, merkleSeqElemRepr)(subviewsSeq)

    subviews.rootHashO == subviewsToCheck.rootHashO
  }

  def tryBlindForTransactionViewTree(
      viewPos: ViewPositionFromRoot
  ): TransactionSubviews = {
    viewPos.position match {
      case (head: MerkleSeqIndexFromRoot) +: tail =>
        TransactionSubviews(
          subviews.tryBlindAllButLeaf(
            head,
            _.tryBlindForTransactionViewTree(ViewPositionFromRoot(tail)),
          )
        )
      case other => throw new UnsupportedOperationException(s"Invalid path: $other")
    }
  }

  lazy val unblindedElements: Seq[TransactionView] = unblindedElementsWithIndex.map(_._1)

  /** Apply `f` to all the unblinded contained subviews */
  def mapUnblinded(f: TransactionView => TransactionView): TransactionSubviews = {
    TransactionSubviews(subviews.mapM(f))
  }

  def pretty: Pretty[TransactionSubviews.this.type] = prettyOfClass(
    unnamedParam(_.subviews)
  )

  /** Return the view hashes of the contained subviews
    *
    * @throws java.lang.IllegalStateException if applied to a [[TransactionSubviews]] with blinded elements
    */
  lazy val trySubviewHashes: Seq[ViewHash] = {
    if (blindedElements.isEmpty) unblindedElements.map(_.viewHash)
    else
      throw new IllegalStateException(
        "Attempting to get subviewHashes from a TransactionSubviewsV1 with blinded elements"
      )
  }

  /** Assert that all contained subviews are unblinded
    *
    * @throws java.lang.IllegalStateException if there are blinded subviews, passing the first blinded subview hash
    *                                         to the provided function to generate the error message
    */
  def assertAllUnblinded(makeMessage: RootHash => String): Unit =
    blindedElements.headOption.foreach(hash => throw new IllegalStateException(makeMessage(hash)))
}

object TransactionSubviews {
  private[data] def fromProtoV1(
      context: (HashOps, ConfirmationPolicy, ProtocolVersion),
      subviewsPO: Option[v1.MerkleSeq],
  ): ParsingResult[TransactionSubviews] = {
    val (hashOps, _, _) = context
    for {
      subviewsP <- ProtoConverter.required("ViewNode.subviews", subviewsPO)
      tvParser = TransactionView.fromByteStringLegacy(ProtoVersion(1))(context)
      subviews <- MerkleSeq.fromProtoV1((hashOps, tvParser), subviewsP)
    } yield TransactionSubviews(subviews)
  }

  def apply(
      subviewsSeq: Seq[MerkleTree[TransactionView]]
  )(protocolVersion: ProtocolVersion, hashOps: HashOps): TransactionSubviews =
    TransactionSubviews(MerkleSeq.fromSeq(hashOps, protocolVersion)(subviewsSeq))

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
  def indices(nbOfSubviews: Int): Seq[MerklePathElement] =
    MerkleSeq.indicesFromSeq(nbOfSubviews)
}
