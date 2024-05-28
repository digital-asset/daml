// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.*
import monocle.PLens

import scala.annotation.tailrec
import scala.collection.mutable

/** Wraps a `GenTransactionTree` where exactly one view (not including subviews) is unblinded.
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  *
  * @throws LightTransactionViewTree$.InvalidLightTransactionViewTree if [[tree]] is not a light transaction view tree
  *                                    (i.e. the wrong set of nodes is blinded)
  */
sealed abstract class LightTransactionViewTree private[data] (
    val tree: GenTransactionTree
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      LightTransactionViewTree.type
    ]
) extends TransactionViewTree
    with HasProtocolVersionedWrapper[LightTransactionViewTree]
    with PrettyPrinting {

  @tailrec
  private[data] override def findTheView(
      viewsWithIndex: Seq[(TransactionView, MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): Either[String, (TransactionView, ViewPosition)] = {
    viewsWithIndex match {
      case Seq() =>
        Left("A light transaction view tree must contain an unblinded view.")
      case Seq((singleView, index)) if singleView.hasAllLeavesBlinded =>
        findTheView(singleView.subviews.unblindedElementsWithIndex, index +: viewPosition)
      case Seq((singleView, index))
          if singleView.viewCommonData.isFullyUnblinded && singleView.viewParticipantData.isFullyUnblinded && singleView.subviews.areFullyBlinded =>
        Right((singleView, index +: viewPosition))
      case Seq((singleView, _index)) =>
        Left(s"Invalid blinding in a light transaction view tree: $singleView")
      case multipleViews =>
        Left(
          s"A transaction view tree must not contain several (partially) unblinded views: " +
            s"${multipleViews.map(_._1)}"
        )
    }
  }

  override def validated: Either[String, this.type] = for {

    _ <- super[TransactionViewTree].validated

    // Check that the subview hashes are consistent with the tree
    _ <- EitherUtil.condUnitE(
      view.subviewHashesConsistentWith(subviewHashes),
      s"The provided subview hashes are inconsistent with the provided view (view: ${view.viewHash} " +
        s"at position: $viewPosition, subview hashes: $subviewHashes)",
    )

  } yield this

  @transient override protected lazy val companionObj: LightTransactionViewTree.type =
    LightTransactionViewTree

  def toProtoV0: v0.LightTransactionViewTree =
    v0.LightTransactionViewTree(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.LightTransactionViewTree =
    v1.LightTransactionViewTree(
      tree = Some(tree.toProtoV1),
      subviewHashes = subviewHashes.map(_.toProtoPrimitive),
    )

  override lazy val pretty: Pretty[LightTransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

/** Specialization of `LightTransactionViewTree` when subviews are a sequence of `TransactionViews`.
  * The hashes of all the direct subviews are computed directly from the sequence of subviews.
  */
private final case class LightTransactionViewTreeV0 private[data] (
    override val tree: GenTransactionTree
)(rpv: RepresentativeProtocolVersion[LightTransactionViewTree.type])
    extends LightTransactionViewTree(tree)(rpv) {
  override val subviewHashes: Seq[ViewHash] = view.subviews.trySubviewHashes
}

/** Specialization of `LightTransactionViewTree` when subviews are a MerkleSeq of `TransactionViews`.
  * In this case, the subview hashes need to be provided because the extra blinding provided by MerkleSeqs
  * could hide some of the subviews.
  *
  * @param subviewHashes: view hashes of all direct subviews
  */
private final case class LightTransactionViewTreeV1 private[data] (
    override val tree: GenTransactionTree,
    override val subviewHashes: Seq[ViewHash],
)(rpv: RepresentativeProtocolVersion[LightTransactionViewTree.type])
    extends LightTransactionViewTree(tree)(rpv)

object LightTransactionViewTree
    extends HasProtocolVersionedWithContextAndValidationCompanion[
      LightTransactionViewTree,
      HashOps,
    ] {
  override val name: String = "LightTransactionViewTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.LightTransactionViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV0(context)(proto)),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.LightTransactionViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV1(context)(proto)),
      _.toProtoV1.toByteString,
    ),
  )

  final case class InvalidLightTransactionViewTree(message: String)
      extends RuntimeException(message)

  /** @throws InvalidLightTransactionViewTree if the tree is not a legal lightweight transaction view tree
    */
  def tryCreate(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree =
    create(tree, subviewHashes, protocolVersionRepresentativeFor(protocolVersion)).valueOr(err =>
      throw InvalidLightTransactionViewTree(err)
    )

  def create(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
      rpv: RepresentativeProtocolVersion[LightTransactionViewTree.type],
  ): Either[String, LightTransactionViewTree] = createV1(tree, subviewHashes, rpv)

  private def createV0(
      tree: GenTransactionTree,
      rpv: RepresentativeProtocolVersion[LightTransactionViewTree.type],
  ): Either[String, LightTransactionViewTree] =
    LightTransactionViewTreeV0(tree)(rpv).validated

  private def createV1(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
      rpv: RepresentativeProtocolVersion[LightTransactionViewTree.type],
  ): Either[String, LightTransactionViewTree] =
    LightTransactionViewTreeV1(tree, subviewHashes)(rpv).validated

  private def fromProtoV0(context: (HashOps, ProtocolVersion))(
      protoT: v0.LightTransactionViewTree
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      tree <- GenTransactionTree.fromProtoV0(context, protoTree)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(0))
      result <- LightTransactionViewTree
        .createV0(tree, rpv)
        .leftMap(e =>
          ProtoDeserializationError.InvariantViolation(s"Unable to create transaction tree: $e")
        )
    } yield result

  private def fromProtoV1(context: (HashOps, ProtocolVersion))(
      protoT: v1.LightTransactionViewTree
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      tree <- GenTransactionTree.fromProtoV1(context, protoTree)
      subviewHashes <- protoT.subviewHashes.traverse(ViewHash.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(1))
      result <- LightTransactionViewTree
        .createV1(tree, subviewHashes, rpv)
        .leftMap(e =>
          ProtoDeserializationError.InvariantViolation(s"Unable to create transaction tree: $e")
        )
    } yield result

  /** Converts a sequence of light transaction view trees to the corresponding full view trees.
    * A light transaction view tree can be converted to its corresponding full view tree if and only if
    * all descendants can be converted.
    *
    * To make the method more generic, light view trees are represented as `A` and full view trees as `B` and the
    * `lens` parameter is used to convert between these types, as needed.
    *
    * @param topLevelOnly whether to return only top-level full view trees
    * @param lightViewTrees the light transaction view trees to convert
    * @return A triple consisting of (1) the full view trees that could be converted,
    *         (2) the light view trees that could not be converted due to missing descendants, and
    *         (3) duplicate light view trees in the input.
    *         The view trees in the output are sorted by view position, i.e., in pre-order.
    *         If the input contains the same view several times, then
    *         the output (1) contains one occurrence and the output (3) every other occurrence of the view.
    */
  def toFullViewTrees[A, B](
      lens: PLens[A, B, LightTransactionViewTree, FullTransactionViewTree],
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
      topLevelOnly: Boolean,
  )(
      lightViewTrees: Seq[A]
  ): (Seq[B], Seq[A], Seq[A]) = {

    val lightViewTreesBoxedInPostOrder = lightViewTrees
      .sortBy(lens.get(_).viewPosition)(ViewPosition.orderViewPosition.toOrdering)
      .reverse

    // All reconstructed full views
    val fullViewByHash = mutable.Map.empty[ViewHash, TransactionView]
    // All reconstructed full view trees, boxed, paired with their view hashes.
    val allFullViewTreesInPreorderB = mutable.ListBuffer.empty[(ViewHash, B)]
    // All light view trees, boxed, that could not be reconstructed to full view trees, due to missing descendants
    val invalidLightViewTreesB = Seq.newBuilder[A]
    // All duplicate light view trees, boxed.
    val duplicateLightViewTreesB = Seq.newBuilder[A]
    // All hashes of non-toplevel full view trees that could be reconstructed
    val subviewHashesB = Set.newBuilder[ViewHash]

    for (lightViewTreeBoxed <- lightViewTreesBoxedInPostOrder) {
      val lightViewTree = lens.get(lightViewTreeBoxed)
      val subviewHashes = lightViewTree.subviewHashes.toSet
      val missingSubviews = subviewHashes -- fullViewByHash.keys

      if (missingSubviews.isEmpty) {
        val fullSubviewsSeq = lightViewTree.subviewHashes.map(fullViewByHash)
        val fullSubviews = TransactionSubviews(fullSubviewsSeq)(protocolVersion, hashOps)
        val fullView = lightViewTree.view.copy(subviews = fullSubviews)
        val fullViewTree = FullTransactionViewTree.tryCreate(
          lightViewTree.tree.mapUnblindedRootViews(_.replace(fullView.viewHash, fullView))
        )
        val fullViewTreeBoxed = lens.replace(fullViewTree)(lightViewTreeBoxed)

        if (topLevelOnly)
          subviewHashesB ++= subviewHashes
        if (fullViewByHash.contains(fullViewTree.viewHash)) {
          // Deduplicate views
          duplicateLightViewTreesB += lightViewTreeBoxed
        } else {
          (fullViewTree.viewHash -> fullViewTreeBoxed) +=: allFullViewTreesInPreorderB
          fullViewByHash += fullView.viewHash -> fullView
        }
      } else {
        invalidLightViewTreesB += lightViewTreeBoxed
      }
    }

    val allSubviewHashes = subviewHashesB.result()
    val allFullViewTreesInPreorder =
      allFullViewTreesInPreorderB
        .result()
        .collect {
          case (viewHash, fullViewTreeBoxed)
              if !topLevelOnly || !allSubviewHashes.contains(viewHash) =>
            fullViewTreeBoxed
        }

    (
      allFullViewTreesInPreorder,
      invalidLightViewTreesB.result().reverse,
      duplicateLightViewTreesB.result().reverse,
    )
  }

  /** Turns a full transaction view tree into a lightweight one. Not stack-safe. */
  def fromTransactionViewTree(
      tvt: FullTransactionViewTree,
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree = {
    val withBlindedSubviews = tvt.view.copy(subviews = tvt.view.subviews.blindFully)
    val genTransactionTree =
      tvt.tree.mapUnblindedRootViews(_.replace(tvt.viewHash, withBlindedSubviews))
    // By definition, the view in a TransactionViewTree has all subviews unblinded
    LightTransactionViewTree.tryCreate(genTransactionTree, tvt.subviewHashes, protocolVersion)
  }

}
