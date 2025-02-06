// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import monocle.PLens

import scala.annotation.tailrec
import scala.collection.mutable

/** Wraps a `GenTransactionTree` where exactly one view is unblinded.
  * The direct subviews of the unblinded view are blinded - this is why the class name is prefixed "Light".
  *
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  *
  * @throws LightTransactionViewTree$.InvalidLightTransactionViewTree if [[tree]] is not a light transaction view tree
  *                                    (i.e. the wrong set of nodes is blinded)
  */
sealed abstract case class LightTransactionViewTree private[data] (
    tree: GenTransactionTree,
    subviewHashesAndKeys: Seq[ViewHashAndKey],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      LightTransactionViewTree.type
    ]
) extends TransactionViewTree
    with HasProtocolVersionedWrapper[LightTransactionViewTree]
    with PrettyPrinting {

  override val subviewHashes: Seq[ViewHash] = subviewHashesAndKeys.map {
    case ViewHashAndKey(viewHash, _) => viewHash
  }

  @tailrec
  private[data] override def findTheView(
      viewsWithIndex: Seq[(TransactionView, MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): Either[String, (TransactionView, ViewPosition)] =
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

  override def validated: Either[String, this.type] = for {
    _ <- super[TransactionViewTree].validated
    // Check that the subview hashes are consistent with the tree
    _ <- Either.cond(
      view.subviewHashesConsistentWith(subviewHashes),
      (),
      s"The provided subview hashes are inconsistent with the provided view (view: ${view.viewHash} " +
        s"at position: $viewPosition, subview hashes: $subviewHashes)",
    )
  } yield this

  @transient override protected lazy val companionObj: LightTransactionViewTree.type =
    LightTransactionViewTree

  def toProtoV30: v30.LightTransactionViewTree =
    v30.LightTransactionViewTree(
      tree = Some(tree.toProtoV30),
      subviewHashesAndKeys = subviewHashesAndKeys.map { case ViewHashAndKey(viewHash, key) =>
        v30.ViewHashAndKey(viewHash.toProtoPrimitive, key.getCryptographicEvidence)
      },
    )

  override lazy val pretty: Pretty[LightTransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

object LightTransactionViewTree
    extends VersioningCompanionContextPVValidation2[
      LightTransactionViewTree,
      (HashOps, Int),
    ] {
  override val name: String = "LightTransactionViewTree"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.LightTransactionViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30,
    )
  )

  final case class InvalidLightTransactionViewTree(message: String)
      extends RuntimeException(message)

  /** @throws InvalidLightTransactionViewTree if the tree is not a legal lightweight transaction view tree
    */
  def tryCreate(
      tree: GenTransactionTree,
      subviewHashesAndKeys: Seq[ViewHashAndKey],
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree =
    create(tree, subviewHashesAndKeys, protocolVersionRepresentativeFor(protocolVersion)).valueOr(
      err => throw InvalidLightTransactionViewTree(err)
    )

  def create(
      tree: GenTransactionTree,
      subviewHashesAndKeys: Seq[ViewHashAndKey],
      representativeProtocolVersion: RepresentativeProtocolVersion[LightTransactionViewTree.type],
  ): Either[String, LightTransactionViewTree] =
    new LightTransactionViewTree(tree, subviewHashesAndKeys)(
      representativeProtocolVersion
    ) {}.validated

  private def fromProtoV30(context: ((HashOps, Int), ProtocolVersion))(
      protoT: v30.LightTransactionViewTree
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      ((hashOps, expectedLength), protocolVersion) = context
      tree <- GenTransactionTree.fromProtoV30((hashOps, protocolVersion), protoTree)
      subviewHashesAndKeys <- protoT.subviewHashesAndKeys.traverse {
        case v30.ViewHashAndKey(viewHashT, keyT) =>
          for {
            viewHash <- ViewHash.fromProtoPrimitive(viewHashT)
            key <- SecureRandomness
              .fromByteString(expectedLength)(keyT)
              .leftMap[ProtoDeserializationError](
                ProtoDeserializationError.CryptoDeserializationError.apply
              )
          } yield ViewHashAndKey(viewHash, key)
      }
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      result <- LightTransactionViewTree
        .create(tree, subviewHashesAndKeys, rpv)
        .leftMap(e =>
          ProtoDeserializationError
            .InvariantViolation("tree", s"Unable to create transaction tree: $e")
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
        val fullView = lightViewTree.view.tryCopy(subviews = fullSubviews)
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
      subviewKeys: Seq[SecureRandomness],
      protocolVersion: ProtocolVersion,
  ): Either[String, LightTransactionViewTree] = {
    val withBlindedSubviews = tvt.view.tryCopy(subviews = tvt.view.subviews.blindFully)
    val genTransactionTree =
      tvt.tree.mapUnblindedRootViews(_.replace(tvt.viewHash, withBlindedSubviews))
    // you must have one key for each subview (to be able to decrypt them)
    Either.cond(
      subviewKeys.sizeIs == tvt.subviewHashes.size,
      // By definition, the view in a TransactionViewTree has all subviews unblinded
      LightTransactionViewTree.tryCreate(
        genTransactionTree,
        tvt.subviewHashes.lazyZip(subviewKeys).map { case (viewHash, key) =>
          ViewHashAndKey(viewHash, key)
        },
        protocolVersion,
      ),
      s"Expected ${tvt.subviewHashes.size} subview keys, but got ${subviewKeys.size}",
    )
  }

}

/** A view hash and its corresponding encryption key. */
final case class ViewHashAndKey(viewHash: ViewHash, viewEncryptionKeyRandomness: SecureRandomness)
