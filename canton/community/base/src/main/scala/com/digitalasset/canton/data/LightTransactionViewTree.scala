// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.LightTransactionViewTree.InvalidLightTransactionViewTree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import monocle.PLens

import java.util.UUID
import scala.collection.mutable

/** Wraps a `GenTransactionTree` where exactly one view (not including subviews) is unblinded.
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  * [[view]] points to the unblinded view in [[tree]].
  *
  * @throws LightTransactionViewTree$.InvalidLightTransactionViewTree if [[tree]] is not a light transaction view tree
  *                                    (i.e. the wrong set of nodes is blinded)
  */
sealed abstract class LightTransactionViewTree private[data] (
    val tree: GenTransactionTree,
    val view: TransactionView,
) extends ViewTree
    with HasVersionedWrapper[LightTransactionViewTree]
    with PrettyPrinting {
  val subviewHashes: Seq[ViewHash]

  override def viewPosition: ViewPosition =
    tree
      .viewPosition(view.rootHash)
      .getOrElse(throw new IllegalStateException("View not found in tree."))

  override protected def companionObj = LightTransactionViewTree

  // Private, because it does not check object invariants and is therefore unsafe.
  private[data] def copy(
      tree: GenTransactionTree,
      view: TransactionView,
      subviewHashes: Seq[ViewHash],
  ): LightTransactionViewTree =
    this match {
      case _: LightTransactionViewTreeV0 => LightTransactionViewTreeV0(tree, view)
      case _: LightTransactionViewTreeV1 => LightTransactionViewTreeV1(tree, view, subviewHashes)
    }

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  lazy val transactionUuid: UUID = checked(tree.commonMetadata.tryUnwrap).uuid

  override val viewHash: ViewHash = ViewHash.fromRootHash(view.rootHash)

  private[this] val commonMetadata: CommonMetadata =
    tree.commonMetadata.unwrap
      .getOrElse(
        throw InvalidLightTransactionViewTree(
          s"The common metadata of a light transaction view tree must be unblinded."
        )
      )

  override def domainId: DomainId = commonMetadata.domainId

  override def mediator: MediatorRef = commonMetadata.mediator

  lazy val confirmationPolicy: ConfirmationPolicy = commonMetadata.confirmationPolicy

  private val unwrapParticipantMetadata: ParticipantMetadata =
    tree.participantMetadata.unwrap
      .getOrElse(
        throw InvalidLightTransactionViewTree(
          s"The participant metadata of a light transaction view tree must be unblinded."
        )
      )

  val ledgerTime: CantonTimestamp = unwrapParticipantMetadata.ledgerTime

  val submissionTime: CantonTimestamp = unwrapParticipantMetadata.submissionTime

  val workflowIdO: Option[WorkflowId] = unwrapParticipantMetadata.workflowIdO

  override lazy val informees: Set[Informee] = view.viewCommonData.tryUnwrap.informees

  lazy val viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap

  def toProtoV0: v0.LightTransactionViewTree =
    v0.LightTransactionViewTree(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.LightTransactionViewTree =
    v1.LightTransactionViewTree(
      tree = Some(tree.toProtoV1),
      subviewHashes = subviewHashes.map(_.toProtoPrimitive),
    )

  override lazy val toBeSigned: Option[RootHash] =
    tree.rootViews.unblindedElements
      .find(_.rootHash == view.rootHash)
      .map(_ => transactionId.toRootHash)

  override def rootHash: RootHash = tree.rootHash

  override lazy val pretty: Pretty[LightTransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

/** Specialization of `LightTransactionViewTree` when subviews are a sequence of `TransactionViews`.
  * The hashes of all the direct subviews are computed directly from the sequence of subviews.
  */
private final case class LightTransactionViewTreeV0 private[data] (
    override val tree: GenTransactionTree,
    override val view: TransactionView,
) extends LightTransactionViewTree(tree, view) {
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
    override val view: TransactionView,
    override val subviewHashes: Seq[ViewHash],
) extends LightTransactionViewTree(tree, view) {
  def validated: Either[String, this.type] = {
    // Check that the subview hashes are consistent with the tree
    val viewPos = tree.viewPosition(view.rootHash).map(_.show).getOrElse("not found in tree")

    Either.cond(
      view.subviewHashesConsistentWith(subviewHashes),
      this,
      s"The provided subview hashes are inconsistent with the provided view (view: ${view.viewHash} at position: $viewPos, subview hashes: $subviewHashes)",
    )
  }
}

object LightTransactionViewTree
    extends HasVersionedMessageWithContextCompanion[LightTransactionViewTree, HashOps] {
  override val name: String = "LightTransactionViewTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.LightTransactionViewTree)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> ProtoCodec(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.LightTransactionViewTree)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  final case class InvalidLightTransactionViewTree(message: String)
      extends RuntimeException(message)
  final case class InvalidLightTransactionViewTreeSequence(message: String)
      extends RuntimeException(message)

  private def findTheView(
      tree: GenTransactionTree
  ): Either[String, TransactionView] = {
    val views = tree.rootViews.unblindedElements.flatMap(_.flatten)
    val lightUnblindedViews = views.mapFilter { view =>
      for {
        v <- view.unwrap.toOption
        _cmd <- v.viewCommonData.unwrap.toOption
        _pmd <- v.viewParticipantData.unwrap.toOption
        _ <- if (v.subviews.areFullyBlinded) Some(()) else None
      } yield v
    }
    lightUnblindedViews match {
      case Seq(v) => Right(v)
      case Seq() => Left(s"No light transaction views in tree")
      case l =>
        Left(
          s"Found too many light transaction views: ${l.length}"
        )
    }
  }

  /** @throws InvalidLightTransactionViewTree if the tree is not a legal lightweight transaction view tree
    */
  def tryCreate(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree =
    create(tree, subviewHashes, protocolVersion).valueOr(err =>
      throw InvalidLightTransactionViewTree(err)
    )

  def create(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
      protocolVersion: ProtocolVersion,
  ): Either[String, LightTransactionViewTree] =
    if (protocolVersion >= ProtocolVersion.v4)
      createV1(tree, subviewHashes)
    else
      createV0(tree)

  private def createV0(tree: GenTransactionTree): Either[String, LightTransactionViewTree] =
    findTheView(tree).map(LightTransactionViewTreeV0(tree, _))

  private def createV1(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
  ): Either[String, LightTransactionViewTree] =
    findTheView(tree).flatMap(LightTransactionViewTreeV1(tree, _, subviewHashes).validated)

  private def fromProtoV0(
      hashOps: HashOps,
      protoT: v0.LightTransactionViewTree,
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      tree <- GenTransactionTree.fromProtoV0(hashOps, protoTree)
      result <- LightTransactionViewTree
        .createV0(tree)
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e")
        )
    } yield result

  private def fromProtoV1(
      hashOps: HashOps,
      protoT: v1.LightTransactionViewTree,
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      tree <- GenTransactionTree.fromProtoV1(hashOps, protoTree)
      subviewHashes <- protoT.subviewHashes.traverse(ViewHash.fromProtoPrimitive)
      result <- LightTransactionViewTree
        .createV1(tree, subviewHashes)
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e")
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
      lens: PLens[A, B, LightTransactionViewTree, TransactionViewTree],
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
        val fullViewTree = TransactionViewTree.tryCreate(
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
      tvt: TransactionViewTree,
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree = {
    val withBlindedSubviews = tvt.view.copy(subviews = tvt.view.subviews.blindFully)
    val genTransactionTree =
      tvt.tree.mapUnblindedRootViews(_.replace(tvt.viewHash, withBlindedSubviews))
    // By definition, the view in a TransactionViewTree has all subviews unblinded
    LightTransactionViewTree.tryCreate(genTransactionTree, tvt.subviewHashes, protocolVersion)
  }

}
