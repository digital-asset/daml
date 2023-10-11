// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.data.TransactionViewTree.InvalidTransactionViewTree
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*

import java.util.UUID
import scala.annotation.tailrec

/** Wraps a `GenTransactionTree` where exactly one view (including subviews) is unblinded.
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  *
  * @throws TransactionViewTree$.InvalidTransactionViewTree if [[tree]] is not a transaction view tree
  *                                    (i.e. the wrong set of nodes is blinded)
  */
final case class TransactionViewTree private (tree: GenTransactionTree)
    extends ViewTree
    with PrettyPrinting {

  @tailrec
  private def findTheView(
      viewsWithIndex: Seq[(TransactionView, MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): (TransactionView, ViewPosition) = {
    viewsWithIndex match {
      case Seq() =>
        throw InvalidTransactionViewTree("A transaction view tree must contain an unblinded view.")
      case Seq((singleView, index)) if singleView.hasAllLeavesBlinded =>
        findTheView(singleView.subviews.unblindedElementsWithIndex, index +: viewPosition)
      case Seq((singleView, index)) if singleView.isFullyUnblinded =>
        (singleView, index +: viewPosition)
      case Seq((singleView, _index)) =>
        throw InvalidTransactionViewTree(
          s"A transaction view tree must contain a fully unblinded view: $singleView"
        )
      case multipleViews =>
        throw InvalidTransactionViewTree(
          s"A transaction view tree must not contain several unblinded views: ${multipleViews.map(_._1)}"
        )
    }
  }

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  lazy val transactionUuid: UUID = checked(tree.commonMetadata.tryUnwrap).uuid

  private val viewAndPosition = findTheView(
    tree.rootViews.unblindedElementsWithIndex
  )

  /** The (top-most) unblinded view. */
  val view: TransactionView = viewAndPosition._1

  override val viewPosition: ViewPosition = viewAndPosition._2

  /** Returns the hashes of the direct subviews of the view represented by this tree.
    * By definition, all subviews are unblinded, therefore this will also work when the subviews
    * are stored in a MerkleSeq.
    */
  def subviewHashes: Seq[ViewHash] = view.subviews.trySubviewHashes

  lazy val tryFlattenToParticipantViews: Seq[ParticipantTransactionView] =
    view.tryFlattenToParticipantViews

  override val viewHash: ViewHash = ViewHash.fromRootHash(view.rootHash)

  /** Determines whether `view` is top-level. */
  val isTopLevel: Boolean = viewPosition.position.sizeCompare(1) == 0

  val submitterMetadataO: Option[SubmitterMetadata] = {
    val result = tree.submitterMetadata.unwrap.toOption

    if (result.isEmpty == isTopLevel) {
      throw InvalidTransactionViewTree(
        "The submitter metadata must be unblinded if and only if the represented view is top-level. " +
          s"Submitter metadata: ${tree.submitterMetadata.unwrap
              .fold(_ => "blinded", _ => "unblinded")}, isTopLevel: $isTopLevel"
      )
    }

    result
  }

  private[this] val commonMetadata: CommonMetadata =
    tree.commonMetadata.unwrap
      .getOrElse(
        throw InvalidTransactionViewTree(
          s"The common metadata of a transaction view tree must be unblinded."
        )
      )

  override def domainId: DomainId = commonMetadata.domainId

  override def mediator: MediatorRef = commonMetadata.mediator

  def confirmationPolicy: ConfirmationPolicy = commonMetadata.confirmationPolicy

  private def unwrapParticipantMetadata: ParticipantMetadata =
    tree.participantMetadata.unwrap
      .getOrElse(
        throw InvalidTransactionViewTree(
          s"The participant metadata of a transaction view tree must be unblinded."
        )
      )

  val ledgerTime: CantonTimestamp = unwrapParticipantMetadata.ledgerTime

  val submissionTime: CantonTimestamp = unwrapParticipantMetadata.submissionTime

  val workflowIdO: Option[WorkflowId] = unwrapParticipantMetadata.workflowIdO

  override lazy val informees: Set[Informee] = view.viewCommonData.tryUnwrap.informees

  lazy val viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap

  override def toBeSigned: Option[RootHash] =
    if (isTopLevel) Some(transactionId.toRootHash) else None

  override def rootHash: RootHash = tree.rootHash

  override def pretty: Pretty[TransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

object TransactionViewTree {
  def tryCreate(tree: GenTransactionTree): TransactionViewTree = TransactionViewTree(tree)

  def create(tree: GenTransactionTree): Either[String, TransactionViewTree] =
    Either.catchOnly[InvalidTransactionViewTree](TransactionViewTree(tree)).leftMap(_.message)

  /** Indicates an attempt to create an invalid [[TransactionViewTree]]. */
  final case class InvalidTransactionViewTree(message: String) extends RuntimeException(message) {}
}
