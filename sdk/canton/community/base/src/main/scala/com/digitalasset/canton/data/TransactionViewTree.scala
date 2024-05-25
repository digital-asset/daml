// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RootHash, TransactionId, ViewHash}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.{LfPartyId, WorkflowId}

import java.util.UUID

trait TransactionViewTree extends ViewTree {

  import TransactionViewTree.*

  def tree: GenTransactionTree

  private[data] def findTheView(
      viewsWithIndex: Seq[(TransactionView, MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): Either[String, (TransactionView, ViewPosition)]

  private[data] lazy val viewAndPositionOrErr: Either[String, (TransactionView, ViewPosition)] =
    findTheView(tree.rootViews.unblindedElementsWithIndex)

  private[data] lazy val viewAndPosition: (TransactionView, ViewPosition) =
    viewAndPositionOrErr.valueOr(msg => throw InvalidTransactionViewTree(msg))

  /** The (top-most) unblinded view. */
  lazy val view: TransactionView = viewAndPosition._1

  override lazy val viewPosition: ViewPosition = viewAndPosition._2

  /** Determines whether `view` is top-level. */
  lazy val isTopLevel: Boolean = viewPosition.position.sizeCompare(1) == 0

  def validated: Either[String, this.type] = for {
    _ <- viewAndPositionOrErr
    _ <- validateMetadata(tree, isTopLevel)
  } yield this

  override def rootHash: RootHash = tree.rootHash

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(rootHash)

  override def toBeSigned: Option[RootHash] = if (isTopLevel) Some(rootHash) else None

  /** Returns the hashes of the direct subviews of the view represented by this tree.
    * By definition, all subviews are unblinded, therefore this will also work when the subviews
    * are stored in a MerkleSeq.
    */
  def subviewHashes: Seq[ViewHash] = view.subviews.trySubviewHashes

  override lazy val viewHash: ViewHash = ViewHash.fromRootHash(view.rootHash)

  override lazy val informees: Set[LfPartyId] =
    view.viewCommonData.tryUnwrap.viewConfirmationParameters.informees

  lazy val viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap

  val submitterMetadataO: Option[SubmitterMetadata] = tree.submitterMetadata.unwrap.toOption

  private[data] lazy val commonMetadata: CommonMetadata = tree.commonMetadata.tryUnwrap

  lazy val transactionUuid: UUID = commonMetadata.uuid

  override def domainId: DomainId = commonMetadata.domainId

  override def mediator: MediatorGroupRecipient = commonMetadata.mediator

  def confirmationPolicy: ConfirmationPolicy = commonMetadata.confirmationPolicy

  private[data] def participantMetadata: ParticipantMetadata = tree.participantMetadata.tryUnwrap

  lazy val ledgerTime: CantonTimestamp = participantMetadata.ledgerTime

  lazy val submissionTime: CantonTimestamp = participantMetadata.submissionTime

  lazy val workflowIdO: Option[WorkflowId] = participantMetadata.workflowIdO
}

object TransactionViewTree {

  private[data] def validateMetadata(
      tree: GenTransactionTree,
      isTopLevel: Boolean,
  ): Either[String, Unit] = for {
    _ <- tree.commonMetadata.unwrap.leftMap(_ =>
      "The common metadata of a transaction view tree must be unblinded."
    )

    _ <- tree.participantMetadata.unwrap.leftMap(_ =>
      "The participant metadata of a transaction view tree must be unblinded."
    )

    _ <- EitherUtil.condUnitE(
      isTopLevel == tree.submitterMetadata.isFullyUnblinded,
      "The submitter metadata must be unblinded if and only if the represented view is top-level. " +
        s"Submitter metadata: ${tree.submitterMetadata.unwrap.fold(_ => "blinded", _ => "unblinded")}, " +
        s"isTopLevel: $isTopLevel",
    )

  } yield ()

  /** Indicates an attempt to create an invalid [[TransactionViewTree]]. */
  final case class InvalidTransactionViewTree(message: String) extends RuntimeException(message) {}

}
