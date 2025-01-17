// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.data.TransactionViewTree.InvalidTransactionViewTree
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

import scala.annotation.tailrec

/** Wraps a `GenTransactionTree` where exactly one view (including subviews) is unblinded.
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  */
final case class FullTransactionViewTree private (tree: GenTransactionTree)
    extends TransactionViewTree
    with PrettyPrinting {

  @tailrec
  private[data] override def findTheView(
      viewsWithIndex: Seq[(TransactionView, MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): Either[String, (TransactionView, ViewPosition)] = {
    viewsWithIndex match {
      case Seq() =>
        Left("A transaction view tree must contain an unblinded view.")
      case Seq((singleView, index)) if singleView.hasAllLeavesBlinded =>
        findTheView(singleView.subviews.unblindedElementsWithIndex, index +: viewPosition)
      case Seq((singleView, index)) if singleView.isFullyUnblinded =>
        Right((singleView, index +: viewPosition))
      case Seq((singleView, _index)) =>
        Left(s"A transaction view tree must contain a fully unblinded view: $singleView")
      case multipleViews =>
        Left(
          s"A transaction view tree must not contain several unblinded views: ${multipleViews.map(_._1)}"
        )
    }
  }

  lazy val tryFlattenToParticipantViews: Seq[ParticipantTransactionView] =
    view.tryFlattenToParticipantViews

  override def pretty: Pretty[FullTransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

object FullTransactionViewTree {

  /** @throws TransactionViewTree$.InvalidTransactionViewTree if tree is not a transaction view tree
    *                                                         (i.e. the wrong set of nodes is blinded)
    */
  def tryCreate(tree: GenTransactionTree): FullTransactionViewTree =
    create(tree).valueOr(msg => throw InvalidTransactionViewTree(msg))

  def create(tree: GenTransactionTree): Either[String, FullTransactionViewTree] =
    FullTransactionViewTree(tree).validated

}
