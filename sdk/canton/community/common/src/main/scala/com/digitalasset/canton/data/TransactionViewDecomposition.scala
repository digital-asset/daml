// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*

import scala.annotation.tailrec

/** Wrapper type for elements of a view decomposition
  *
  * It contains an `lfNode` as well as all descendant nodes, categorized either as `NewView` or `SameView`.
  */
sealed trait TransactionViewDecomposition extends Product with Serializable with PrettyPrinting {
  def lfNode: LfActionNode
  def nodeId: LfNodeId
  def rbContext: RollbackContext
}

object TransactionViewDecomposition {

  /** Encapsulates a new view.
    * A `rootNode` is categorized as `NewView` if it will be sent explicitly to all informee participants.
    * This is the case for all root nodes of the underlying transaction or if the parent node has fewer informee participants.
    *
    * @param rootNode the node constituting the view
    * @param viewConfirmationParameters contains both the informees of rootNode and quorums
    * @param rootSeed the seed of the rootNode
    * @param tailNodes all core nodes except `rootNode` and all child views, sorted in pre-order traversal order
    *
    * @throws java.lang.IllegalArgumentException if a subview has the same `informees` and `threshold`
    */
  final case class NewView(
      rootNode: LfActionNode,
      viewConfirmationParameters: ViewConfirmationParameters,
      rootSeed: Option[LfHash],
      override val nodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
      override val rbContext: RollbackContext,
  ) extends TransactionViewDecomposition {

    override def lfNode: LfActionNode = rootNode

    /** All nodes of this view, i.e. core nodes and subviews, in execution order */
    def allNodes: NonEmpty[Seq[TransactionViewDecomposition]] =
      NonEmpty(Seq, SameView(rootNode, nodeId, rbContext), tailNodes*)

    def childViews: Seq[NewView] = tailNodes.collect { case v: NewView => v }

    override protected def pretty: Pretty[NewView] = prettyOfClass(
      param("root node template", _.rootNode.templateId),
      param("view confirmation parameters", _.viewConfirmationParameters),
      param("node ID", _.nodeId),
      param("rollback context", _.rbContext),
      param("tail nodes", _.tailNodes),
    )
  }

  /** An `lfNode` is categorized as `SameView`, if it is descendant of a node categorized as `NewView` and
    * it will not be sent around explicitly.
    */
  final case class SameView(
      lfNode: LfActionNode,
      override val nodeId: LfNodeId,
      override val rbContext: RollbackContext,
  ) extends TransactionViewDecomposition {

    override protected def pretty: Pretty[SameView] = prettyOfClass(
      param("lf node template", _.lfNode.templateId),
      param("node ID", _.nodeId),
      param("rollback context", _.rbContext),
    )
  }

  // Count all the NewViews representing actual views (in contrast to SameViews that are part of their parent)
  @tailrec
  def countNestedViews(views: Seq[TransactionViewDecomposition], count: Int = 0): Int =
    views match {
      case head +: rest =>
        head match {
          case newView: TransactionViewDecomposition.NewView =>
            countNestedViews(newView.tailNodes ++ rest, count + 1)
          case _: TransactionViewDecomposition.SameView =>
            countNestedViews(rest, count)
        }
      case _ => // scala compiler is not happy matching on Seq() thinking that there is some other missing case
        count
    }
}
