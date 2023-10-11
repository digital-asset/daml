// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*

import scala.annotation.tailrec

/** Wrapper type for elements of a view decomposition
  */
sealed trait TransactionViewDecomposition extends Product with Serializable with PrettyPrinting {
  def lfNode: LfActionNode
  def nodeId: LfNodeId
  def rbContext: RollbackContext
}

object TransactionViewDecomposition {

  /** Encapsulates a new view.
    *
    * @param rootNode the node constituting the view
    * @param informees the informees of rootNode
    * @param threshold the threshold of rootNode
    * @param rootSeed the seed of the rootNode
    * @param tailNodes all core nodes except `rootNode` and all child views, sorted in pre-order traversal order
    *
    * @throws java.lang.IllegalArgumentException if a subview has the same `informees` and `threshold`
    */
  final case class NewView(
      rootNode: LfActionNode,
      informees: Set[Informee],
      threshold: NonNegativeInt,
      rootSeed: Option[LfHash],
      override val nodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
      override val rbContext: RollbackContext,
  ) extends TransactionViewDecomposition {

    childViews.foreach { sv =>
      require(
        (sv.informees, sv.threshold) != ((informees, threshold)),
        s"Children must have different informees or thresholds than parent. " +
          s"Found threshold $threshold and informees $informees",
      )
    }

    override def lfNode: LfActionNode = rootNode

    /** All nodes of this view, i.e. core nodes and subviews, in execution order */
    def allNodes: NonEmpty[Seq[TransactionViewDecomposition]] =
      NonEmpty(Seq, SameView(rootNode, nodeId, rbContext), tailNodes: _*)

    def childViews: Seq[NewView] = tailNodes.collect { case v: NewView => v }

    /** This view with the submittingAdminParty (if defined) added as extra confirming party.
      * This needs to be called on root views to guarantee proper authorization.
      */
    def withSubmittingAdminParty(
        submittingAdminPartyO: Option[LfPartyId],
        confirmationPolicy: ConfirmationPolicy,
    ): NewView = {
      val (newInformees, newThreshold) =
        confirmationPolicy.withSubmittingAdminParty(submittingAdminPartyO)(informees, threshold)

      copy(informees = newInformees, threshold = newThreshold)
    }

    override def pretty: Pretty[NewView] = prettyOfClass(
      param("root node template", _.rootNode.templateId),
      param("informees", _.informees),
      param("threshold", _.threshold),
      param("node ID", _.nodeId),
      param("rollback context", _.rbContext),
      param("tail nodes", _.tailNodes),
    )
  }

  /** Encapsulates a node that belongs to core of some [[com.digitalasset.canton.data.TransactionViewDecomposition.NewView]]. */
  final case class SameView(
      lfNode: LfActionNode,
      override val nodeId: LfNodeId,
      override val rbContext: RollbackContext,
  ) extends TransactionViewDecomposition {

    override def pretty: Pretty[SameView] = prettyOfClass(
      param("lf node template", _.lfNode.templateId),
      param("node ID", _.nodeId),
      param("rollback context", _.rbContext),
    )
  }

  // Count all the NewViews representing actual views (in contrast to SameViews that are part of their parent)
  @tailrec
  def countNestedViews(views: Seq[TransactionViewDecomposition], count: Int = 0): Int = {
    views match {
      case head +: rest =>
        head match {
          case (newView: TransactionViewDecomposition.NewView) =>
            countNestedViews(newView.tailNodes ++ rest, count + 1)
          case (_: TransactionViewDecomposition.SameView) =>
            countNestedViews(rest, count)
        }
      case _ => // scala compiler is not happy matching on Seq() thinking that there is some other missing case
        count
    }
  }
}
