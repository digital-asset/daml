// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
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

    childViews.foreach { sv =>
      require(
        sv.viewConfirmationParameters != viewConfirmationParameters,
        s"Children must have different informees or quorums than parent. " +
          s"Found informees ${viewConfirmationParameters.informees} and quorums ${viewConfirmationParameters.quorums}",
      )
    }

    override def lfNode: LfActionNode = rootNode

    /** All nodes of this view, i.e. core nodes and subviews, in execution order */
    def allNodes: NonEmpty[Seq[TransactionViewDecomposition]] =
      NonEmpty(
        Seq,
        SameView(rootNode, nodeId, rbContext),
        tailNodes: _*
      )

    def childViews: Seq[NewView] = tailNodes.collect { case v: NewView => v }

    /** This view with the submittingAdminParty (if defined) added as an extra confirming party.
      * This needs to be called on root views to guarantee proper authorization.
      * Intended for use in [[com.digitalasset.canton.version.ProtocolVersion.v5]] and lower, it includes
      * the submitting admin party in the original quorum and updates the threshold.
      */
    def withSubmittingAdminParty(
        submittingAdminPartyO: Option[LfPartyId],
        confirmationPolicy: ConfirmationPolicy,
    ): NewView = {
      val newViewConfirmationParameters =
        confirmationPolicy.withSubmittingAdminParty(submittingAdminPartyO)(
          viewConfirmationParameters
        )
      copy(
        viewConfirmationParameters = newViewConfirmationParameters
      )
    }

    /** This view with the submittingAdminParty (if defined) added as an extra confirming party.
      * This needs to be called on root views to guarantee proper authorization.
      * Intended for use in [[com.digitalasset.canton.version.ProtocolVersion.v6]] and higher, it adds
      * an extra quorum with the submitting party.
      */
    def withSubmittingAdminPartyQuorum(
        submittingAdminPartyO: Option[LfPartyId],
        confirmationPolicy: ConfirmationPolicy,
    ): NewView = {
      val newViewConfirmationParameters =
        confirmationPolicy.withSubmittingAdminPartyQuorum(submittingAdminPartyO)(
          viewConfirmationParameters
        )
      copy(
        viewConfirmationParameters = newViewConfirmationParameters
      )
    }

    override def pretty: Pretty[NewView] = prettyOfClass(
      param("root node template", _.rootNode.templateId),
      param("view confirmation parameters", _.viewConfirmationParameters),
      param("node ID", _.nodeId),
      param("rollback context", _.rbContext),
      param("tail nodes", _.tailNodes),
    )
  }

  /** Encapsulates a node that belongs to core of some [[com.digitalasset.canton.data.TransactionViewDecomposition.NewView]].
    */
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
          case newView: TransactionViewDecomposition.NewView =>
            countNestedViews(newView.tailNodes ++ rest, count + 1)
          case _: TransactionViewDecomposition.SameView =>
            countNestedViews(rest, count)
        }
      case _ => // scala compiler is not happy matching on Seq() thinking that there is some other missing case
        count
    }
  }
}
