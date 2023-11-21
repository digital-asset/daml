// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.Chain
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.{nowarn, tailrec}
import scala.concurrent.{ExecutionContext, Future}

trait TransactionViewDecompositionFactory {

  /** Converts `transaction: Transaction` into the corresponding `ViewDecomposition`s.
    */
  def fromTransaction(
      confirmationPolicy: ConfirmationPolicy,
      topologySnapshot: TopologySnapshot,
      transaction: WellFormedTransaction[WithoutSuffixes],
      viewRbContext: RollbackContext,
      submittingAdminPartyO: Option[LfPartyId],
  )(implicit ec: ExecutionContext): Future[Seq[NewView]]
}

object TransactionViewDecompositionFactory {

  private type ConformationPolicy = (Set[Informee], NonNegativeInt)

  def apply(protocolVersion: ProtocolVersion): TransactionViewDecompositionFactory = {
    if (protocolVersion >= ProtocolVersion.v5) V2 else V1
  }

  private[data] object V1 extends TransactionViewDecompositionFactory {

    override def fromTransaction(
        confirmationPolicy: ConfirmationPolicy,
        topologySnapshot: TopologySnapshot,
        transaction: WellFormedTransaction[WithoutSuffixes],
        viewRbContext: RollbackContext,
        submittingAdminPartyO: Option[LfPartyId],
    )(implicit ec: ExecutionContext): Future[Seq[NewView]] = {

      def idAndNode(id: LfNodeId): (LfNodeId, LfNode) = id -> transaction.unwrap.nodes(id)

      def createNewView: ((LfNodeId, LfActionNode, RollbackContext)) => Future[NewView] = {
        case (rootNodeId, rootNode, rbContext) =>
          confirmationPolicy
            .informeesAndThreshold(rootNode, topologySnapshot)
            .flatMap { case (informees, threshold) =>
              val rootSeed = transaction.seedFor(rootNodeId)
              val tailNodesF = collectTailNodes(rootNode, informees, threshold, rbContext)
              tailNodesF.map(tailNodes =>
                NewView(
                  LfTransactionUtil.lightWeight(rootNode),
                  informees,
                  threshold,
                  rootSeed,
                  rootNodeId,
                  tailNodes,
                  rbContext,
                )
              )
            }
      }

      def collectTailNodes(
          rootNode: LfActionNode,
          viewInformees: Set[Informee],
          viewThreshold: NonNegativeInt,
          rbContext: RollbackContext,
      )(implicit ec: ExecutionContext): Future[Seq[TransactionViewDecomposition]] = {

        val children = LfTransactionUtil.children(rootNode).map(idAndNode)
        val actionNodeChildren = peelAwayTopLevelRollbackNodes(children, rbContext)
        actionNodeChildren
          .parTraverse { case (childNodeId, childNode, childRbContext) =>
            confirmationPolicy.informeesAndThreshold(childNode, topologySnapshot).flatMap {
              case (childInformees, childThreshold) =>
                if (childInformees == viewInformees && childThreshold == viewThreshold) {
                  // childNode belongs to the core of the view, as informees and threshold are the same

                  val nodeAsSameView =
                    SameView(LfTransactionUtil.lightWeight(childNode), childNodeId, childRbContext)

                  val otherTailNodesF =
                    collectTailNodes(childNode, viewInformees, viewThreshold, childRbContext)
                  otherTailNodesF.map(nodeAsSameView +: _)
                } else {
                  // Node is the root of a direct subview, as informees or threshold are different
                  createNewView((childNodeId, childNode, childRbContext)).map(Seq(_))
                }
            }
          }
          .map(_.flatten)
      }

      /* Ensure all top-level nodes are action nodes by moving rollback hierarchy into
       * respective rollback-contexts. Action node subtrees don't change.
       */
      def peelAwayTopLevelRollbackNodes(
          nodes: Seq[(LfNodeId, LfNode)],
          rbContext: RollbackContext,
      ): Seq[(LfNodeId, LfActionNode, RollbackContext)] = {
        type LfNodesWithRbContext = (RollbackContext, Seq[(LfNodeId, LfNode)])
        val actionNodes = List.newBuilder[(LfNodeId, LfActionNode, RollbackContext)]

        // Perform depth-first search only descending on rollback nodes, lifting up action nodes nested
        // under one or more levels of rollback nodes. Action nodes are lifted up in-order.
        @nowarn("msg=match may not be exhaustive")
        @tailrec
        def go(
            nodesGroupedByRbContext: Seq[LfNodesWithRbContext]
        ): Seq[(LfNodeId, LfActionNode, RollbackContext)] =
          nodesGroupedByRbContext match {
            case Seq() => actionNodes.result()
            case (_, Seq()) +: remainingGroups =>
              go(remainingGroups)
            case (rbContext, (nodeId, an: LfActionNode) +: remainingNodes) +: remainingGroups =>
              // append action node in depth-first left-to-right order
              actionNodes += ((nodeId, an, rbContext))
              go((rbContext -> remainingNodes) +: remainingGroups)
            case (rbContext, (_, rn: LfNodeRollback) +: remainingNodes) +: remainingGroups =>
              val rollbackChildren = rn.children.map(idAndNode).toSeq
              val rollbackChildrenRbContext = rbContext.enterRollback
              val remainingChildrenRbContext = rollbackChildrenRbContext.exitRollback
              // Grouping rollback children and siblings by rollback context along with groups from "earlier calls"
              // enables single tail-recursive call.
              go(
                Seq(
                  rollbackChildrenRbContext -> rollbackChildren,
                  remainingChildrenRbContext -> remainingNodes,
                ) ++ remainingGroups
              )
          }

        go(Seq(rbContext -> nodes))
      }

      val rootNodes =
        peelAwayTopLevelRollbackNodes(transaction.unwrap.roots.toSeq.map(idAndNode), viewRbContext)
      rootNodes.parTraverse(createNewView)
    }

  }

  private[data] object V2 extends TransactionViewDecompositionFactory {

    private final case class ActionNodeInfo(
        informees: Set[Informee],
        threshold: NonNegativeInt,
        children: Seq[LfNodeId],
        seed: Option[LfHash],
    ) {
      def confirmationPolicy: (Set[Informee], NonNegativeInt) = (informees, threshold)
    }

    private final case class BuildState[V](
        views: Chain[V] = Chain.empty,
        rollbackContext: RollbackContext = RollbackContext.empty,
    ) {
      def withViews(
          views: Chain[V],
          rollbackContext: RollbackContext,
      ): BuildState[V] =
        BuildState[V](this.views ++ views, rollbackContext)

      def withNewView(view: V, rollbackContext: RollbackContext): BuildState[V] = {
        BuildState[V](views :+ view, rollbackContext)
      }

      def childState: BuildState[TransactionViewDecomposition] =
        BuildState(Chain.empty, rollbackContext)

      def enterRollback(): BuildState[V] = copy(rollbackContext = rollbackContext.enterRollback)

      def exitRollback(): BuildState[V] = copy(rollbackContext = rollbackContext.exitRollback)
    }

    private final case class Builder(
        nodesM: Map[LfNodeId, LfNode],
        actionNodeInfoM: Map[LfNodeId, ActionNodeInfo],
    ) {

      private def node(nodeId: LfNodeId): LfNode = nodesM.getOrElse(
        nodeId,
        throw new IllegalStateException(s"Did not find $nodeId in node map"),
      )

      private def actionNodeInfo(nodeId: LfNodeId): ActionNodeInfo =
        actionNodeInfoM.getOrElse(
          nodeId,
          throw new IllegalStateException(s"Did not find $nodeId in policy map"),
        )

      private def build(nodeId: LfNodeId, state: BuildState[NewView]): BuildState[NewView] = {
        node(nodeId) match {
          case actionNode: LfActionNode =>
            buildNewView[NewView](nodeId, actionNode, actionNodeInfo(nodeId), state)
          case rollbackNode: LfNodeRollback =>
            rollbackNode.children
              .foldLeft(state.enterRollback()) { (bs, nId) =>
                build(nId, bs)
              }
              .exitRollback()
        }
      }

      def builds(nodeIds: Seq[LfNodeId], state: BuildState[NewView]): BuildState[NewView] = {
        nodeIds.foldLeft(state)((s, nid) => build(nid, s))
      }

      private def buildNewView[V >: NewView](
          nodeId: LfNodeId,
          actionNode: LfActionNode,
          info: ActionNodeInfo,
          state: BuildState[V],
      ): BuildState[V] = {

        val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
          buildChildView(nId, info.confirmationPolicy, bs)
        }

        val newView = NewView(
          LfTransactionUtil.lightWeight(actionNode),
          info.informees,
          info.threshold,
          info.seed,
          nodeId,
          childState.views.toList,
          state.rollbackContext,
        )

        state.withNewView(newView, childState.rollbackContext)

      }

      private def buildChildView(
          nodeId: LfNodeId,
          parentConfirmationPolicy: ConformationPolicy,
          state: BuildState[TransactionViewDecomposition],
      ): BuildState[TransactionViewDecomposition] = {
        node(nodeId) match {
          case actionNode: LfActionNode =>
            val info = actionNodeInfoM(nodeId)
            if (parentConfirmationPolicy == info.confirmationPolicy) {
              val sameView =
                SameView(LfTransactionUtil.lightWeight(actionNode), nodeId, state.rollbackContext)
              val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
                buildChildView(nId, parentConfirmationPolicy, bs)
              }
              state.withViews(sameView +: childState.views, childState.rollbackContext)
            } else {
              buildNewView(nodeId, actionNode, info, state)
            }
          case rollbackNode: LfNodeRollback =>
            rollbackNode.children
              .foldLeft(state.enterRollback()) { (bs, nId) =>
                buildChildView(nId, parentConfirmationPolicy, bs)
              }
              .exitRollback()
        }
      }
    }

    override def fromTransaction(
        confirmationPolicy: ConfirmationPolicy,
        topologySnapshot: TopologySnapshot,
        transaction: WellFormedTransaction[WithoutSuffixes],
        viewRbContext: RollbackContext,
        submittingAdminPartyO: Option[LfPartyId],
    )(implicit ec: ExecutionContext): Future[Seq[NewView]] = {

      val tx: LfVersionedTransaction = transaction.unwrap

      val policyMapF = tx.nodes.collect({ case (nodeId, node: LfActionNode) =>
        val itF = confirmationPolicy.informeesAndThreshold(node, topologySnapshot)
        val childNodeIds = node match {
          case e: LfNodeExercises => e.children.toSeq
          case _ => Seq.empty
        }
        itF.map({ case (i, t) =>
          nodeId -> ActionNodeInfo(i, t, childNodeIds, transaction.seedFor(nodeId))
        })
      })

      Future.sequence(policyMapF).map(_.toMap).map { policyMap =>
        Builder(tx.nodes, policyMap)
          .builds(tx.roots.toSeq, BuildState[NewView](rollbackContext = viewRbContext))
          .views
          .map(_.withSubmittingAdminParty(submittingAdminPartyO, confirmationPolicy))
          .toList
      }
    }
  }
}
