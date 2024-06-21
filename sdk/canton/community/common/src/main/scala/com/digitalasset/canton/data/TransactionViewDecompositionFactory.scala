// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.Chain
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LfTransactionUtil

import scala.concurrent.{ExecutionContext, Future}

case object TransactionViewDecompositionFactory {

  /** Keeps track of the state of the transaction view tree.
    *
    * @param views chains `NewView` and `SameView` as they get created to construct a transaction view tree
    * @param informees is used to aggregate the informees' partyId until a NewView is created
    * @param quorums is used to aggregate the different quorums (originated from the different ActionNodes) until a
    *                NewView is created
    */
  final private case class BuildState[V](
      views: Chain[V] = Chain.empty,
      informees: Set[LfPartyId] = Set.empty,
      quorums: Chain[Quorum] = Chain.empty,
      rollbackContext: RollbackContext = RollbackContext.empty,
  ) {

    def withViews(
        views: Chain[V],
        informees: Set[LfPartyId],
        quorums: Chain[Quorum],
        rollbackContext: RollbackContext,
    ): BuildState[V] =
      BuildState[V](
        this.views ++ views,
        this.informees ++ informees,
        this.quorums ++ quorums,
        rollbackContext,
      )

    def withNewView(view: V, rollbackContext: RollbackContext): BuildState[V] = {
      BuildState[V](this.views :+ view, this.informees, this.quorums, rollbackContext)
    }

    def childState: BuildState[TransactionViewDecomposition] =
      BuildState(Chain.empty, Set.empty, Chain.empty, rollbackContext)

    def enterRollback(): BuildState[V] = copy(rollbackContext = rollbackContext.enterRollback)

    def exitRollback(): BuildState[V] = copy(rollbackContext = rollbackContext.exitRollback)
  }

  final private case class ActionNodeInfo(
      informees: Map[LfPartyId, Set[ParticipantId]],
      quorum: Quorum,
      children: Seq[LfNodeId],
      seed: Option[LfHash],
  ) {
    lazy val participants: Set[ParticipantId] = informees.values.flatten.toSet
  }

  final private case class Builder(
      nodesM: Map[LfNodeId, LfNode],
      actionNodeInfoM: Map[LfNodeId, ActionNodeInfo],
  ) {

    private def node(nodeId: LfNodeId): LfNode = nodesM.getOrElse(
      nodeId,
      throw new IllegalStateException(s"Did not find $nodeId in node map"),
    )

    private def build(nodeId: LfNodeId, state: BuildState[NewView]): BuildState[NewView] =
      node(nodeId) match {
        case actionNode: LfActionNode =>
          val info = actionNodeInfoM(nodeId)
          buildNewView[NewView](nodeId, actionNode, info, state)
        case rollbackNode: LfNodeRollback =>
          builds(rollbackNode.children.toSeq, state.enterRollback()).exitRollback()
      }

    def builds(nodeIds: Seq[LfNodeId], state: BuildState[NewView]): BuildState[NewView] =
      nodeIds.foldLeft(state)((s, nid) => build(nid, s))

    private def buildNewView[V >: NewView](
        nodeId: LfNodeId,
        actionNode: LfActionNode,
        info: ActionNodeInfo,
        state: BuildState[V],
    ): BuildState[V] = {

      val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
        buildChildView(nId, info.participants, bs)
      }

      val newView = NewView(
        LfTransactionUtil.lightWeight(actionNode),
        /* We can use tryCreate here because at this point we only have one quorum
         * that is generated directly from the informees of the action node.
         * Only later in the process (after the tree is built) do we aggregate all the children
         * unique quorums together into a list held by the `parent` view.
         */
        ViewConfirmationParameters
          .tryCreate(
            info.informees.keySet ++ childState.informees,
            (info.quorum +: childState.quorums.toList).distinct,
          ),
        info.seed,
        nodeId,
        childState.views.toList,
        state.rollbackContext,
      )

      state.withNewView(newView, childState.rollbackContext)
    }

    private def buildChildView(
        nodeId: LfNodeId,
        currentParticipants: Set[ParticipantId],
        state: BuildState[TransactionViewDecomposition],
    ): BuildState[TransactionViewDecomposition] = {

      /* The recipients of a transaction node are all participants that
       * host a witness of the node. So we should look at the participant recipients of
       * a node to decide when a new view is needed. In particular, a change in the informees triggers a new view only if
       * a new informee participant enters the game.
       */
      def needNewView(
          node: ActionNodeInfo,
          currentParticipants: Set[ParticipantId],
      ): Boolean = !node.participants.subsetOf(currentParticipants)

      node(nodeId) match {
        case actionNode: LfActionNode =>
          val info = actionNodeInfoM(nodeId)
          if (!needNewView(info, currentParticipants)) {
            val sameView = SameView(
              LfTransactionUtil.lightWeight(actionNode),
              nodeId,
              state.rollbackContext,
            )
            val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
              buildChildView(nId, currentParticipants, bs)
            }

            state
              .withViews(
                sameView +: childState.views,
                info.informees.keySet ++ childState.informees,
                info.quorum +: childState.quorums,
                childState.rollbackContext,
              )
          } else
            buildNewView(nodeId, actionNode, info, state)
        case rollbackNode: LfNodeRollback =>
          rollbackNode.children
            .foldLeft(state.enterRollback()) { (bs, nId) =>
              buildChildView(nId, currentParticipants, bs)
            }
            .exitRollback()
      }
    }
  }

  def fromTransaction(
      confirmationPolicy: ConfirmationPolicy,
      topologySnapshot: TopologySnapshot,
      transaction: WellFormedTransaction[WithoutSuffixes],
      viewRbContext: RollbackContext,
      submittingAdminPartyO: Option[LfPartyId],
  )(implicit ec: ExecutionContext, tc: TraceContext): Future[Seq[NewView]] = {

    val tx: LfVersionedTransaction = transaction.unwrap

    val policyMapF: Iterable[Future[(NodeId, ActionNodeInfo)]] =
      tx.nodes.collect({ case (nodeId, node: LfActionNode) =>
        val childNodeIds = node match {
          case e: LfNodeExercises => e.children.toSeq
          case _ => Seq.empty
        }
        createActionNodeInfo(
          confirmationPolicy,
          topologySnapshot,
          node,
          nodeId,
          childNodeIds,
          transaction,
        )
      })

    Future.sequence(policyMapF).map(_.toMap).map { policyMap =>
      Builder(tx.nodes, policyMap)
        .builds(tx.roots.toSeq, BuildState[NewView](rollbackContext = viewRbContext))
        .views
        .map(
          _.withSubmittingAdminParty(submittingAdminPartyO, confirmationPolicy)
        )
        .toList
    }
  }

  private def createActionNodeInfo(
      confirmationPolicy: ConfirmationPolicy,
      topologySnapshot: TopologySnapshot,
      node: LfActionNode,
      nodeId: LfNodeId,
      childNodeIds: Seq[LfNodeId],
      transaction: WellFormedTransaction[WithoutSuffixes],
  )(implicit ec: ExecutionContext, tc: TraceContext): Future[(LfNodeId, ActionNodeInfo)] = {
    def createQuorum(
        informeesMap: Map[LfPartyId, (Set[ParticipantId], NonNegativeInt)],
        threshold: NonNegativeInt,
    ): Quorum = {
      Quorum(
        informeesMap.mapFilter { case (_, weight) =>
          Option.when(weight.unwrap > 0)(
            PositiveInt.tryCreate(weight.unwrap)
          )
        },
        threshold,
      )
    }

    val itF = confirmationPolicy.informeesParticipantsAndThreshold(node, topologySnapshot)
    itF.map({ case (i, t) =>
      nodeId -> ActionNodeInfo(
        i.fmap { case (participants, _) => participants },
        createQuorum(i, t),
        childNodeIds,
        transaction.seedFor(nodeId),
      )
    })
  }

}
