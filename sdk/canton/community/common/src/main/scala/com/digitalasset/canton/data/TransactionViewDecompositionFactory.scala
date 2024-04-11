// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.Chain
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.lf.transaction.NodeId
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
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

  def apply(protocolVersion: ProtocolVersion): TransactionViewDecompositionFactory = {
    if (protocolVersion >= ProtocolVersion.v6) V3
    else if (protocolVersion >= ProtocolVersion.v5) V2
    else V1
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
                  ViewConfirmationParameters.create(informees, threshold),
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

  private[data] abstract class NewTransactionViewDecompositionFactoryImpl
      extends TransactionViewDecompositionFactory {

    /** Keeps track of the state of the transaction view tree.
      *
      * @param views chains `NewView` and `SameView` as they get created to construct a transaction view tree
      * @param informees is used to aggregate the informees' partyId and trust level until a NewView is created
      * @param quorums is used to aggregate the different quorums (originated from the different ActionNodes) until a
      *                NewView is created
      */
    case class BuildState[V](
        views: Chain[V] = Chain.empty,
        informees: Map[LfPartyId, TrustLevel] = Map.empty,
        quorums: Chain[Quorum] = Chain.empty,
        rollbackContext: RollbackContext = RollbackContext.empty,
    ) {

      def withViews(
          views: Chain[V],
          informees: Map[LfPartyId, TrustLevel],
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
        BuildState[V](views :+ view, Map.empty, Chain.empty, rollbackContext)
      }

      def childState: BuildState[TransactionViewDecomposition] =
        BuildState(Chain.empty, Map.empty, Chain.empty, rollbackContext)

      def enterRollback(): BuildState[V] = copy(rollbackContext = rollbackContext.enterRollback)

      def exitRollback(): BuildState[V] = copy(rollbackContext = rollbackContext.exitRollback)
    }

    abstract protected class Builder[A](
        nodesM: Map[LfNodeId, LfNode],
        actionNodeInfoM: Map[LfNodeId, A],
    ) {

      protected def node(nodeId: LfNodeId): LfNode = nodesM.getOrElse(
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

      protected def buildNewView[V >: NewView](
          nodeId: LfNodeId,
          actionNode: LfActionNode,
          info: A,
          state: BuildState[V],
      ): BuildState[V]

    }

  }

  private[data] object V2 extends NewTransactionViewDecompositionFactoryImpl {

    private final case class ActionNodeInfoV2(
        viewConfirmationParameters: ViewConfirmationParameters,
        children: Seq[LfNodeId],
        seed: Option[LfHash],
    )

    private final case class BuilderV2(
        nodesM: Map[LfNodeId, LfNode],
        actionNodeInfoM: Map[LfNodeId, ActionNodeInfoV2],
    ) extends Builder[ActionNodeInfoV2](nodesM, actionNodeInfoM) {

      override protected def buildNewView[V >: NewView](
          nodeId: LfNodeId,
          actionNode: LfActionNode,
          info: ActionNodeInfoV2,
          state: BuildState[V],
      ): BuildState[V] = {

        val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
          buildChildView(nId, info, bs)
        }

        val newView = NewView(
          LfTransactionUtil.lightWeight(actionNode),
          info.viewConfirmationParameters,
          info.seed,
          nodeId,
          childState.views.toList,
          state.rollbackContext,
        )

        state.withNewView(newView, childState.rollbackContext)
      }

      private def buildChildView(
          nodeId: LfNodeId,
          parentActionNodeInfo: ActionNodeInfoV2,
          state: BuildState[TransactionViewDecomposition],
      ): BuildState[TransactionViewDecomposition] = {

        def needNewView(
            parent: ActionNodeInfoV2,
            node: ActionNodeInfoV2,
        ): Boolean =
          parent.viewConfirmationParameters != node.viewConfirmationParameters

        node(nodeId) match {
          case actionNode: LfActionNode =>
            val info = actionNodeInfoM(nodeId)
            if (!needNewView(parentActionNodeInfo, info)) {
              val sameView = SameView(
                LfTransactionUtil.lightWeight(actionNode),
                nodeId,
                state.rollbackContext,
              )
              val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
                buildChildView(nId, parentActionNodeInfo, bs)
              }
              state.withViews(
                sameView +: childState.views,
                Map.empty,
                Chain.empty,
                childState.rollbackContext,
              )
            } else
              buildNewView(nodeId, actionNode, info, state)
          case rollbackNode: LfNodeRollback =>
            // TODO(#18332): use builds instead of foldLeft
            rollbackNode.children
              .foldLeft(state.enterRollback()) { (bs, nId) =>
                buildChildView(nId, parentActionNodeInfo, bs)
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

      def createActionNodeInfo(
          node: LfActionNode,
          nodeId: LfNodeId,
          childNodeIds: Seq[LfNodeId],
      )(implicit ec: ExecutionContext): Future[(LfNodeId, ActionNodeInfoV2)] = {
        val itF = confirmationPolicy.informeesAndThreshold(node, topologySnapshot)
        itF.map({ case (i, t) =>
          nodeId -> ActionNodeInfoV2(
            ViewConfirmationParameters.create(i, t),
            childNodeIds,
            transaction.seedFor(nodeId),
          )
        })
      }

      val policyMapF: Iterable[Future[(NodeId, ActionNodeInfoV2)]] =
        tx.nodes.collect({ case (nodeId, node: LfActionNode) =>
          val childNodeIds = node match {
            case e: LfNodeExercises => e.children.toSeq
            case _ => Seq.empty
          }
          createActionNodeInfo(
            node,
            nodeId,
            childNodeIds,
          )
        })

      Future.sequence(policyMapF).map(_.toMap).map { policyMap =>
        BuilderV2(tx.nodes, policyMap)
          .builds(tx.roots.toSeq, BuildState[NewView](rollbackContext = viewRbContext))
          .views
          .map(
            _.withSubmittingAdminParty(submittingAdminPartyO, confirmationPolicy)
          )
          .toList
      }
    }

  }

  private[data] object V3 extends NewTransactionViewDecompositionFactoryImpl {

    private final case class ActionNodeInfoV3(
        informees: Map[LfPartyId, (Set[ParticipantId], TrustLevel)],
        quorum: Quorum,
        children: Seq[LfNodeId],
        seed: Option[LfHash],
    ) {
      lazy val participants: Set[ParticipantId] = informees.values.flatMap {
        case (participants, _) => participants
      }.toSet
    }

    private final case class BuilderV3(
        nodesM: Map[LfNodeId, LfNode],
        actionNodeInfoM: Map[LfNodeId, ActionNodeInfoV3],
    ) extends Builder[ActionNodeInfoV3](nodesM, actionNodeInfoM) {

      // make sure that there are no duplicate informees with different trust levels
      private def mergeParentAndChildrenInformees(
          parentInfo: ActionNodeInfoV3,
          childrenInformees: Map[LfPartyId, TrustLevel],
      ): Map[LfPartyId, TrustLevel] = {
        val parentInformees = parentInfo.informees.fmap { case (_, requiredTrustLevel) =>
          requiredTrustLevel
        }
        if (
          parentInformees.keys.exists(key => {
            val parentTrustLevel = parentInformees.get(key)
            val childrenTrustLevel = childrenInformees.get(key)
            childrenTrustLevel.isDefined && parentTrustLevel != childrenTrustLevel
          })
        ) {
          throw new IllegalStateException("Duplicate informees with different trust levels")
        }
        parentInformees ++ childrenInformees
      }

      override protected def buildNewView[V >: NewView](
          nodeId: LfNodeId,
          actionNode: LfActionNode,
          info: ActionNodeInfoV3,
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
              mergeParentAndChildrenInformees(info, childState.informees),
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
            node: ActionNodeInfoV3,
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
                  mergeParentAndChildrenInformees(info, childState.informees),
                  info.quorum +: childState.quorums,
                  childState.rollbackContext,
                )
            } else
              buildNewView(nodeId, actionNode, info, state)
          case rollbackNode: LfNodeRollback =>
            // TODO(#18332): use builds instead of foldLeft
            rollbackNode.children
              .foldLeft(state.enterRollback()) { (bs, nId) =>
                buildChildView(nId, currentParticipants, bs)
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

      val policyMapF: Iterable[Future[(NodeId, ActionNodeInfoV3)]] =
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
        BuilderV3(tx.nodes, policyMap)
          .builds(tx.roots.toSeq, BuildState[NewView](rollbackContext = viewRbContext))
          .views
          .map(
            _.withSubmittingAdminPartyQuorum(submittingAdminPartyO, confirmationPolicy)
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
    )(implicit ec: ExecutionContext): Future[(LfNodeId, ActionNodeInfoV3)] = {
      def createQuorum(
          informeesMap: Map[LfPartyId, (Set[ParticipantId], NonNegativeInt, TrustLevel)],
          threshold: NonNegativeInt,
      ): Quorum = {
        Quorum(
          informeesMap.mapFilter { case (_, weight, _) =>
            Option.when(weight.unwrap > 0)(
              PositiveInt.tryCreate(weight.unwrap)
            )
          },
          threshold,
        )
      }

      val itF = confirmationPolicy.informeesParticipantsAndThreshold(node, topologySnapshot)
      itF.map({ case (i, t) =>
        nodeId -> ActionNodeInfoV3(
          i.fmap { case (participants, _, requiredTrustLevel) =>
            (participants, requiredTrustLevel)
          },
          createQuorum(i, t),
          childNodeIds,
          transaction.seedFor(nodeId),
        )
      })
    }

  }

}
