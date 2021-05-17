// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.transaction.{NodeId, GenTransaction}
import com.daml.lf.transaction.Node.{
  GenNode,
  GenActionNode,
  NodeRollback,
  NodeExercises,
  LeafOnlyActionNode,
}
import com.daml.lf.value.Value
import com.daml.lf.data.ImmArray

private[lf] object NormalizeRollbacks {

  type Nid = NodeId
  type Cid = Value.ContractId
  type TX = GenTransaction[Nid, Cid]
  type Node = GenNode[Nid, Cid]
  type ActNode = GenActionNode[Nid, Cid]

  // Normalize a transaction so rollback nodes satisfy the normalization rules.
  // see `makeRoll` below

  def normalizeTx(txOriginal: TX): TX = {

    // Here we generate fresh node-ids for the normalized transaction.
    val ids = Iterator.from(0).map(NodeId(_)) // perhaps we could reuse orig nids

    // There is no connection between the ids in the pre and post transaction.
    // Semantically this is fine: The ids are just of tie the tree structure together.
    // But perhaps for ease of debugging we might consider trying to reuse existing ids.

    // We accumulate (imperatively!) a nodes-map for the normalized transaction
    var nodes: Map[Nid, Node] = Map.empty

    def pushNode(node: Node): Nid = {
      val nodeId = ids.next()
      nodes += (nodeId -> node)
      nodeId
    }

    def pushNodes(nodes: List[Node]): List[Nid] = nodes.map(pushNode)

    import Canonical.{Norm, Case, caseNorms}
    // The normalization phase works by constructing intermediate values which are
    // `Canonical` in the sense that only properly normalized nodes can be represented.
    // Although this doesn't ensure correctness, one class of bugs is avoided.

    // The `force*` functions move from the world of Norms into standard transactions.
    // This is also where node-ids for the resulting normalized transaction are generated.

    def forceRoll(x: Norm.Roll): Node = {
      x match {
        case Norm.Roll1(act) =>
          val child = pushNode(act.node)
          NodeRollback(children = ImmArray(List(child)))
        case Norm.Roll2(h, m, t) =>
          val nodes = List(h.node) ++ m ++ List(t.node)
          val children = pushNodes(nodes)
          NodeRollback(children = ImmArray(children))
      }
    }

    def forceNorm(x: Norm): Node = {
      x match {
        case Norm.Act(node) => node
        case roll: Norm.Roll => forceRoll(roll)
      }
    }

    def forceNorms(xs: List[Norm]): List[Node] = xs.map(forceNorm)

    // makeRoll: encodes the normalization transformation rules:
    //   rule #1: R [ ] -> ε
    //   rule #2: R [ R [ xs… ] , ys… ] -> R [ xs… ] , R [ ys… ]
    //   rule #3: R [ xs… , R [ ys… ] ] ->  R [ xs… , ys… ]

    def makeRoll(norms: List[Norm]): List[Norm] = {
      caseNorms(norms) match {
        case Case.Empty =>
          // normalization rule #1
          Nil

        case Case.Single(roll: Norm.Roll) =>
          // normalization rule #2 or #3
          List(roll)

        case Case.Single(act: Norm.Act) =>
          // no rule
          List(Norm.Roll1(act))

        case Case.Multi(h: Norm.Roll, m, t) =>
          // normalization rule #2
          h :: makeRoll(m ++ List(t))

        case Case.Multi(h: Norm.Act, m, t: Norm.Roll) =>
          // normalization rule #3
          List(pushIntoRoll(h, forceNorms(m), t))

        case Case.Multi(h: Norm.Act, m, t: Norm.Act) =>
          // no rule
          List(Norm.Roll2(h, forceNorms(m), t))
      }
    }

    def pushIntoRoll(a1: Norm.Act, xs2: List[Node], t: Norm.Roll): Norm.Roll = {
      t match {
        case Norm.Roll1(a3) => Norm.Roll2(a1, xs2, a3)
        case Norm.Roll2(a3, xs4, a5) => Norm.Roll2(a1, xs2 ++ List(a3.node) ++ xs4, a5)
      }
    }

    // Here we traverse the original transaction structure.
    // During the transformation, an original `Node` is mapped into a List[Norm]

    // The List is necessary because the rules can:
    // (1) drop nodes; (2) combine nodes (3) lift nodes from a lower level to a higher level.

    // TODO: https://github.com/digital-asset/daml/issues/8020
    // make this traversal code stack-safe

    txOriginal match {
      case GenTransaction(nodesOriginal, rootsOriginal) =>
        def traverseNids(nids: List[Nid]): List[Norm] = nids.flatMap(traverseNid)
        def traverseNid(nid: Nid): List[Norm] = traverseNode(nodesOriginal(nid))

        def traverseNode(node: Node): List[Norm] = {
          node match {

            case NodeRollback(children) =>
              makeRoll(traverseNids(children.toList))

            case ex: NodeExercises[_, _] =>
              val norms = traverseNids(ex.children.toList)
              val children = pushNodes(forceNorms(norms))
              List(Norm.Act(ex.copy(children = ImmArray(children))))

            case act: LeafOnlyActionNode[_] =>
              List(Norm.Act(act))
          }
        }
        val norms = traverseNids(rootsOriginal.toList)
        val roots = pushNodes(forceNorms(norms))
        GenTransaction(nodes, ImmArray(roots))
    }
  }

  // Types which ensure we can only represent the properly normalized cases.
  object Canonical {

    // A properly normalized Tx/node
    sealed trait Norm
    object Norm {

      // A non-rollback tx/node
      final case class Act(node: ActNode) extends Norm

      // A *normalized* rollback tx/node. 2 cases:
      // - rollback containing a single non-rollback tx/node.
      // - rollback of 2 or more tx/nodes, such that first and last are not rollbacks.
      sealed trait Roll extends Norm
      final case class Roll1(act: Act) extends Roll
      final case class Roll2(head: Act, middle: List[Node], tail: Act) extends Roll
    }

    // Case analysis on a list of Norms, distinuishing: Empty, Single and Multi forms
    // The Multi form separes the head and tail element for the middle-list.
    sealed trait Case
    object Case {
      final case object Empty extends Case
      final case class Single(n: Norm) extends Case
      final case class Multi(h: Norm, m: List[Norm], t: Norm) extends Case
    }

    def caseNorms(xs: List[Norm]): Case = {
      xs.length match {
        case 0 => Case.Empty
        case 1 => Case.Single(xs(0))
        case n => Case.Multi(xs(0), xs.slice(1, n - 1), xs(n - 1))
      }
    }
  }
}
