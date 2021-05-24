// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.transaction.{NodeId, GenTransaction}
import com.daml.lf.transaction.Node.{GenNode, NodeRollback, NodeExercises, LeafOnlyActionNode}
import com.daml.lf.value.Value
import com.daml.lf.data.ImmArray

private[lf] object NormalizeRollbacks {

  type Nid = NodeId
  type Cid = Value.ContractId
  type TX = GenTransaction[Nid, Cid]
  type Node = GenNode[Nid, Cid]
  type LeafNode = LeafOnlyActionNode[Cid]
  type ExeNode = NodeExercises[Nid, Cid]

  // Normalize a transaction so rollback nodes satisfy the normalization rules.
  // see `makeRoll` below

  import Canonical.{Norm, Case, caseNorms}
  // The normalization phase works by constructing intermediate values which are
  // `Canonical` in the sense that only properly normalized nodes can be represented.
  // Although this doesn't ensure correctness, one class of bugs is avoided.

  def normalizeTx(txOriginal: TX): TX = {

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

            case exe: NodeExercises[_, _] =>
              val norms = traverseNids(exe.children.toList)
              List(Norm.Exe(exe, norms))

            case leaf: LeafOnlyActionNode[_] =>
              List(Norm.Leaf(leaf))
          }
        }
        val norms = traverseNids(rootsOriginal.toList)
        val (finalState, roots) = forceNorms(initialState, norms)
        GenTransaction(finalState.nodeMap, ImmArray(roots))
    }
  }

  // State manages a counter for node-id generation, and accumulates the nodes-map for
  // the normalized transaction

  // There is no connection between the ids in the original and normalized transaction.

  private case class State(index: Int, nodeMap: Map[Nid, Node]) {
    def next: (State, Nid) = (State(index + 1, nodeMap), NodeId(index))
    def push(nid: Nid, node: Node): (State, Nid) =
      (State(index, nodeMap = nodeMap + (nid -> node)), nid)
  }

  private val initialState = State(100, Map.empty)

  // The `force*` functions move from the world of Norms into standard transactions.

  private def forceAct(s0: State, x: Norm.Act): (State, Nid) = {
    val (s, me) = s0.next
    x match {
      case Norm.Leaf(node) =>
        s.push(me, node)
      case Norm.Exe(exe, subs) =>
        val (s1, children) = forceNorms(s, subs)
        val node = exe.copy(children = ImmArray(children))
        s1.push(me, node)
    }
  }

  private def forceRoll(s0: State, x: Norm.Roll): (State, Nid) = {
    val (s, me) = s0.next
    x match {
      case Norm.Roll1(act) =>
        val (s1, child) = forceAct(s, act)
        val node = NodeRollback(children = ImmArray(List(child)))
        s1.push(me, node)
      case Norm.Roll2(h, m, t) =>
        val (s1, hh) = forceAct(s, h)
        val (s2, mm) = forceNorms(s1, m)
        val (s3, tt) = forceAct(s2, t)
        val children = List(hh) ++ mm ++ List(tt)
        val node = NodeRollback(children = ImmArray(children))
        s3.push(me, node)
    }
  }

  private def forceNorm(s: State, x: Norm): (State, Nid) = {
    x match {
      case act: Norm.Act => forceAct(s, act)
      case roll: Norm.Roll => forceRoll(s, roll)
    }
  }

  private def forceNorms(s: State, xs: List[Norm]): (State, List[Nid]) = {
    xs match {
      case Nil => (s, Nil)
      case x :: xs =>
        val (s1, y) = forceNorm(s, x)
        val (s2, ys) = forceNorms(s1, xs)
        (s2, y :: ys)
    }
  }

  // makeRoll: encodes the normalization transformation rules:
  //   rule #1: R [ ] -> ε
  //   rule #2: R [ R [ xs… ] , ys… ] -> R [ xs… ] , R [ ys… ]
  //   rule #3: R [ xs… , R [ ys… ] ] ->  R [ xs… , ys… ]

  private def makeRoll(norms: List[Norm]): List[Norm] = {
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
        List(pushIntoRoll(h, m, t))

      case Case.Multi(h: Norm.Act, m, t: Norm.Act) =>
        // no rule
        List(Norm.Roll2(h, m, t))
    }
  }

  private def pushIntoRoll(a1: Norm.Act, xs2: List[Norm], t: Norm.Roll): Norm.Roll = {
    t match {
      case Norm.Roll1(a3) => Norm.Roll2(a1, xs2, a3)
      case Norm.Roll2(a3, xs4, a5) => Norm.Roll2(a1, xs2 ++ List(a3) ++ xs4, a5)
    }
  }

  // Types which ensure we can only represent the properly normalized cases.
  private object Canonical {

    // A properly normalized Tx/node
    sealed trait Norm
    object Norm {

      // A non-rollback tx/node
      sealed trait Act extends Norm
      final case class Leaf(node: LeafNode) extends Act
      final case class Exe(node: ExeNode, children: List[Norm]) extends Act

      // A *normalized* rollback tx/node. 2 cases:
      // - rollback containing a single non-rollback tx/node.
      // - rollback of 2 or more tx/nodes, such that first and last are not rollbacks.
      sealed trait Roll extends Norm
      final case class Roll1(act: Act) extends Roll
      final case class Roll2(head: Act, middle: List[Norm], tail: Act) extends Roll
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
