// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Trampoline.{Bounce, Land, Trampoline}
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.Node.{GenNode, LeafOnlyActionNode, NodeExercises, NodeRollback}

// Simple tree representation for transactions. No node-ids and mapping here!

// A sub transaction-tree (TxTree); a node and it's children.
// Ties the recursive knot via the Nid type parameter of GenNode.
final case class TxTree[Cid](node: GenNode[TxTree[Cid], Cid])

// A full transaction (TxTreeTop) is a forest of trees
final case class TxTreeTop[Cid](roots: ImmArray[TxTree[Cid]]) {

  // The final tx has increasing node-ids when nodes are listed in pre-order.
  // The root node-id is 0 (we have tests that rely on this)
  def toStandardTransaction: GenTransaction[NodeId, Cid] = {
    pushTrees(initialState, roots.toList) { (finalState, roots) =>
      Land(GenTransaction(finalState.nodeMap, ImmArray(roots)))
    }.bounce
  }

  private[this] type Tree = TxTree[Cid]
  private[this] type Nid = NodeId
  private[this] type Node = GenNode[Nid, Cid]

  // State manages a counter for node-id generation, and accumulates the nodes-map
  private[this] case class State(index: Int, nodeMap: Map[Nid, Node]) {

    def next[R](k: (State, Nid) => Trampoline[R]): Trampoline[R] = {
      k(State(index + 1, nodeMap), NodeId(index))
    }

    def push[R](nid: Nid, node: Node)(k: (State, Nid) => Trampoline[R]): Trampoline[R] = {
      k(State(index, nodeMap = nodeMap + (nid -> node)), nid)
    }

  }

  private val initialState = State(0, Map.empty)

  private[this] def pushTree[R](s: State, x: Tree)(
      k: (State, Nid) => Trampoline[R]
  ): Trampoline[R] = {
    x match {
      case TxTree(node) =>
        s.next { (s, me) =>
          node match {

            case leaf: LeafOnlyActionNode[_] =>
              s.push(me, leaf)(k)

            case exe: NodeExercises[_, _] =>
              pushTrees(s, exe.children.toList) { (s, children) =>
                val node = exe.copy(children = ImmArray(children))
                s.push(me, node)(k)
              }

            case rb: NodeRollback[_] =>
              pushTrees(s, rb.children.toList) { (s, children) =>
                val node = rb.copy(children = ImmArray(children))
                s.push(me, node)(k)
              }
          }
        }
    }
  }

  private[this] def pushTrees[R](s: State, xs: List[Tree])(
      k: (State, List[Nid]) => Trampoline[R]
  ): Trampoline[R] = {
    Bounce { () =>
      xs match {
        case Nil => k(s, Nil)
        case x :: xs =>
          pushTree(s, x) { (s, y) =>
            pushTrees(s, xs) { (s, ys) =>
              Bounce { () =>
                k(s, y :: ys)
              }
            }
          }
      }
    }
  }
}
