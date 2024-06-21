// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package test

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.{Node, NodeId, Transaction}

import scala.language.implicitConversions
import TreeTransactionBuilder._

import java.util.concurrent.atomic.AtomicInteger

trait TreeTransactionBuilder {

  def nextNodeId(): NodeId

  def toVersionedTransaction(roots: NodeWrapper*): VersionedTransaction =
    nodesToTx(roots).toVersionedTransaction

  def toCommittedTransaction(roots: NodeWrapper*): CommittedTransaction =
    CommittedTransaction(toVersionedTransaction(roots: _*))

  def toTransaction(roots: NodeWrapper*): Transaction = nodesToTx(roots).toTransaction

  private def nodesToTx(wrappers: Seq[NodeWrapper]): Tx =
    wrappers.map(nodeToTx).fold(Tx.empty)(combine)

  private def nodeToTx(wrapper: NodeWrapper): Tx = {
    val nodeId = nextNodeId()
    wrapper match {
      case Leaf(node) =>
        Tx(Map(nodeId -> node), Seq(nodeId))

      case Parent(builder, children) =>
        val childrenTx = nodesToTx(children)
        Tx(childrenTx.nodes + (nodeId -> builder(childrenTx.roots)), Seq(nodeId))
    }
  }

  private def combine(tx1: Tx, tx2: Tx): Tx = Tx(tx1.nodes ++ tx2.nodes, tx1.roots ++ tx2.roots)

}

object TreeTransactionBuilder extends TreeTransactionBuilder {

  private val atomic = new AtomicInteger()

  override def nextNodeId(): NodeId = NodeId(atomic.getAndIncrement())

  private final case class Tx(nodes: Map[NodeId, Node], roots: Seq[NodeId]) {
    private def version =
      nodes.values.flatMap(_.optVersion).headOption.getOrElse(TransactionVersion.VDev)
    def toTransaction: Transaction =
      Transaction(nodes, ImmArray.from(roots))
    def toVersionedTransaction: VersionedTransaction =
      VersionedTransaction(version, nodes, ImmArray.from(roots))
  }
  private object Tx {
    val empty: Tx = Tx(Map.empty, Seq.empty)
  }

  sealed trait NodeWrapper
  final case class Leaf(node: Node) extends NodeWrapper
  final case class Parent(builder: Seq[NodeId] => Node, children: Seq[NodeWrapper])
      extends NodeWrapper

  implicit def leaf(node: Node): NodeWrapper = node match {
    case exercise: Node.Exercise if exercise.children.isEmpty => Leaf(exercise)
    case leaf: Node.LeafOnlyAction => Leaf(leaf)
    case _ => throw new IllegalArgumentException(s"Node $node is not supported $node as leaf")
  }

  implicit class NodeOps(node: Node) {
    def withChildren(children: NodeWrapper*): NodeWrapper = node match {
      case n: Node.Exercise if n.children.isEmpty =>
        Parent(c => n.copy(children = ImmArray.from(c)), children)

      case n: Node.Rollback if n.children.isEmpty =>
        Parent(c => n.copy(children = ImmArray.from(c)), children)

      case _ => throw new IllegalArgumentException(s"Node $node is not supported $node as parent")
    }
  }

}
