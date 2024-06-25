// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package test

import com.digitalasset.daml.lf.data._

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.HashMap

class NodeIdTransactionBuilder extends TestIdFactory {

  private val ids: Iterator[NodeId] = Iterator.from(0).map(NodeId)
  private var nodes: Map[NodeId, Node] = HashMap.empty
  private var children: Map[NodeId, BackStack[NodeId]] =
    HashMap.empty.withDefaultValue(BackStack.empty)
  private var roots: BackStack[NodeId] = BackStack.empty

  private[this] def newNode(node: Node): NodeId = {
    val nodeId = ids.next()
    nodes += (nodeId -> node)
    nodeId
  }

  def add(node: Node): NodeId = ids.synchronized {
    val nodeId = newNode(node)
    roots = roots :+ nodeId
    nodeId
  }

  def add(node: Node, parentId: NodeId): NodeId = ids.synchronized {
    lazy val nodeId = newNode(node) // lazy to avoid getting the next id if the method later throws
    nodes(parentId) match {
      case _: Node.Exercise | _: Node.Rollback =>
        children += parentId -> (children(parentId) :+ nodeId)
      case _ =>
        throw new IllegalArgumentException(
          s"Node ${parentId.index} either does not exist or is not an exercise or rollback"
        )
    }
    nodeId
  }

  def build(): VersionedTransaction = ids.synchronized {
    import TransactionVersion.Ordering
    val finalNodes = nodes.transform {
      case (nid, rb: Node.Rollback) =>
        rb.copy(children = children(nid).toImmArray)
      case (nid, exe: Node.Exercise) =>
        exe.copy(children = children(nid).toImmArray)
      case (_, node: Node.LeafOnlyAction) =>
        node
    }
    val txVersion = finalNodes.values.foldLeft(TransactionVersion.minVersion)((acc, node) =>
      node.optVersion match {
        case Some(version) => acc max version
        case None => acc
      }
    )
    val finalRoots = roots.toImmArray
    VersionedTransaction(txVersion, finalNodes, finalRoots)
  }

  def buildSubmitted(): SubmittedTransaction = SubmittedTransaction(build())

  def buildCommitted(): CommittedTransaction = CommittedTransaction(build())

}
