// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.transaction.{
  NodeId,
  TransactionCoder,
  TransactionOuterClass,
  VersionedTransaction,
}
import com.daml.lf.value.ValueCoder

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object TransactionConversions {

  def encodeTransaction(
      tx: VersionedTransaction
  ): Either[ValueCoder.EncodeError, RawTransaction] =
    TransactionCoder
      .encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, tx)
      .map(transaction => RawTransaction(transaction.toByteString))

  def decodeTransaction(
      rawTx: RawTransaction
  ): Either[ValueCoder.DecodeError, VersionedTransaction] = {
    val transaction = TransactionOuterClass.Transaction.parseFrom(rawTx.byteString)
    TransactionCoder
      .decodeTransaction(
        TransactionCoder.NidDecoder,
        ValueCoder.CidDecoder,
        transaction,
      )
  }

  def encodeTransactionNodeId(nodeId: NodeId): RawTransaction.NodeId =
    RawTransaction.NodeId(nodeId.index.toString)

  def decodeTransactionNodeId(transactionNodeId: RawTransaction.NodeId): NodeId =
    NodeId(transactionNodeId.value.toInt)

  def extractTransactionVersion(rawTransaction: RawTransaction): String =
    TransactionOuterClass.Transaction.parseFrom(rawTransaction.byteString).getVersion

  def extractNodeId(rawTransactionNode: RawTransaction.Node): RawTransaction.NodeId =
    RawTransaction.NodeId(
      TransactionOuterClass.Node.parseFrom(rawTransactionNode.byteString).getNodeId
    )

  def reconstructTransaction(
      transactionVersion: String,
      nodesWithIds: Seq[TransactionNodeIdWithNode],
  ): RawTransaction = {
    // Reconstruct roots by considering the transaction nodes in order and
    // marking all child nodes as non-roots and skipping over them.
    val nonRoots = mutable.HashSet.empty[RawTransaction.NodeId]
    val txBuilder = TransactionOuterClass.Transaction.newBuilder.setVersion(transactionVersion)
    for (TransactionNodeIdWithNode(nodeId, rawNode) <- nodesWithIds) {
      val node = TransactionOuterClass.Node.parseFrom(rawNode.byteString)
      val _ = txBuilder.addNodes(node)
      if (!nonRoots.contains(nodeId)) {
        val _ = txBuilder.addRoots(nodeId.value)
      }
      if (node.hasExercise) {
        val children = node.getExercise.getChildrenList.asScala.map(RawTransaction.NodeId).toSet
        nonRoots ++= children
      }
    }
    RawTransaction(txBuilder.build.toByteString)
  }
}

final case class TransactionNodeIdWithNode(
    nodeId: RawTransaction.NodeId,
    node: RawTransaction.Node,
)
