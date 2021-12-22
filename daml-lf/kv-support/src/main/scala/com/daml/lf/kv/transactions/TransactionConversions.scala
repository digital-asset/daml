// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.{
  NodeId,
  TransactionCoder,
  TransactionOuterClass,
  VersionedTransaction,
}
import com.daml.lf.value.ValueCoder

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object TransactionConversions {

  def encodeTransaction(
      tx: VersionedTransaction
  ): Either[ValueCoder.EncodeError, RawTransaction] =
    TransactionCoder
      .encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, tx)
      .map(transaction => RawTransaction(transaction.toByteString))

  def decodeTransaction(
      rawTx: RawTransaction
  ): Either[ConversionError, VersionedTransaction] =
    Try(TransactionOuterClass.Transaction.parseFrom(rawTx.byteString)) match {
      case Success(transaction) =>
        TransactionCoder
          .decodeTransaction(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            transaction,
          )
          .left
          .map(ConversionError.DecodeError)
      case Failure(throwable) => Left(ConversionError.ParseError(throwable.getMessage))
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

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def reconstructTransaction(
      transactionVersion: String,
      nodesWithIds: Seq[TransactionNodeIdWithNode],
  ): Either[ConversionError.ParseError, RawTransaction] = {
    import scalaz.std.either._
    import scalaz.std.list._
    import scalaz.syntax.traverse._

    // Reconstruct roots by considering the transaction nodes in order and
    // marking all child nodes as non-roots and skipping over them.
    val nonRoots = mutable.HashSet.empty[RawTransaction.NodeId]
    val transactionBuilder =
      TransactionOuterClass.Transaction.newBuilder.setVersion(transactionVersion)

    nodesWithIds
      .map { case TransactionNodeIdWithNode(rawNodeId, rawNode) =>
        Try(TransactionOuterClass.Node.parseFrom(rawNode.byteString))
          .map { node =>
            transactionBuilder.addNodes(node)
            if (!nonRoots.contains(rawNodeId)) {
              transactionBuilder.addRoots(rawNodeId.value)
            }
            if (node.hasExercise) {
              val children =
                node.getExercise.getChildrenList.asScala.map(RawTransaction.NodeId).toSet
              nonRoots ++= children
            }
          }
          .toEither
          .left
          .map(throwable => ConversionError.ParseError(throwable.getMessage))
      }
      .toList
      .sequence_
      .map(_ => RawTransaction(transactionBuilder.build.toByteString))
  }
}

final case class TransactionNodeIdWithNode(
    nodeId: RawTransaction.NodeId,
    node: RawTransaction.Node,
)
