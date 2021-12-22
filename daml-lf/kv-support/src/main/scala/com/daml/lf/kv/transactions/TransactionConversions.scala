// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.{
  GlobalKey,
  NodeId,
  TransactionCoder,
  TransactionOuterClass,
  VersionedTransaction,
}
import com.daml.lf.value.{Value, ValueCoder}

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

  def decodeContractIdsAndKeys(
      rawTransaction: RawTransaction
  ): Either[ConversionError, Set[ContractIdOrKey]] =
    Try(TransactionOuterClass.Transaction.parseFrom(rawTransaction.byteString)) match {
      case Failure(throwable) =>
        Left(ConversionError.ParseError(throwable.getMessage))
      case Success(transaction) =>
        TransactionCoder
          .decodeVersion(transaction.getVersion)
          .flatMap { txVersion =>
            transaction.getNodesList.asScala
              .foldLeft[Either[ValueCoder.DecodeError, Set[ContractIdOrKey]]](Right(Set.empty)) {
                case (Right(contractIdsOrKeys), node) =>
                  TransactionCoder.decodeNodeVersion(txVersion, node).flatMap { nodeVersion =>
                    node.getNodeTypeCase match {
                      case TransactionOuterClass.Node.NodeTypeCase.ROLLBACK =>
                        // Nodes under rollback will potentially produce outputs such as divulgence.
                        // Actual outputs must be a subset of, or the same as, computed outputs and
                        // we currently relax this check by widening the latter set, treating a node the
                        // same regardless of whether it was under a rollback node or not.
                        // Computed outputs that are not actual outputs can be safely trimmed: examples
                        // are transient contracts and, now, potentially also outputs from nodes under
                        // rollback.
                        Right(contractIdsOrKeys)

                      case TransactionOuterClass.Node.NodeTypeCase.CREATE =>
                        val protoCreate = node.getCreate
                        for {
                          newContractIdsOrKeys <- TransactionCoder
                            .nodeKey(nodeVersion, protoCreate)
                            .map {
                              case Some(key) => contractIdsOrKeys + ContractIdOrKey.Key(key)
                              case None => contractIdsOrKeys
                            }
                          contractId <- ValueCoder.CidDecoder
                            .decode(protoCreate.getContractIdStruct)
                        } yield newContractIdsOrKeys + ContractIdOrKey.Id(contractId)

                      case TransactionOuterClass.Node.NodeTypeCase.EXERCISE =>
                        val protoExercise = node.getExercise
                        for {
                          newContractIdsOrKeys <- TransactionCoder
                            .nodeKey(nodeVersion, protoExercise)
                            .map {
                              case Some(key) => contractIdsOrKeys + ContractIdOrKey.Key(key)
                              case None => contractIdsOrKeys
                            }
                          contractId <- ValueCoder.CidDecoder
                            .decode(protoExercise.getContractIdStruct)
                        } yield newContractIdsOrKeys + ContractIdOrKey.Id(contractId)

                      case TransactionOuterClass.Node.NodeTypeCase.FETCH =>
                        // A fetch may cause a divulgence, which is why the target contract is a potential output.
                        ValueCoder.CidDecoder.decode(node.getFetch.getContractIdStruct).map {
                          contractId => contractIdsOrKeys + ContractIdOrKey.Id(contractId)
                        }

                      case TransactionOuterClass.Node.NodeTypeCase.LOOKUP_BY_KEY =>
                        // Contract state only modified on divulgence, in which case we'll have a fetch node,
                        // so no outputs from lookup node.
                        Right(contractIdsOrKeys)

                      case TransactionOuterClass.Node.NodeTypeCase.NODETYPE_NOT_SET =>
                        Left(ValueCoder.DecodeError("NODETYPE_NOT_SET not supported"))
                    }
                  }
                case (Left(error), _) => Left(error)
              }
          }
          .left
          .map(ConversionError.DecodeError)
    }
}

final case class TransactionNodeIdWithNode(
    nodeId: RawTransaction.NodeId,
    node: RawTransaction.Node,
)

sealed trait ContractIdOrKey extends Product with Serializable
object ContractIdOrKey {
  final case class Id(id: Value.ContractId) extends ContractIdOrKey
  final case class Key(key: GlobalKey) extends ContractIdOrKey
}
