// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.SafeProto
import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray}
import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.daml.lf.transaction.{
  GlobalKey,
  NodeId,
  TransactionCoder,
  TransactionOuterClass,
  VersionedTransaction,
}
import com.daml.lf.value.{Value, ValueCoder}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object TransactionConversions {

  def encodeTransaction(
      tx: VersionedTransaction
  ): Either[ValueCoder.EncodeError, RawTransaction] =
    for {
      msg <-
        TransactionCoder.encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, tx)
      bytes <- SafeProto.toByteString(msg).left.map(ValueCoder.EncodeError(_))
    } yield RawTransaction(bytes)

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
  ): Either[ConversionError, RawTransaction] = {
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
      .flatMap(_ =>
        SafeProto.toByteString(transactionBuilder.build()) match {
          case Right(bytes) =>
            Right(RawTransaction(bytes))
          case Left(msg) =>
            Left(ConversionError.EncodeError(ValueCoder.EncodeError(msg)))
        }
      )
  }

  /** Decodes and extracts outputs of a submitted transaction, that is the IDs and keys of contracts created or updated
    * by processing a submission.
    *
    * The results, among the others, may include contract IDs of transient contracts (created and archived within the same transaction),
    * contract IDs that may cause divulgence and keys that are not modified. Actual outputs must be a subset of,
    * or the same as, computed outputs and we currently relax this check by widening the latter set,
    * treating a node the same regardless of whether it was a child of a rollback node or not, for example.
    * Computed outputs that are not actual outputs can be safely trimmed.
    */
  def extractTransactionOutputs(
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
                      case NodeTypeCase.ROLLBACK =>
                        // Nodes under a rollback node may potentially produce outputs such as divulgence.
                        Right(contractIdsOrKeys)

                      case NodeTypeCase.CREATE =>
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

                      case NodeTypeCase.EXERCISE =>
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

                      case NodeTypeCase.FETCH =>
                        // A fetch may cause divulgence, which is why the target contract is a potential output.
                        ValueCoder.CidDecoder.decode(node.getFetch.getContractIdStruct).map {
                          contractId => contractIdsOrKeys + ContractIdOrKey.Id(contractId)
                        }

                      case NodeTypeCase.LOOKUP_BY_KEY =>
                        // Contract state only modified on divulgence, in which case we'll have a fetch node,
                        // so no outputs from lookup node.
                        Right(contractIdsOrKeys)

                      case NodeTypeCase.NODETYPE_NOT_SET =>
                        Left(ValueCoder.DecodeError("NODETYPE_NOT_SET not supported"))
                    }
                  }
                case (Left(error), _) => Left(error)
              }
          }
          .left
          .map(ConversionError.DecodeError)
    }

  /** Removes `Fetch`, `LookupByKey` and `Rollback` nodes (including their children) from a transaction tree. */
  def keepCreateAndExerciseNodes(
      rawTransaction: RawTransaction
  ): Either[ConversionError, RawTransaction] =
    Try(TransactionOuterClass.Transaction.parseFrom(rawTransaction.byteString)) match {
      case Failure(throwable) => Left(ConversionError.ParseError(throwable.getMessage))
      case Success(transaction) =>
        val nodes = transaction.getNodesList.asScala
        val nodeMap: Map[String, TransactionOuterClass.Node] =
          nodes.view.map(n => n.getNodeId -> n).toMap

        @tailrec
        def goNodesToKeep(
            toVisit: FrontStack[String],
            result: Set[String],
        ): Either[ConversionError, Set[String]] = toVisit match {
          case FrontStack() => Right(result)
          case FrontStackCons(nodeId, previousToVisit) =>
            nodeMap.get(nodeId) match {
              case Some(node) =>
                node.getNodeTypeCase match {
                  case NodeTypeCase.CREATE =>
                    goNodesToKeep(previousToVisit, result + nodeId)
                  case NodeTypeCase.EXERCISE =>
                    goNodesToKeep(
                      node.getExercise.getChildrenList.asScala.to(ImmArray) ++: previousToVisit,
                      result + nodeId,
                    )
                  case NodeTypeCase.ROLLBACK | NodeTypeCase.FETCH | NodeTypeCase.LOOKUP_BY_KEY |
                      NodeTypeCase.NODETYPE_NOT_SET =>
                    goNodesToKeep(previousToVisit, result)
                }
              case None =>
                Left(ConversionError.InternalError(s"Invalid transaction node id $nodeId"))
            }
        }

        goNodesToKeep(transaction.getRootsList.asScala.to(FrontStack), Set.empty).flatMap {
          nodesToKeep =>
            val filteredRoots = transaction.getRootsList.asScala.filter(nodesToKeep)

            val filteredNodes = nodes.collect {
              case node if nodesToKeep(node.getNodeId) =>
                if (node.hasExercise) {
                  val exerciseNode = node.getExercise
                  val keptChildren =
                    exerciseNode.getChildrenList.asScala.filter(nodesToKeep)
                  val newExerciseNode = exerciseNode.toBuilder
                    .clearChildren()
                    .addAllChildren(keptChildren.asJavaCollection)
                    .build()

                  node.toBuilder
                    .setExercise(newExerciseNode)
                    .build()
                } else {
                  node
                }
            }

            val newTransaction = transaction
              .newBuilderForType()
              .addAllRoots(filteredRoots.asJavaCollection)
              .addAllNodes(filteredNodes.asJavaCollection)
              .setVersion(transaction.getVersion)
              .build()

            SafeProto.toByteString(newTransaction) match {
              case Right(bytes) =>
                Right(RawTransaction(bytes))
              case Left(msg) =>
                // Should not happen as removing nodes should results into a smaller transaction.
                Left(ConversionError.InternalError(msg))
            }
        }
    }
}

final case class TransactionNodeIdWithNode(
    nodeId: RawTransaction.NodeId,
    node: RawTransaction.Node,
)

sealed abstract class ContractIdOrKey extends Product with Serializable
object ContractIdOrKey {
  final case class Id(id: Value.ContractId) extends ContractIdOrKey
  final case class Key(key: GlobalKey) extends ContractIdOrKey
}
