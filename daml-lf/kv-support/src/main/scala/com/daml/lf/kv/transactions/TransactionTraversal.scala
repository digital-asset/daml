// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray, Ref}
import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.TransactionOuterClass.Node
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass, TransactionVersion}
import com.daml.lf.value.ValueCoder
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Try

object TransactionTraversal {

  // Helper to traverse the transaction, top-down, while keeping track of the
  // witnessing parties of each node.
  def traverseTransactionWithWitnesses(rawTx: RawTransaction)(
      f: (RawTransaction.NodeId, RawTransaction.Node, Set[Ref.Party]) => Unit
  ): Either[ConversionError, Unit] =
    for {
      tx <- Try(TransactionOuterClass.Transaction.parseFrom(rawTx.byteString)).toEither.left.map(
        throwable => ConversionError.ParseError(throwable.getMessage)
      )
      nodes = tx.getNodesList.iterator.asScala.map(n => n.getNodeId -> n).toMap
      txVersion <- TransactionVersion.fromString(tx.getVersion).left.map(ConversionError.ParseError)
    } yield {
      discard(
        go(
          tx.getRootsList.asScala.view
            .map(RawTransaction.NodeId(_) -> Set.empty[Ref.Party])
            .to(FrontStack)
        )
      )

      @tailrec
      def go(
          toVisit: FrontStack[(RawTransaction.NodeId, Set[Ref.Party])]
      ): Either[ConversionError, Unit] = {
        toVisit match {
          case FrontStack() => Right(())
          case FrontStackCons((nodeId, parentWitnesses), toVisit) =>
            val node = nodes(nodeId.value)
            informeesOfNode(txVersion, node) match {
              case Left(error) => Left(ConversionError.DecodeError(error))
              case Right(nodeWitnesses) =>
                val witnesses = parentWitnesses union nodeWitnesses
                f(nodeId, RawTransaction.Node(node.toByteString), witnesses)
                // Recurse into children (if any).
                node.getNodeTypeCase match {
                  case Node.NodeTypeCase.EXERCISE =>
                    val next = node.getExercise.getChildrenList.asScala.view
                      .map(RawTransaction.NodeId(_) -> witnesses)
                      .to(ImmArray)
                    go(next ++: toVisit)

                  case _ =>
                    go(toVisit)
                }
            }
        }
      }
    }

  private def informeesOfNode(
      txVersion: TransactionVersion,
      node: TransactionOuterClass.Node,
  ): Either[ValueCoder.DecodeError, Set[Ref.Party]] =
    TransactionCoder
      .protoActionNodeInfo(txVersion, node)
      .map(_.informeesOfNode)
}
