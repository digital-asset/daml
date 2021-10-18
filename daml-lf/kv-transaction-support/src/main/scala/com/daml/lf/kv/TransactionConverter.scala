// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv

import com.daml.ledger.participant.state.kvutils.store.events.TransactionNodeWitnesses
import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray, Ref}
import com.daml.lf.transaction.TransactionOuterClass.Node
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass, TransactionVersion}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object TransactionConverter {
  type RawNodeId = String

  def transactionToWitnesses(
      tx: TransactionOuterClass.Transaction
  ): mutable.ArrayBuffer[TransactionNodeWitnesses] = {
    val nodes = tx.getNodesList.iterator.asScala.map(n => n.getNodeId -> n).toMap
    val txVersion = TransactionVersion.assertFromString(tx.getVersion)

    @tailrec
    def go(
        toVisit: FrontStack[(RawNodeId, Set[Ref.Party])],
        acc: mutable.ArrayBuffer[TransactionNodeWitnesses],
    ): mutable.ArrayBuffer[TransactionNodeWitnesses] = {
      toVisit match {
        case FrontStack() => acc
        case FrontStackCons((nodeId, parentWitnesses), toVisit) =>
          val node = nodes(nodeId)
          val witnesses = parentWitnesses union informeesOfNode(txVersion, node)
          val witnessesProto = TransactionNodeWitnesses
            .newBuilder()
            .setNodeId(nodeId)
            .addAllParty(witnesses.map(_.toString).asJava)
            .build()
          // Recurse into children (if any).
          node.getNodeTypeCase match {
            case Node.NodeTypeCase.EXERCISE =>
              val next =
                node.getExercise.getChildrenList.asScala.view.map(_ -> witnesses).to(ImmArray)
              go(next ++: toVisit, acc += witnessesProto)

            case _ =>
              go(toVisit, acc += witnessesProto)
          }
      }
    }
    go(
      tx.getRootsList.asScala.view.map(_ -> Set.empty[Ref.Party]).to(FrontStack),
      mutable.ArrayBuffer.empty,
    )
  }

  private def informeesOfNode(
      txVersion: TransactionVersion,
      node: TransactionOuterClass.Node,
  ): Set[Ref.Party] =
    TransactionCoder
      .protoActionNodeInfo(txVersion, node)
      .fold(err => throw new IllegalArgumentException(err.errorMessage), _.informeesOfNode) // FIXME
}
