// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.daml.ledger.api.v2.transaction.TransactionTree
import com.daml.ledger.api.v2.transaction.TreeEvent.Kind
import com.daml.ledger.javaapi.data.Generators.*
import com.daml.ledger.javaapi.data.{
  TransactionTree as TransactionTreeJava,
  TransactionTreeUtils,
  TreeEvent,
}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.jdk.CollectionConverters.*

class TransactionTreeOpsSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with ScalaCheckDrivenPropertyChecks {

  "rootNodeIds and childNodeIds" should "find the root and children node ids of the transaction tree" in forAll(
    transactionTreeGenWithIdsInPreOrder
  ) { transactionTreeOuter =>
    import TransactionTreeOps.*

    val transactionTree = TransactionTree.fromJavaProto(transactionTreeOuter)

    case class WrappedEvent(nodeId: Int, children: List[WrappedEvent]) {
      def descendants(): Seq[WrappedEvent] =
        Seq(this) ++ children ++ children.flatMap(_.descendants())
    }

    val wrappedTree: Seq[WrappedEvent] = TransactionTreeUtils
      .buildTree(
        TransactionTreeJava.fromProto(transactionTreeOuter),
        (treeEvent: TreeEvent, children: java.util.List[WrappedEvent]) =>
          WrappedEvent(treeEvent.getNodeId, children.asScala.toList),
      )
      .asScala
      .toSeq

    transactionTree.rootNodeIds() shouldBe wrappedTree.map(_.nodeId)

    val wrappedEventsById =
      wrappedTree.flatMap(_.descendants()).map(event => event.nodeId -> event).toMap

    val events = transactionTree.eventsById.values

    val exercisedEvents = events.collect(e =>
      e.kind match {
        case Kind.Exercised(exercised) => exercised
      }
    )

    exercisedEvents.foreach { event =>
      transactionTree
        .childNodeIds(event) shouldBe wrappedEventsById
        .get(event.nodeId)
        .value
        .children
        .map(_.nodeId)
    }

  }
}
