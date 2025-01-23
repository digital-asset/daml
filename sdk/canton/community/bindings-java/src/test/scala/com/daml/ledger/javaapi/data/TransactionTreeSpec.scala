// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.OptionValues.convertOptionToValuable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.jdk.CollectionConverters.*

class TransactionTreeSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "TransactionTree.buildTree" should "convert a transaction tree to a wrapped tree" in forAll(
    transactionTreeGenWithIdsInPreOrder
  ) { transactionTreeOuter =>
    val transactionTree = TransactionTree.fromProto(transactionTreeOuter)

    case class WrappedEvent(nodeId: Int, children: List[WrappedEvent]) {

      def descendants(): Seq[WrappedEvent] =
        Seq(this) ++ children ++ children.flatMap(_.descendants())

      def lastDescendantNodeId(): Int = descendants().map(_.nodeId).maxOption.getOrElse(nodeId)
    }

    val wrappedTree: Seq[WrappedEvent] = TransactionTreeUtils
      .buildTree(
        transactionTree,
        (treeEvent: TreeEvent, children: java.util.List[WrappedEvent]) =>
          WrappedEvent(treeEvent.getNodeId, children.asScala.toList),
      )
      .asScala
      .toSeq

    transactionTree.getRootNodeIds.asScala shouldBe wrappedTree.map(_.nodeId)

    val wrappedEventsById =
      wrappedTree.flatMap(_.descendants()).map(event => event.nodeId -> event).toMap

    transactionTree.getEventsById.asScala.values
      .map(_.getNodeId) shouldBe wrappedEventsById.keys.toList.sorted

    transactionTree.getEventsById.asScala.foreach { case (nodeId, treeEvent: TreeEvent) =>
      treeEvent.getLastDescendantNodeId shouldBe wrappedEventsById
        .get(nodeId)
        .value
        .lastDescendantNodeId()
    }
  }
}
