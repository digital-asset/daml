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
      val event = treeEvent.toProtoTreeEvent
      val lastDescendantNodeId =
        if (event.hasExercised) event.getExercised.getLastDescendantNodeId else nodeId.toInt

      lastDescendantNodeId shouldBe wrappedEventsById
        .get(nodeId)
        .value
        .lastDescendantNodeId()
    }
  }

  "TransactionTree.getRootNodeIds" should "provide root node ids that are not descendant of others" in forAll(
    transactionTreeGenWithIdsInPreOrder
  ) { transactionTreeOuter =>
    val transactionTree = TransactionTree.fromProto(transactionTreeOuter)
    val eventDescendantsRanges =
      transactionTree.getEventsById.asScala.view.mapValues(_.toProtoTreeEvent).map {
        case (id, event) =>
          (id, if (event.hasExercised) Int.box(event.getExercised.getLastDescendantNodeId) else id)
      }

    transactionTree.getRootNodeIds.asScala.foreach(nodeid =>
      eventDescendantsRanges.exists { case (start, end) =>
        nodeid > start && nodeid <= end
      } shouldBe false
    )
  }
}
