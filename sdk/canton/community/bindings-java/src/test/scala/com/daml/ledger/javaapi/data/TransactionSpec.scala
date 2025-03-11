// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.Generators.*
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.jdk.CollectionConverters.*

class TransactionSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with ScalaCheckDrivenPropertyChecks {

  "Transaction.buildTree" should "convert a transaction to a wrapped tree" in forAll(
    transactionGenWithIdsInPreOrder
  ) { transactionOuter =>
    val transaction = Transaction.fromProto(transactionOuter)

    case class WrappedEvent(nodeId: Int, children: List[WrappedEvent]) {

      def descendants(): Seq[WrappedEvent] =
        Seq(this) ++ children ++ children.flatMap(_.descendants())

      def lastDescendantNodeId(): Int = descendants().map(_.nodeId).maxOption.getOrElse(nodeId)
    }

    val wrappedTree: Seq[WrappedEvent] = TransactionUtils
      .buildTree(
        transaction,
        (event: Event, children: java.util.List[WrappedEvent]) =>
          WrappedEvent(event.getNodeId, children.asScala.toList),
      )
      .asScala
      .toSeq

    transaction.getRootNodeIds.asScala shouldBe wrappedTree.map(_.nodeId)

    val wrappedEventsById =
      wrappedTree.flatMap(_.descendants()).map(event => event.nodeId -> event).toMap

    wrappedEventsById.values.foreach { wrappedEvent =>
      wrappedEvent.children shouldBe wrappedEvent.children.distinct
      all(wrappedEvent.children.map(_.nodeId)) should be > wrappedEvent.nodeId
    }

    transaction.getEventsById.asScala.values
      .map(_.getNodeId) shouldBe wrappedEventsById.keys.toList.sorted

    transaction.getEventsById.asScala.foreach { case (nodeId, event: Event) =>
      val eventOuter = event.toProtoEvent
      val lastDescendantNodeId =
        if (eventOuter.hasExercised) eventOuter.getExercised.getLastDescendantNodeId
        else nodeId.toInt

      lastDescendantNodeId shouldBe wrappedEventsById
        .get(nodeId)
        .value
        .lastDescendantNodeId()
    }
  }

  "Transaction.getRootNodeIds" should "provide root node ids that are not descendant of others" in forAll(
    transactionGenWithIdsInPreOrder
  ) { transactionOuter =>
    val transaction = Transaction.fromProto(transactionOuter)
    val eventDescendantsRanges =
      transaction.getEventsById.asScala.view.mapValues(_.toProtoEvent).map { case (id, event) =>
        (id, if (event.hasExercised) Int.box(event.getExercised.getLastDescendantNodeId) else id)
      }

    transaction.getRootNodeIds.asScala.foreach(nodeid =>
      eventDescendantsRanges.exists { case (start, end) =>
        nodeid > start && nodeid <= end
      } shouldBe false
    )
  }

  "Transaction.getChildNodeIds" should "find the children node ids of exercised events" in forAll(
    transactionGenWithIdsInPreOrder
  ) { transactionOuter =>
    val transaction = Transaction.fromProto(transactionOuter)

    case class WrappedEvent(nodeId: Int, children: List[WrappedEvent]) {

      def descendants(): Seq[WrappedEvent] =
        Seq(this) ++ children ++ children.flatMap(_.descendants())
    }

    val wrappedTree: Seq[WrappedEvent] = TransactionUtils
      .buildTree(
        transaction,
        (event: Event, children: java.util.List[WrappedEvent]) =>
          WrappedEvent(event.getNodeId, children.asScala.toList),
      )
      .asScala
      .toSeq

    val wrappedEventsById =
      wrappedTree.flatMap(_.descendants()).map(event => event.nodeId -> event).toMap

    val events = transaction.getEventsById.asScala.values

    val exercisedEvents =
      events
        .filter(_.toProtoEvent.hasExercised)
        .map(_.toProtoEvent.getExercised)
        .map(ExercisedEvent.fromProto)

    exercisedEvents.foreach { event =>
      transaction
        .getChildNodeIds(event)
        .asScala shouldBe wrappedEventsById
        .get(event.getNodeId)
        .value
        .children
        .map(_.nodeId)
    }
  }
}
