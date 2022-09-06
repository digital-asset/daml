// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.TransactionOuterClass.{Node, NodeRollback, Transaction}
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.ValueCoder
import com.google.protobuf.ByteString
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class TransactionTraversalSpec extends AnyFunSuite with Matchers {

  test("traverseTransactionWithWitnesses - consuming nested exercises") {
    val builder = TransactionBuilder()

    // Creation of a contract with Alice as signatory, and Bob is controller of one of the choices.
    val createNid = builder.addNode(
      createNode(List("Alice"), List("Alice", "Bob"))
    )

    // Exercise of a contract where Alice as signatory, Charlie has a choice or is an observer.
    val exeNid = builder.addNode(
      exerciseNode(
        signatories = List("Alice"),
        stakeholders = List("Alice", "Charlie"),
        consuming = true,
        createNid,
      )
    )

    // Non-consuming exercise of a contract where Alice as signatory, Charlie has a choice or is an observer.
    val nonConsumingExeNid = builder.addNode(
      exerciseNode(
        signatories = List("Alice"),
        stakeholders = List("Alice", "Charlie"),
        consuming = false,
      )
    )

    // A fetch of some contract created by Bob.
    val fetchNid = builder.addNode(
      fetchNode(
        signatories = List("Bob"),
        stakeholders = List("Bob"),
      )
    )

    // Root node exercising a contract only known to Alice.
    val rootNid = builder.addRoot(
      exerciseNode(
        signatories = List("Alice"),
        stakeholders = List("Alice"),
        consuming = true,
        fetchNid,
        nonConsumingExeNid,
        exeNid,
      )
    )
    val rawTx = RawTransaction(builder.build.toByteString)

    TransactionTraversal.traverseTransactionWithWitnesses(rawTx) {
      case (RawTransaction.NodeId(`createNid`), _, witnesses) =>
        witnesses should contain.only("Alice", "Bob", "Charlie")
        ()
      case (RawTransaction.NodeId(`exeNid`), _, witnesses) =>
        witnesses should contain.only("Alice", "Charlie")
        ()
      case (RawTransaction.NodeId(`nonConsumingExeNid`), _, witnesses) =>
        // Non-consuming exercises are only witnessed by signatories.
        witnesses should contain only "Alice"
        ()
      case (RawTransaction.NodeId(`rootNid`), _, witnesses) =>
        witnesses should contain only "Alice"
        ()
      case (RawTransaction.NodeId(`fetchNid`), _, witnesses) =>
        // This is of course ill-authorized, but we check that parent witnesses are included.
        witnesses should contain.only("Alice", "Bob")
        ()
      case what =>
        fail(s"Traversed to unknown node: $what")
    } shouldBe Right(())
  }

  test("traverseTransactionWithWitnesses - transaction parsing error") {
    val rawTx = RawTransaction(ByteString.copyFromUtf8("wrong"))
    val actual = TransactionTraversal.traverseTransactionWithWitnesses(rawTx)((_, _, _) => ())
    actual shouldBe Left(ConversionError.ParseError("Protocol message tag had invalid wire type."))
  }

  test("traverseTransactionWithWitnesses - transaction version parsing error") {
    val rawTx = RawTransaction(Transaction.newBuilder().setVersion("wrong").build.toByteString)
    val actual = TransactionTraversal.traverseTransactionWithWitnesses(rawTx)((_, _, _) => ())
    actual shouldBe Left(ConversionError.ParseError("Unsupported transaction version 'wrong'"))
  }

  test("traverseTransactionWithWitnesses - node decoding error") {
    val rootNodeId = "1"
    val rawTx = RawTransaction(
      Transaction
        .newBuilder()
        .setVersion(TransactionVersion.VDev.protoValue)
        .addNodes(
          Node.newBuilder().setNodeId(rootNodeId).setRollback(NodeRollback.getDefaultInstance)
        )
        .addRoots(rootNodeId)
        .build
        .toByteString
    )
    val actual = TransactionTraversal.traverseTransactionWithWitnesses(rawTx)((_, _, _) => ())
    actual shouldBe Left(
      ConversionError.DecodeError(
        ValueCoder.DecodeError(
          "protoActionNodeInfo only supports action nodes but was applied to a rollback node"
        )
      )
    )
  }

  // --------------------------------------------------------
  // Helpers for constructing transactions.

  case class TransactionBuilder() {
    private var roots = List.empty[String]
    private var nodes = Map.empty[String, Node]
    private var nextNodeId = -1

    def addNode(node: Node.Builder): String = {
      nextNodeId += 1
      val nodeId = nextNodeId.toString
      nodes += nodeId -> node.setNodeId(nodeId).build
      nodeId
    }

    def addRoot(node: Node.Builder): String = {
      // Check that children actually exist.
      assert(
        !node.hasExercise ||
          node.getExercise.getChildrenList.asScala.map(nodes.contains).forall(identity)
      )
      val nodeId = addNode(node)
      roots ::= nodeId
      nodeId
    }

    def build: Transaction =
      Transaction.newBuilder
        .setVersion(TransactionVersion.minVersion.protoValue)
        .addAllNodes(nodes.values.asJava)
        .addAllRoots(roots.reverse.asJava)
        .build
  }

  private def withNodeBuilder[A](build: Node.Builder => A): Node.Builder = {
    val b = Node.newBuilder
    build(b)
    b
  }

  /** Construct a create node. Signatories are the parties whose signature is required to create the
    * contract. Stakeholders of a contract are the signatories and observers.
    */
  private def createNode(signatories: Iterable[String], stakeholders: Iterable[String]) =
    withNodeBuilder {
      _.getCreateBuilder
        .addAllSignatories(signatories.asJava)
        .addAllStakeholders(stakeholders.asJava)
    }

  /** Construct a fetch node. Signatories are the signatories of the contract we're fetching.
    * Stakeholders of a contract are the signatories and observers.
    */
  private def fetchNode(signatories: Iterable[String], stakeholders: Iterable[String]) =
    withNodeBuilder {
      _.getFetchBuilder
        .addAllSignatories(signatories.asJava)
        .addAllStakeholders(stakeholders.asJava)
    }

  /** Construct an exercise node. Signatories are the signatories of the contract we're exercising
    * on.
    */
  private def exerciseNode(
      signatories: Iterable[String],
      stakeholders: Iterable[String],
      consuming: Boolean,
      children: String*
  ) =
    withNodeBuilder {
      _.getExerciseBuilder
        .setConsuming(consuming)
        .addAllSignatories(signatories.asJava)
        .addAllStakeholders(stakeholders.asJava)
        /* NOTE(JM): Actors are no longer included in exercises by the compiler, hence we don't set them */
        .addAllChildren(children.asJava)
    }

}
