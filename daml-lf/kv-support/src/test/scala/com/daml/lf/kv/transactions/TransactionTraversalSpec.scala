// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.TransactionOuterClass.{
  KeyWithMaintainers,
  Node,
  NodeLookupByKey,
  NodeRollback,
  Transaction,
}
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.google.protobuf.ByteString
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class TransactionTraversalSpec extends AnyFunSuite with Matchers {

  test("traverseTransactionWithWitnesses - consuming nested exercises") {
    val testTransaction = RawTransactionForTest()
    TransactionTraversal.traverseTransactionWithWitnesses(testTransaction.rawTx) {
      case (RawTransaction.NodeId(testTransaction.`createNid`), _, witnesses) =>
        witnesses should contain.only("Alice", "Bob", "Charlie")
        ()
      case (RawTransaction.NodeId(testTransaction.`exeNid`), _, witnesses) =>
        witnesses should contain.only("Alice", "Charlie")
        ()
      case (RawTransaction.NodeId(testTransaction.`nonConsumingExeNid`), _, witnesses) =>
        // Non-consuming exercises are only witnessed by signatories.
        witnesses should contain only "Alice"
        ()
      case (RawTransaction.NodeId(testTransaction.`rootNid`), _, witnesses) =>
        witnesses should contain only "Alice"
        ()
      case (RawTransaction.NodeId(testTransaction.`fetchNid`), _, witnesses) =>
        // This is of course ill-authorized, but we check that parent witnesses are included.
        witnesses should contain.only("Alice", "Bob")
        ()
      case what =>
        fail(s"Traversed to unknown node: $what")
    } shouldBe Right(())
  }

  test("extractPerPackageWitnesses - extract package witness mapping as expected") {
    val testTransaction = RawTransactionForTest()
    val result = TransactionTraversal.extractPerPackageWitnesses(testTransaction.rawTx)
    result shouldBe
      Right(
        Map(
          "template_exercise" -> Set("Alice", "Charlie"),
          "interface_exercise" -> Set("Alice", "Charlie"),
          "template_create" -> Set("Alice", "Charlie", "Bob"),
          "template_fetch" -> Set("Alice", "Bob"),
        )
      )
  }

  test(
    "extractPerPackageWitnesses - extract package witness mapping as expected including rollback node"
  ) {
    val testTransaction = RawTransactionForTest()
    def lookupByKeyNode(index: Int) = withNodeBuilder { builder =>
      builder.setLookupByKey(
        NodeLookupByKey.newBuilder
          .setTemplateId(identifierForTemplateId(s"template_lookup_by_key$index"))
          .setKeyWithMaintainers(
            KeyWithMaintainers.newBuilder().addMaintainers(s"LookupByKey$index Party")
          )
      )
    }
    val transactionBuilder = testTransaction.builder
    transactionBuilder.addRoot(
      exerciseNode(
        signatories = Seq("Exercise with Rollback Node Party"),
        stakeholders = Seq.empty,
        consuming = false,
        children = transactionBuilder.addNode(
          buildRollbackNodeWithChild(
            transactionBuilder.addNode(lookupByKeyNode(0)),
            transactionBuilder.addNode(
              buildRollbackNodeWithChild(transactionBuilder.addNode(lookupByKeyNode(1)))
            ),
          )
        ),
      )
    )
    val result = TransactionTraversal.extractPerPackageWitnesses(testTransaction.rawTx)
    result shouldBe
      Right(
        Map(
          "template_exercise" -> Set("Alice", "Charlie", "Exercise with Rollback Node Party"),
          "interface_exercise" -> Set("Alice", "Charlie", "Exercise with Rollback Node Party"),
          "template_create" -> Set("Alice", "Charlie", "Bob"),
          "template_fetch" -> Set("Alice", "Bob"),
          "template_lookup_by_key0" -> Set(
            "LookupByKey0 Party",
            "Exercise with Rollback Node Party",
          ),
          "template_lookup_by_key1" -> Set(
            "LookupByKey1 Party",
            "Exercise with Rollback Node Party",
          ),
        )
      )
  }

  private def buildRollbackNodeWithChild(children: String*) = {
    withNodeBuilder { builder =>
      builder.setRollback(
        NodeRollback.newBuilder.addAllChildren(children.asJava)
      )
    }
  }
  test("traversal - transaction parsing error") {
    val rawTx = RawTransaction(ByteString.copyFromUtf8("wrong"))
    TransactionTraversal.traverseTransactionWithWitnesses(rawTx)((_, _, _) => ()) shouldBe Left(
      ConversionError.ParseError("Protocol message tag had invalid wire type.")
    )
    TransactionTraversal.extractPerPackageWitnesses(rawTx) shouldBe Left(
      ConversionError.ParseError("Protocol message tag had invalid wire type.")
    )
  }

  test("traversal - transaction version parsing error") {
    val rawTx = RawTransaction(Transaction.newBuilder().setVersion("wrong").build.toByteString)
    val actual = TransactionTraversal.traverseTransactionWithWitnesses(rawTx)((_, _, _) => ())
    actual shouldBe Left(ConversionError.ParseError("Unsupported transaction version 'wrong'"))
  }

  test("traversal - decode error on rollback nodes") {
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
    TransactionTraversal.traverseTransactionWithWitnesses(rawTx)((_, _, _) => ()) shouldBe Left(
      ConversionError.DecodeError(
        ValueCoder.DecodeError(
          "protoActionNodeInfo only supports action nodes but was applied to a rollback node"
        )
      )
    )
  }

  // --------------------------------------------------------
  // Helpers for constructing transactions.
  case class RawTransactionForTest() {
    val builder: TransactionBuilder = TransactionBuilder()

    // Creation of a contract with Alice as signatory, and Bob is controller of one of the choices.
    val createNid: String = builder.addNode(
      createNode(List("Alice"), List("Alice", "Bob"))
    )

    // Exercise of a contract where Alice as signatory, Charlie has a choice or is an observer.
    val exeNid: String = builder.addNode(
      exerciseNode(
        signatories = List("Alice"),
        stakeholders = List("Alice", "Charlie"),
        consuming = true,
        createNid,
      )
    )

    // Non-consuming exercise of a contract where Alice as signatory, Charlie has a choice or is an observer.
    val nonConsumingExeNid: String = builder.addNode(
      exerciseNode(
        signatories = List("Alice"),
        stakeholders = List("Alice", "Charlie"),
        consuming = false,
      )
    )

    // A fetch of some contract created by Bob.
    val fetchNid: String = builder.addNode(
      fetchNode(
        signatories = List("Bob"),
        stakeholders = List("Bob"),
      )
    )

    // Root node exercising a contract only known to Alice.
    val rootNid: String = builder.addRoot(
      exerciseNode(
        signatories = List("Alice"),
        stakeholders = List("Alice"),
        consuming = true,
        fetchNid,
        nonConsumingExeNid,
        exeNid,
      )
    )

    def rawTx: RawTransaction = RawTransaction(builder.build.toByteString)
  }

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
        .setTemplateId(identifierForTemplateId("template_create"))
        .addAllSignatories(signatories.asJava)
        .addAllStakeholders(stakeholders.asJava)
    }

  /** Construct a fetch node. Signatories are the signatories of the contract we're fetching.
    * Stakeholders of a contract are the signatories and observers.
    */
  private def fetchNode(signatories: Iterable[String], stakeholders: Iterable[String]) =
    withNodeBuilder {
      _.getFetchBuilder
        .setTemplateId(identifierForTemplateId("template_fetch"))
        .addAllSignatories(signatories.asJava)
        .addAllStakeholders(stakeholders.asJava)
    }

  /** Construct an exercise node. Signatories are the signatories of the contract we're exercising
    * on.
    */
  private def exerciseNode(
      signatories: Seq[String],
      stakeholders: Seq[String],
      consuming: Boolean,
      children: String*
  ) =
    withNodeBuilder {
      _.getExerciseBuilder
        .setConsuming(consuming)
        .setTemplateId(identifierForTemplateId("template_exercise"))
        .setInterfaceId(identifierForTemplateId("interface_exercise"))
        .addAllSignatories(signatories.asJava)
        .addAllStakeholders(stakeholders.asJava)
        /* NOTE(JM): Actors are no longer included in exercises by the compiler, hence we don't set them */
        .addAllChildren(children.asJava)
    }

  private def identifierForTemplateId = ValueOuterClass.Identifier.newBuilder().setPackageId _
}
