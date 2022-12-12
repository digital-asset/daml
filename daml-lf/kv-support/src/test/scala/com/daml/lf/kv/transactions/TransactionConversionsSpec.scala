// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.kv.ConversionError
import com.daml.lf.kv.transactions.TransactionConversions.{
  extractNodeId,
  extractTransactionVersion,
  reconstructTransaction,
}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{
  GlobalKey,
  Node,
  NodeId,
  TransactionOuterClass,
  TransactionVersion,
  VersionedTransaction,
}
import com.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.google.protobuf
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class TransactionConversionsSpec extends AnyWordSpec with Matchers {

  import TransactionConversionsSpec._

  private val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("#id"))

  "TransactionUtilsSpec" should {
    "encodeTransaction" in {
      TransactionConversions.encodeTransaction(aVersionedTransaction) shouldBe Right(
        aRawTransaction
      )
    }

    "extractTransactionVersion" in {
      val actualVersion = extractTransactionVersion(aRawTransaction)
      actualVersion shouldBe TransactionVersion.VDev.protoValue
    }

    "extractNodeId" in {
      val actualNodeId = extractNodeId(aRawRootNode)
      actualNodeId shouldBe RawTransaction.NodeId("1")
    }
  }

  "decodeTransaction" should {
    "successfully decode a transaction" in {
      TransactionConversions.decodeTransaction(aRawTransaction) shouldBe Right(
        aVersionedTransaction
      )
    }

    "fail to decode a non-parsable transaction" in {
      TransactionConversions.decodeTransaction(
        RawTransaction(ByteString.copyFromUtf8("wrong"))
      ) shouldBe Left(
        ConversionError.ParseError("Protocol message tag had invalid wire type.")
      )
    }
  }

  "reconstructTransaction" should {
    "successfully reconstruct a transaction" in {
      val reconstructedTransaction = reconstructTransaction(
        TransactionVersion.VDev.protoValue,
        Seq(
          TransactionNodeIdWithNode(aRawRootNodeId, aRawRootNode),
          TransactionNodeIdWithNode(aRawChildNodeId, aRawChildNode),
        ),
      )
      reconstructedTransaction shouldBe Right(aRawTransaction)
    }

    "fail to reconstruct a non-parsable transaction" in {
      val reconstructedTransaction = reconstructTransaction(
        TransactionVersion.VDev.protoValue,
        Seq(
          TransactionNodeIdWithNode(
            aRawRootNodeId,
            RawTransaction.Node(ByteString.copyFromUtf8("wrong")),
          )
        ),
      )
      reconstructedTransaction shouldBe Left(
        ConversionError.ParseError("Protocol message tag had invalid wire type.")
      )
    }
  }

  "extractTransactionOutputs" should {
    "return a single output for a create without a key" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, cid)
      builder.add(createNode)
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set(ContractIdOrKey.Id(createNode.coid)))
    }

    "return two outputs for a create with a key" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, cid, hasKey = true)
      builder.add(createNode)
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(
        Set(
          ContractIdOrKey.Id(createNode.coid),
          ContractIdOrKey.Key(aGlobalKey),
        )
      )
    }

    "return a single output for a transient contract" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, cid, hasKey = true)
      builder.add(createNode)
      builder.add(exercise(builder, cid))
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(
        Set(
          ContractIdOrKey.Id(createNode.coid),
          ContractIdOrKey.Key(aGlobalKey),
        )
      )
    }

    "return a single output for an exercise without a key" in {
      val builder = TransactionBuilder()
      val exerciseNode = exercise(builder, cid)
      builder.add(exerciseNode)
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set(ContractIdOrKey.Id(exerciseNode.targetCoid)))
    }

    "return two outputs for a consuming exercise with a key" in {
      val builder = TransactionBuilder()
      val exerciseNode = exercise(builder, cid, hasKey = true)
      builder.add(exerciseNode)
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(
        Set(
          ContractIdOrKey.Id(exerciseNode.targetCoid),
          ContractIdOrKey.Key(aGlobalKey),
        )
      )
    }

    "return two outputs for a non-consuming exercise with a key" in {
      val builder = TransactionBuilder()
      val exerciseNode = exercise(builder, cid, hasKey = true, consuming = false)
      builder.add(exerciseNode)
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(
        Set(
          ContractIdOrKey.Id(exerciseNode.targetCoid),
          ContractIdOrKey.Key(aGlobalKey),
        )
      )
    }

    "return one output per fetch and fetch-by-key" in {
      val builder = TransactionBuilder()
      val fetchNode1 =
        fetch(builder, Value.ContractId.V1(crypto.Hash.hashPrivateKey("#id1")), byKey = true)
      val fetchNode2 =
        fetch(builder, Value.ContractId.V1(crypto.Hash.hashPrivateKey("#id2")), byKey = false)
      builder.add(fetchNode1)
      builder.add(fetchNode2)
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(
        Set(
          ContractIdOrKey.Id(fetchNode1.coid),
          ContractIdOrKey.Id(fetchNode2.coid),
        )
      )
    }

    "return no output for a failing lookup-by-key" in {
      val builder = TransactionBuilder()
      builder.add(lookup(builder, cid, found = false))
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set.empty)
    }

    "return no output for a successful lookup-by-key" in {
      val builder = TransactionBuilder()
      builder.add(lookup(builder, cid, found = true))
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set.empty)
    }

    "return outputs for nodes under a rollback node" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      val createNode =
        create(builder, Value.ContractId.V1(crypto.Hash.hashPrivateKey("#id1")), hasKey = true)
      builder.add(createNode, rollback)
      val exerciseNode =
        exercise(builder, Value.ContractId.V1(crypto.Hash.hashPrivateKey("#id2")), hasKey = true)
      builder.add(exerciseNode, rollback)
      val fetchNode =
        fetch(builder, Value.ContractId.V1(crypto.Hash.hashPrivateKey("#id3")), byKey = true)
      builder.add(fetchNode, rollback)
      val result = TransactionConversions.extractTransactionOutputs(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(
        Set(
          ContractIdOrKey.Id(createNode.coid),
          ContractIdOrKey.Key(aGlobalKey),
          ContractIdOrKey.Id(exerciseNode.targetCoid),
          ContractIdOrKey.Id(fetchNode.coid),
        )
      )
    }

    "fail on a non-parsable transaction" in {
      val result = TransactionConversions.extractTransactionOutputs(
        RawTransaction(ByteString.copyFromUtf8("wrong"))
      )
      result shouldBe Left(
        ConversionError.ParseError("Protocol message tag had invalid wire type.")
      )
    }

    "fail on a broken transaction version" in {
      val result = TransactionConversions.extractTransactionOutputs(
        RawTransaction(
          TransactionOuterClass.Transaction.newBuilder().setVersion("???").build().toByteString
        )
      )
      result shouldBe Left(
        ConversionError.DecodeError(ValueCoder.DecodeError("Unsupported transaction version '???'"))
      )
    }

    "fail on a broken transaction node version" in {
      val result = TransactionConversions.extractTransactionOutputs(
        RawTransaction(
          TransactionOuterClass.Transaction
            .newBuilder()
            .setVersion(TransactionVersion.VDev.protoValue)
            .addRoots(aRawRootNodeId.value)
            .addNodes(buildProtoNode(aRawRootNodeId.value)(_.setVersion("???")))
            .build()
            .toByteString
        )
      )
      result shouldBe Left(
        ConversionError.DecodeError(ValueCoder.DecodeError("Unsupported transaction version '???'"))
      )
    }

    "fail on a broken contract ID" in {
      val result = TransactionConversions.extractTransactionOutputs(
        RawTransaction(
          TransactionOuterClass.Transaction
            .newBuilder()
            .setVersion(TransactionVersion.VDev.protoValue)
            .addRoots(aRawRootNodeId.value)
            .addNodes(
              buildProtoNode(aRawRootNodeId.value) { builder =>
                builder.setVersion(TransactionVersion.VDev.protoValue)
                builder.setExercise(
                  exerciseBuilder()
                    .setContractIdStruct(
                      ValueOuterClass.ContractId.newBuilder().setContractId("???")
                    )
                )
              }
            )
            .build()
            .toByteString
        )
      )
      result shouldBe Left(
        ConversionError.DecodeError(ValueCoder.DecodeError("cannot parse contractId \"???\""))
      )
    }
  }

  "keepCreateAndExerciseNodes" should {
    "remove `Fetch`, `LookupByKey`, and `Rollback` nodes from the transaction tree" in {
      val actual = TransactionConversions.keepCreateAndExerciseNodes(
        RawTransaction(aRichNodeTreeTransaction.toByteString)
      )

      actual match {
        case Right(rawTransaction) =>
          val transaction = TransactionOuterClass.Transaction.parseFrom(rawTransaction.byteString)
          transaction.getRootsList.asScala should contain theSameElementsInOrderAs Seq(
            "Exercise-1",
            "Create-1",
          )
          val nodes = transaction.getNodesList.asScala
          nodes.map(_.getNodeId) should contain theSameElementsInOrderAs Seq(
            "Create-1",
            "Create-2",
            "Create-3",
            "Exercise-2",
            "Exercise-1",
          )
          nodes(3).getExercise.getChildrenList.asScala should contain theSameElementsInOrderAs Seq(
            "Create-3"
          )
          nodes(4).getExercise.getChildrenList.asScala should contain theSameElementsInOrderAs Seq(
            "Create-2",
            "Exercise-2",
          )
        case Left(_) => fail("should be Right")
      }
    }

    "fail on a non-parsable transaction" in {
      val result = TransactionConversions.keepCreateAndExerciseNodes(
        RawTransaction(ByteString.copyFromUtf8("wrong"))
      )

      result shouldBe Left(
        ConversionError.ParseError("Protocol message tag had invalid wire type.")
      )
    }

    "fail on a transaction with invalid roots" in {
      val result = TransactionConversions.keepCreateAndExerciseNodes(
        RawTransaction(
          aRichNodeTreeTransaction.toBuilder.addRoots("non-existent").build().toByteString
        )
      )

      result shouldBe Left(
        ConversionError.InternalError("Invalid transaction node id non-existent")
      )
    }
  }
}

object TransactionConversionsSpec {
  private val aChildNodeId = NodeId(2)
  private val aRootNodeId = NodeId(1)
  private val aRawChildNodeId = TransactionConversions.encodeTransactionNodeId(aChildNodeId)
  private val aRawRootNodeId = TransactionConversions.encodeTransactionNodeId(aRootNodeId)

  private val aChildObserver = Ref.Party.assertFromString("childObserver")
  private val aChoiceId = Ref.Name.assertFromString("dummyChoice")
  private val aDummyName = "dummyName"
  private val aModuleName = "DummyModule"
  private val aPackageId = "-dummyPkg-"
  private val aProtoTemplateId = ValueOuterClass.Identifier
    .newBuilder()
    .setPackageId(aPackageId)
    .addModuleName(aModuleName)
    .addName(aDummyName)
  private val aRootObserver = Ref.Party.assertFromString("rootObserver")
  private val aTargetContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("dummyCoid"))
  private val aTemplateId = Ref.Identifier(
    Ref.PackageId.assertFromString(aPackageId),
    Ref.QualifiedName.assertFromString(s"$aModuleName:$aDummyName"),
  )
  private val aUnitArg =
    ValueOuterClass.Value.newBuilder().setUnit(protobuf.Empty.newBuilder()).build().toByteString
  private val aUnitValue = Value.ValueUnit

  private val aChildNode = buildProtoNode(aRawChildNodeId.value) { builder =>
    builder.setVersion(TransactionVersion.VDev.protoValue)
    builder.setExercise(
      exerciseBuilder()
        .addObservers(aChildObserver)
        .setArgUnversioned(aUnitArg)
        .setChoice(aChoiceId)
        .setConsuming(true)
        .setContractIdStruct(
          ValueOuterClass.ContractId.newBuilder().setContractId(aTargetContractId.coid)
        )
        .setTemplateId(aProtoTemplateId)
    )
  }
  private val aChildExerciseNode = exercise(Set(aChildObserver), ImmArray.empty)
  private val aGlobalKey = GlobalKey(aTemplateId, aUnitValue)
  private val aRawChildNode = RawTransaction.Node(aChildNode.toByteString)
  private val aRootExerciseNode = exercise(Set(aRootObserver), ImmArray(aChildNodeId))
  private val aRootNode = buildProtoNode(aRawRootNodeId.value) { builder =>
    builder.setVersion(TransactionVersion.VDev.protoValue)
    builder.setExercise(
      exerciseBuilder()
        .addObservers(aRootObserver)
        .setArgUnversioned(aUnitArg)
        .addChildren(aRawChildNodeId.value)
        .setChoice(aChoiceId)
        .setConsuming(true)
        .setContractIdStruct(
          ValueOuterClass.ContractId.newBuilder().setContractId(aTargetContractId.coid)
        )
        .setTemplateId(aProtoTemplateId)
    )
  }
  private val aRawRootNode = RawTransaction.Node(aRootNode.toByteString)
  private val aRawTransaction = RawTransaction(
    TransactionOuterClass.Transaction
      .newBuilder()
      .addRoots(aRawRootNodeId.value)
      .addNodes(aRootNode)
      .addNodes(aChildNode)
      .setVersion(TransactionVersion.VDev.protoValue)
      .build()
      .toByteString
  )
  private val aRichNodeTreeTransaction = {
    val roots = Seq("Exercise-1", "Fetch-1", "LookupByKey-1", "Create-1")
    val nodes: Seq[TransactionOuterClass.Node] = Seq(
      buildProtoNode("Fetch-1")(_.setFetch(fetchBuilder())),
      buildProtoNode("LookupByKey-1")(_.setLookupByKey(lookupByKeyBuilder())),
      buildProtoNode("Create-1")(_.setCreate(createBuilder())),
      buildProtoNode("LookupByKey-2")(_.setLookupByKey(lookupByKeyBuilder())),
      buildProtoNode("Fetch-2")(_.setFetch(fetchBuilder())),
      buildProtoNode("Create-2")(_.setCreate(createBuilder())),
      buildProtoNode("Fetch-3")(_.setFetch(fetchBuilder())),
      buildProtoNode("Create-3")(_.setCreate(createBuilder())),
      buildProtoNode("LookupByKey-3")(_.setLookupByKey(lookupByKeyBuilder())),
      buildProtoNode("Exercise-2")(
        _.setExercise(
          exerciseBuilder().addAllChildren(
            Seq("Fetch-3", "Create-3", "LookupByKey-3").asJava
          )
        )
      ),
      buildProtoNode("Exercise-1")(
        _.setExercise(
          exerciseBuilder().addAllChildren(
            Seq("LookupByKey-2", "Fetch-2", "Create-2", "Exercise-2").asJava
          )
        )
      ),
      buildProtoNode("Rollback-1")(
        _.setRollback(
          rollbackBuilder().addAllChildren(Seq("RollbackChild-1", "RollbackChild-2").asJava)
        )
      ),
      buildProtoNode("RollbackChild-1")(_.setCreate(createBuilder())),
      buildProtoNode("RollbackChild-2")(_.setFetch(fetchBuilder())),
    )

    TransactionOuterClass.Transaction
      .newBuilder()
      .addAllRoots(roots.asJava)
      .addAllNodes(nodes.asJava)
      .build()
  }
  private val aVersionedTransaction = VersionedTransaction(
    TransactionVersion.VDev,
    Map(aRootNodeId -> aRootExerciseNode, aChildNodeId -> aChildExerciseNode),
    ImmArray(aRootNodeId),
  )

  private def create(builder: TransactionBuilder, id: Value.ContractId, hasKey: Boolean = false) =
    builder.create(
      id = id,
      templateId = aTemplateId,
      argument = Value.ValueRecord(None, ImmArray.Empty),
      signatories = Set.empty,
      observers = Set.empty,
      key = if (hasKey) Some(aUnitValue) else None,
    )

  private def exercise(
      builder: TransactionBuilder,
      id: Value.ContractId,
      hasKey: Boolean = false,
      consuming: Boolean = true,
  ) =
    builder.exercise(
      contract = create(builder, id, hasKey),
      choice = aChoiceId,
      argument = Value.ValueRecord(None, ImmArray.Empty),
      actingParties = Set.empty,
      consuming = consuming,
      result = Some(aUnitValue),
    )

  private def exercise(choiceObservers: Set[Ref.Party], children: ImmArray[NodeId]) =
    Node.Exercise(
      targetCoid = aTargetContractId,
      templateId = aTemplateId,
      interfaceId = None,
      choiceId = aChoiceId,
      consuming = true,
      actingParties = Set.empty,
      chosenValue = aUnitValue,
      stakeholders = Set.empty,
      signatories = Set.empty,
      choiceObservers = choiceObservers,
      children = children,
      exerciseResult = None,
      key = None,
      byKey = false,
      version = TransactionVersion.VDev,
    )

  private def fetch(builder: TransactionBuilder, id: Value.ContractId, byKey: Boolean) =
    builder.fetch(contract = create(builder, id, hasKey = true), byKey = byKey)

  private def lookup(builder: TransactionBuilder, id: Value.ContractId, found: Boolean) =
    builder.lookupByKey(contract = create(builder, id, hasKey = true), found = found)

  private def buildProtoNode(nodeId: String)(
      nodeImpl: TransactionOuterClass.Node.Builder => TransactionOuterClass.Node.Builder
  ) =
    nodeImpl(TransactionOuterClass.Node.newBuilder().setNodeId(nodeId)).build()

  private def fetchBuilder() = TransactionOuterClass.NodeFetch.newBuilder()

  private def exerciseBuilder() = TransactionOuterClass.NodeExercise.newBuilder()

  private def rollbackBuilder() = TransactionOuterClass.NodeRollback.newBuilder()

  private def createBuilder() = TransactionOuterClass.NodeCreate.newBuilder()

  private def lookupByKeyBuilder() = TransactionOuterClass.NodeLookupByKey.newBuilder()
}
