// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

class TransactionConversionsSpec extends AnyWordSpec with Matchers {

  import TransactionConversionsSpec._
  import TransactionBuilder.Implicits._

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

  "decodeContractIdsAndKeys" should {
    "return a single output for a create without a key" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, "#1")
      builder.add(createNode)
      val result = TransactionConversions.decodeContractIdsAndKeys(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set(ContractIdOrKey.Id(createNode.coid)))
    }

    "return two outputs for a create with a key" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, "#2", hasKey = true)
      builder.add(createNode)
      val result = TransactionConversions.decodeContractIdsAndKeys(
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
      val createNode = create(builder, "#3", hasKey = true)
      builder.add(createNode)
      builder.add(exercise(builder, "#3"))
      val result = TransactionConversions.decodeContractIdsAndKeys(
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
      val exerciseNode = exercise(builder, "#4")
      builder.add(exerciseNode)
      val result = TransactionConversions.decodeContractIdsAndKeys(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set(ContractIdOrKey.Id(exerciseNode.targetCoid)))
    }

    "return two outputs for an exercise with a key" in {
      val builder = TransactionBuilder()
      val exerciseNode = exercise(builder, "#5", hasKey = true)
      builder.add(exerciseNode)
      val result = TransactionConversions.decodeContractIdsAndKeys(
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
      val fetchNode1 = fetch(builder, "#6", byKey = true)
      val fetchNode2 = fetch(builder, "#7", byKey = false)
      builder.add(fetchNode1)
      builder.add(fetchNode2)
      val result = TransactionConversions.decodeContractIdsAndKeys(
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
      builder.add(lookup(builder, "#8", found = false))
      val result = TransactionConversions.decodeContractIdsAndKeys(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set.empty)
    }

    "return no output for a successful lookup-by-key" in {
      val builder = TransactionBuilder()
      builder.add(lookup(builder, "#9", found = true))
      val result = TransactionConversions.decodeContractIdsAndKeys(
        TransactionConversions
          .encodeTransaction(builder.build())
          .fold(error => fail(error.errorMessage), identity)
      )
      result shouldBe Right(Set.empty)
    }

    "return outputs for nodes under a rollback node" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      val createNode = create(builder, "#10", hasKey = true)
      builder.add(createNode, rollback)
      val exerciseNode = exercise(builder, "#11", hasKey = true)
      builder.add(exerciseNode, rollback)
      val fetchNode = fetch(builder, "#12", byKey = true)
      builder.add(fetchNode, rollback)
      val result = TransactionConversions.decodeContractIdsAndKeys(
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
      val result = TransactionConversions.decodeContractIdsAndKeys(
        RawTransaction(ByteString.copyFromUtf8("wrong"))
      )
      result shouldBe Left(
        ConversionError.ParseError("Protocol message tag had invalid wire type.")
      )
    }

    "fail on a broken transaction version" in {
      val result = TransactionConversions.decodeContractIdsAndKeys(
        RawTransaction(
          TransactionOuterClass.Transaction.newBuilder().setVersion("???").build().toByteString
        )
      )
      result shouldBe Left(
        ConversionError.DecodeError(ValueCoder.DecodeError("Unsupported transaction version '???'"))
      )
    }

    "fail on a broken transaction node version" in {
      val result = TransactionConversions.decodeContractIdsAndKeys(
        RawTransaction(
          TransactionOuterClass.Transaction
            .newBuilder()
            .setVersion(TransactionVersion.VDev.protoValue)
            .addRoots(aRawRootNodeId.value)
            .addNodes(
              TransactionOuterClass.Node
                .newBuilder()
                .setNodeId(aRawRootNodeId.value)
                .setVersion("???")
            )
            .build()
            .toByteString
        )
      )
      result shouldBe Left(
        ConversionError.DecodeError(ValueCoder.DecodeError("Unsupported transaction version '???'"))
      )
    }

    "fail on a broken contract ID" in {
      val result = TransactionConversions.decodeContractIdsAndKeys(
        RawTransaction(
          TransactionOuterClass.Transaction
            .newBuilder()
            .setVersion(TransactionVersion.VDev.protoValue)
            .addRoots(aRawRootNodeId.value)
            .addNodes(
              TransactionOuterClass.Node
                .newBuilder()
                .setNodeId(aRawRootNodeId.value)
                .setVersion(TransactionVersion.VDev.protoValue)
                .setExercise(
                  TransactionOuterClass.NodeExercise
                    .newBuilder()
                    .setContractIdStruct(
                      ValueOuterClass.ContractId.newBuilder().setContractId("???")
                    )
                )
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

  private val aChildNode = TransactionOuterClass.Node
    .newBuilder()
    .setNodeId(aRawChildNodeId.value)
    .setVersion(TransactionVersion.VDev.protoValue)
    .setExercise(
      TransactionOuterClass.NodeExercise
        .newBuilder()
        .addObservers(aChildObserver)
        .setArgUnversioned(aUnitArg)
        .setChoice(aChoiceId)
        .setConsuming(true)
        .setContractIdStruct(
          ValueOuterClass.ContractId.newBuilder().setContractId(aTargetContractId.coid)
        )
        .setTemplateId(aProtoTemplateId)
    )
  private val aChildExerciseNode = exercise(Set(aChildObserver), ImmArray.empty)
  private val aGlobalKey = GlobalKey(aTemplateId, aUnitValue)
  private val aRawChildNode = RawTransaction.Node(aChildNode.build().toByteString)
  private val aRootExerciseNode = exercise(Set(aRootObserver), ImmArray(aChildNodeId))
  private val aRootNode = TransactionOuterClass.Node
    .newBuilder()
    .setNodeId(aRawRootNodeId.value)
    .setVersion(TransactionVersion.VDev.protoValue)
    .setExercise(
      TransactionOuterClass.NodeExercise
        .newBuilder()
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
  private val aRawRootNode = RawTransaction.Node(aRootNode.build().toByteString)
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
      byInterface = None,
      version = TransactionVersion.VDev,
    )

  private def fetch(builder: TransactionBuilder, id: Value.ContractId, byKey: Boolean) =
    builder.fetch(contract = create(builder, id, hasKey = true), byKey = byKey)

  private def lookup(builder: TransactionBuilder, id: Value.ContractId, found: Boolean) =
    builder.lookupByKey(contract = create(builder, id, hasKey = true), found = found)
}
