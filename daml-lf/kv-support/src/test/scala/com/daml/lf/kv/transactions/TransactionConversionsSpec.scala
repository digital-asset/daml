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
import com.daml.lf.transaction.{
  Node,
  NodeId,
  TransactionOuterClass,
  TransactionVersion,
  VersionedTransaction,
}
import com.daml.lf.value.{Value, ValueOuterClass}
import com.google.protobuf
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransactionConversionsSpec extends AnyWordSpec with Matchers {

  import TransactionConversionsSpec._

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
  private val aRootObserver = Ref.Party.assertFromString("rootObserver")
  private val aTargetContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("dummyCoid"))
  private val aTemplateId = ValueOuterClass.Identifier
    .newBuilder()
    .setPackageId(aPackageId)
    .addModuleName(aModuleName)
    .addName(aDummyName)
  private val aUnitArg =
    ValueOuterClass.Value.newBuilder().setUnit(protobuf.Empty.newBuilder()).build().toByteString

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
        .setTemplateId(aTemplateId)
    )
  private val aChildExerciseNode = createExerciseNode(Set(aChildObserver), ImmArray.empty)
  private val aRawChildNode = RawTransaction.Node(aChildNode.build().toByteString)
  private val aRootExerciseNode = createExerciseNode(Set(aRootObserver), ImmArray(aChildNodeId))
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
        .setTemplateId(aTemplateId)
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

  private def createExerciseNode(choiceObservers: Set[Ref.Party], children: ImmArray[NodeId]) =
    Node.Exercise(
      targetCoid = aTargetContractId,
      templateId = Ref.Identifier(
        Ref.PackageId.assertFromString(aPackageId),
        Ref.QualifiedName.assertFromString(s"$aModuleName:$aDummyName"),
      ),
      choiceId = aChoiceId,
      consuming = true,
      actingParties = Set.empty,
      chosenValue = Value.ValueUnit,
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
}
