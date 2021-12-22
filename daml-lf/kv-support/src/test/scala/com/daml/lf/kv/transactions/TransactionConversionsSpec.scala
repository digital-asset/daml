// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.kv.ConversionError
import com.daml.lf.kv.transactions.TransactionConversions.{
  extractNodeId,
  extractTransactionVersion,
  reconstructTransaction,
}
import com.daml.lf.transaction.{TransactionOuterClass, TransactionVersion}
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransactionConversionsSpec extends AnyWordSpec with Matchers {

  import TransactionConversionsSpec._

  "TransactionUtilsSpec" should {
    "extractTransactionVersion" in {
      val actualVersion = extractTransactionVersion(aRawTransaction)
      actualVersion shouldBe TransactionVersion.VDev.protoValue
    }

    "extractNodeId" in {
      val actualNodeId = extractNodeId(aRawRootNode)
      actualNodeId shouldBe RawTransaction.NodeId("rootId")
    }

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
  private val aRootNodeId = "rootId"
  private val aChildNodeId = "childId"
  private val aRawRootNodeId = RawTransaction.NodeId(aRootNodeId)
  private val aRawChildNodeId = RawTransaction.NodeId(aChildNodeId)

  private val aChildNode = TransactionOuterClass.Node
    .newBuilder()
    .setNodeId(aChildNodeId)
    .setExercise(
      TransactionOuterClass.NodeExercise
        .newBuilder()
        .addObservers("childObserver")
    )
  private val aRawChildNode = RawTransaction.Node(aChildNode.build().toByteString)
  private val aRootNode = TransactionOuterClass.Node
    .newBuilder()
    .setNodeId(aRootNodeId)
    .setExercise(
      TransactionOuterClass.NodeExercise
        .newBuilder()
        .addChildren(aChildNodeId)
        .addObservers("rootObserver")
    )
  private val aRawRootNode = RawTransaction.Node(aRootNode.build().toByteString)
  private val aRawTransaction = RawTransaction(
    TransactionOuterClass.Transaction
      .newBuilder()
      .addRoots(aRootNodeId)
      .addNodes(aRootNode)
      .addNodes(aChildNode)
      .setVersion(TransactionVersion.VDev.protoValue)
      .build()
      .toByteString
  )
}
