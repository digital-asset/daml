// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.test.{
  TestIdFactory,
  TestNodeBuilder,
  TreeTransactionBuilder,
}
import com.digitalasset.daml.lf.transaction.{Node, NodeId, VersionedTransaction}
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class HashSchemeV3Test extends BaseTest with AnyWordSpecLike with Matchers with HashUtilsTest {

  "v3 encoding" should {

    val ids: Iterator[NodeId] = Iterator.from(0).map(NodeId.apply)
    val txBuilder = new TreeTransactionBuilder with TestNodeBuilder with TestIdFactory {
      override def nextNodeId(): NodeId = ids.next()
    }

    val create1: Node.Create = txBuilder.create(
      id = cid1,
      templateId = defRef("module", "name"),
      argument = VA.int64.inj(31380L),
      signatories = Set(alice),
    )

    val tx1: VersionedTransaction = txBuilder.toVersionedTransaction(create1)

    val seed1 =
      LfHash.assertFromString("926bbb6f341bc0092ae65d06c6e284024907148cc29543ef6bff0930f5d52c19")

    val nodeSeeds1 = Map(tx1.nodes.keys.loneElement -> seed1)

    val stableHash =
      Hash.tryFromHexString("12206a9d55a1dec26c168d06f7425e04cfc7f51c608641d732fa2d3e646f8d794d66")
    val stableTransactionHash =
      Hash.tryFromHexString("1220c63101d7e16740dad5b021c04835ec8e2ad13623916912be6a4f74388d545cc9")
    val stableMetadataHash =
      Hash.tryFromHexString("122043cfe04cf9b3c6c4f7a7bc5955ce96b5c44496c65969a84fc00b5cec74aa1edd")

    def unwrappedHex(hash: Hash): String =
      Bytes.fromByteArray(hash.unwrap.toByteArray).toHexString

    "explain hash encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = TransactionHash.tryHashTransactionWithMetadata(
        HashingSchemeVersion.V3,
        tx1,
        nodeSeeds1,
        metadata,
        hashTracer = hashTracer,
      )
      hashTracer.result shouldBe
        s"""'00000030' # Hash Purpose
           |'02' # 02 (Hashing Scheme Version)
           |'${unwrappedHex(stableTransactionHash)}' # Transaction
           |'${unwrappedHex(stableMetadataHash)}' # Metadata
           |""".stripMargin
      assertStringTracer(hashTracer, hash)
    }

    "create the expected hash" in {
      val hash = TransactionHash.tryHashTransactionWithMetadata(
        HashingSchemeVersion.V3,
        tx1,
        nodeSeeds1,
        metadata,
      )
      hash shouldBe stableHash
    }

    "create the same hash as v2 for transactions" in {
      val hashV2 = VersionedTransactionHasher.tryHashTransaction(
        HashingSchemeVersion.V2,
        tx1,
        nodeSeeds1,
      )
      val hashV3 = VersionedTransactionHasher.tryHashTransaction(
        HashingSchemeVersion.V3,
        tx1,
        nodeSeeds1,
      )
      hashV3 shouldBe hashV2
    }

    "create the expected transactions hash" in {
      val hash: Hash = VersionedTransactionHasher.tryHashTransaction(
        HashingSchemeVersion.V2,
        tx1,
        nodeSeeds1,
      )
      hash shouldBe stableTransactionHash
    }

    "include max record time in the metadata hash" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = TransactionMetadataHasher.tryHashMetadata(
        HashingSchemeVersion.V3,
        metadata,
        hashTracer = hashTracer,
      )
      hashTracer.result shouldBe
        s"""'00000030' # Hash Purpose
           |'01' # 01 (Metadata Encoding Version)
           |# Act As Parties
           |'00000002' # 2 (int)
           |'00000005' # 5 (int)
           |'616c696365' # alice (string)
           |'00000003' # 3 (int)
           |'626f62' # bob (string)
           |# Command Id
           |'0000000a' # 10 (int)
           |'636f6d6d616e642d6964' # command-id (string)
           |# Transaction UUID
           |'00000024' # 36 (int)
           |'34633634373164332d346530392d343964642d616464662d366364393065313963353833' # 4c6471d3-4e09-49dd-addf-6cd90e19c583 (string)
           |# Mediator Group
           |'00000000' # 0 (int)
           |# Synchronizer Id
           |'00000010' # 16 (int)
           |'73796e6368726f6e697a65723a3a6964' # synchronizer::id (string)
           |# Min Time Boundary
           |'01' # Some
           |'000000000000aaaa' # 43690 (long)
           |# Max Time Boundary
           |'01' # Some
           |'000000000000bbbb' # 48059 (long)
           |# Preparation Time
           |'0000000000000000' # 0 (long)
           |# Disclosed Contracts
           |'00000002' # 2 (int)
           |# Created At
           |'000000c92a69c000' # 864000000000 (long)
           |# Create Contract
           |'f6bb13796130c23e43c952b86b1583032be325dc40072c02a0279069fc3656c1' # Disclosed Contract
           |# Created At
           |'0000019254d38000' # 1728000000000 (long)
           |# Create Contract
           |'010ee30a2b17bd729bc5ccada01a62bfb7283641610feb5913fb46b2972368a4' # Disclosed Contract
           |# Max Record Time
           |'01' # Some
           |'0000025b7f3d4000' # 2592000000000 (long)
           |""".stripMargin
      assertStringTracer(hashTracer, hash)
    }

    "create the expected metadata hash" in {
      val hash = TransactionMetadataHasher.tryHashMetadata(
        HashingSchemeVersion.V3,
        metadata,
      )
      hash shouldBe stableMetadataHash
    }

  }

}
