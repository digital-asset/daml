// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.crypto.HashUtils.HashTracer
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.TransactionSpec
import com.digitalasset.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class MetadataHashV1Spec extends AnyWordSpec with Matchers with HashUtils {

  private val transactionUUID = UUID.fromString("4c6471d3-4e09-49dd-addf-6cd90e19c583")
  private val metadata = Hash.TransactionMetadataBuilderV1.Metadata(
    actAs = ImmArray(Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    commandId = Ref.CommandId.assertFromString("command-id"),
    submissionId = Ref.SubmissionId.assertFromString("submission-id"),
    transactionUUID = transactionUUID,
    mediatorGroup = 0,
    domainId = "domainId",
    ledgerEffectiveTime = Some(Time.Timestamp.Epoch),
    submissionTime = Time.Timestamp.Epoch,
    submissionSeed =
      Hash.assertFromString("d70605b7a7398f79c0aaa7a280ac0fa7ca079dce3f9a8f1f1d1044f82822591e"),
    disclosedContracts = ImmArray(
      TransactionSpec.dummyCreateNode(
        Value.ContractId.assertFromString(
          "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
        )
      ),
      TransactionSpec.dummyCreateNode(
        Value.ContractId.assertFromString(
          "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
        )
      ),
    ),
  )

  "Metadata Encoding" should {
    val defaultHash = Hash
      .fromString("dc4d4e50d93603f3c7d624889b71ef2ea06c55bec187ada5dbee89c1a7046924")
      .getOrElse(fail("Invalid hash"))
    "be stable" in {
      Hash.hashTransactionMetadataV1(metadata) shouldBe defaultHash
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer(true)
      Hash.hashTransactionMetadataV1(metadata, hashTracer) shouldBe defaultHash
      hashTracer.result shouldBe """'01' # 01 (Node Encoding Version)
                                   |# Act As Parties
                                   |'00000002' # 2 (int)
                                   |'00000005' # 5 (int)
                                   |'616c696365' # alice (string)
                                   |'00000003' # 3 (int)
                                   |'626f62' # bob (string)
                                   |# Command Id
                                   |'0000000a' # 10 (int)
                                   |'636f6d6d616e642d6964' # command-id (string)
                                   |# Submission Id
                                   |'0000000d' # 13 (int)
                                   |'7375626d697373696f6e2d6964' # submission-id (string)
                                   |# Transaction UUID
                                   |'00000024' # 36 (int)
                                   |'34633634373164332d346530392d343964642d616464662d366364393065313963353833' # 4c6471d3-4e09-49dd-addf-6cd90e19c583 (string)
                                   |# Mediator Group
                                   |'00000000' # 0 (int)
                                   |# Domain Id
                                   |'00000008' # 8 (int)
                                   |'646f6d61696e4964' # domainId (string)
                                   |# Ledger Effective Time
                                   |'01' # Some
                                   |'0000000000000000' # 0 (long)
                                   |# Submission Time
                                   |'0000000000000000' # 0 (long)
                                   |# Submission Seed
                                   |'d70605b7a7398f79c0aaa7a280ac0fa7ca079dce3f9a8f1f1d1044f82822591e' # Submission Seed Hash
                                   |# Disclosed Contracts
                                   |'00000002' # 2 (int)
                                   |  '00' # 00 (Value Encoding Version)
                                   |  '07' # 07 (Value Encoding Purpose)
                                   |  '01' # 01 (Node Encoding Version)
                                   |  # Create Node
                                   |  # Node Version
                                   |  '00000003' # 3 (int)
                                   |  '322e31' # 2.1 (string)
                                   |  '00' # Node Tag
                                   |  # Contract Id
                                   |  '00000021' # 33 (int)
                                   |  '0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                   |  # Package Name
                                   |  '00000007' # 7 (int)
                                   |  '506b674e616d65' # PkgName (string)
                                   |  # Template Id
                                   |  '0000000a' # 10 (int)
                                   |  '2d64756d6d79506b672d' # -dummyPkg- (string)
                                   |  '00000001' # 1 (int)
                                   |  '0000000b' # 11 (int)
                                   |  '44756d6d794d6f64756c65' # DummyModule (string)
                                   |  '00000001' # 1 (int)
                                   |  '00000009' # 9 (int)
                                   |  '64756d6d794e616d65' # dummyName (string)
                                   |  # Arg
                                   |  '00000021' # 33 (int)
                                   |  '0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd' # 0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd (contractId)
                                   |  # Signatories
                                   |  '00000000' # 0 (int)
                                   |  # Stakeholders
                                   |  '00000000' # 0 (int)
                                   |'794e6290ed0ac0d35e73ca09b099c147cc364f4d244ec125ed853159b4d4a389' # Disclosed Contract
                                   |  '00' # 00 (Value Encoding Version)
                                   |  '07' # 07 (Value Encoding Purpose)
                                   |  '01' # 01 (Node Encoding Version)
                                   |  # Create Node
                                   |  # Node Version
                                   |  '00000003' # 3 (int)
                                   |  '322e31' # 2.1 (string)
                                   |  '00' # Node Tag
                                   |  # Contract Id
                                   |  '00000021' # 33 (int)
                                   |  '0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b' # 0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b (contractId)
                                   |  # Package Name
                                   |  '00000007' # 7 (int)
                                   |  '506b674e616d65' # PkgName (string)
                                   |  # Template Id
                                   |  '0000000a' # 10 (int)
                                   |  '2d64756d6d79506b672d' # -dummyPkg- (string)
                                   |  '00000001' # 1 (int)
                                   |  '0000000b' # 11 (int)
                                   |  '44756d6d794d6f64756c65' # DummyModule (string)
                                   |  '00000001' # 1 (int)
                                   |  '00000009' # 9 (int)
                                   |  '64756d6d794e616d65' # dummyName (string)
                                   |  # Arg
                                   |  '00000021' # 33 (int)
                                   |  '0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd' # 0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd (contractId)
                                   |  # Signatories
                                   |  '00000000' # 0 (int)
                                   |  # Stakeholders
                                   |  '00000000' # 0 (int)
                                   |'bfa0daa76c3aff7e62aeb31e940e0d386903a5a85f4b8f2edb1c331a99bbfb4f' # Disclosed Contract
                                   |""".stripMargin
      assertStringTracer(hashTracer, defaultHash)
    }
  }

}
