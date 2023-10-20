// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.SerializationDeserializationTestHelpers.DefaultValueUntilExclusive
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.transaction.{LegalIdentityClaim, SignedTopologyTransaction}
import com.digitalasset.canton.{BaseTest, SerializationDeserializationTestHelpers}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.data.GeneratorsTransferData.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.sequencing.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.topology.transaction.GeneratorsTransaction.*
  import com.digitalasset.canton.protocol.messages.GeneratorsMessages.*
  import com.digitalasset.canton.protocol.messages.GeneratorsMessages.GeneratorsVerdict.*
  import com.digitalasset.canton.protocol.messages.GeneratorsMessages.GeneratorsLocalVerdict.*

  "Serialization and deserialization methods" should {
    "compose to the identity" in {
      testProtocolVersioned(StaticDomainParameters)
      testProtocolVersioned(com.digitalasset.canton.protocol.DynamicDomainParameters)

      testProtocolVersioned(AcsCommitment)
      testProtocolVersioned(Verdict)
      testProtocolVersioned(MediatorResponse)
      testMemoizedProtocolVersionedWithCtx(TypedSignedProtocolMessageContent, TestHash)
      testProtocolVersionedWithCtx(SignedProtocolMessage, TestHash)

      testProtocolVersioned(LocalVerdict)
      testProtocolVersioned(TransferResult)
      testProtocolVersioned(MalformedMediatorRequestResult)
      testProtocolVersionedWithCtx(EnvelopeContent, TestHash)
      testMemoizedProtocolVersionedWithCtx(TransactionResultMessage, TestHash)

      testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.AcknowledgeRequest)
      testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.AggregationRule)
      testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.ClosedEnvelope)

      testVersioned(ContractMetadata)(
        GeneratorsProtocol.contractMetadataArb(canHaveEmptyKey = true)
      )
      testVersioned[SerializableContract](
        SerializableContract,
        List(DefaultValueUntilExclusive(_.copy(contractSalt = None), ProtocolVersion.v4)),
      )(GeneratorsProtocol.serializableContractArb(canHaveEmptyKey = true))

      testProtocolVersioned(com.digitalasset.canton.data.ActionDescription)

      // Merkle tree leaves
      testMemoizedProtocolVersionedWithCtx(CommonMetadata, TestHash)
      testMemoizedProtocolVersionedWithCtx(ParticipantMetadata, TestHash)
      testMemoizedProtocolVersionedWithCtx(SubmitterMetadata, TestHash)
      testMemoizedProtocolVersionedWithCtx(TransferInCommonData, TestHash)
      testMemoizedProtocolVersionedWithCtx(TransferInView, TestHash)
      testMemoizedProtocolVersionedWithCtx(TransferOutCommonData, TestHash)
      testMemoizedProtocolVersionedWithCtx(TransferOutView, TestHash)

      Seq(ConfirmationPolicy.Vip, ConfirmationPolicy.Signatory).map { confirmationPolicy =>
        testMemoizedProtocolVersionedWithCtx(
          com.digitalasset.canton.data.ViewCommonData,
          (TestHash, confirmationPolicy),
        )
      }

      testMemoizedProtocolVersioned(SignedTopologyTransaction)
      testMemoizedProtocolVersioned(LegalIdentityClaim)

      testMemoizedProtocolVersionedWithCtx(
        com.digitalasset.canton.data.ViewParticipantData,
        TestHash,
      )
    }

    "be exhaustive" in {
      val requiredTests =
        findHasProtocolVersionedWrapperSubClasses("com.digitalasset.canton.protocol.messages")

      val missingTests = requiredTests.diff(testedClasses.toList)

      /*
        If this test fails, it means that one class inheriting from HasProtocolVersionWrapper in the
        package is not tested in the SerializationDeserializationTests
       */
      clue(s"Missing tests should be empty but found: $missingTests")(missingTests shouldBe empty)
    }
  }
}
