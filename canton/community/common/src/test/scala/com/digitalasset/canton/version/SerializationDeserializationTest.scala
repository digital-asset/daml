// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  AggregationRule,
  Batch,
  ClosedEnvelope,
  GeneratorsProtocol as GeneratorsProtocolSequencing,
  MaxRequestSizeToDeserialize,
  SubmissionRequest,
}
import com.digitalasset.canton.topology.transaction.{
  GeneratorsTransaction,
  SignedTopologyTransactionX,
  TopologyTransactionX,
}
import com.digitalasset.canton.{BaseTest, SerializationDeserializationTestHelpers}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {
  import com.digitalasset.canton.sequencing.GeneratorsSequencing.*

  forAll(Table("protocol version", ProtocolVersion.supported*)) { version =>
    val generatorsProtocol = new GeneratorsProtocol(version)
    val generatorsData =
      new GeneratorsData(version, generatorsProtocol)
    val generatorsTransaction = new GeneratorsTransaction(version, generatorsProtocol)
    val generatorsLocalVerdict = GeneratorsLocalVerdict(version)
    val generatorsVerdict = GeneratorsVerdict(version, generatorsLocalVerdict)
    val generatorsMessages = new GeneratorsMessages(
      version,
      generatorsData,
      generatorsProtocol,
      generatorsLocalVerdict,
      generatorsVerdict,
    )
    val generatorsProtocolSeq = new GeneratorsProtocolSequencing(
      version,
      generatorsMessages,
    )
    val generatorsTransferData = new GeneratorsTransferData(
      version,
      generatorsProtocol,
      generatorsProtocolSeq,
    )

    import generatorsData.*
    import generatorsMessages.*
    import generatorsTransferData.*
    import generatorsVerdict.*
    import generatorsLocalVerdict.*
    import generatorsProtocol.*
    import generatorsProtocolSeq.*
    import generatorsTransaction.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        testProtocolVersioned(StaticDomainParameters)
        testProtocolVersioned(DynamicDomainParameters)

        testProtocolVersioned(AcsCommitment)
        testProtocolVersioned(Verdict)
        testProtocolVersioned(ConfirmationResponse)
        testMemoizedProtocolVersionedWithCtx(TypedSignedProtocolMessageContent, version)
        testProtocolVersionedWithCtx(SignedProtocolMessage, version)

        testProtocolVersioned(LocalVerdict)
        testProtocolVersioned(TransferResult)
        testProtocolVersioned(MalformedConfirmationRequestResult)
        testProtocolVersionedWithCtx(EnvelopeContent, (TestHash, version))
        testMemoizedProtocolVersioned(TransactionResultMessage)

        testProtocolVersioned(AcknowledgeRequest)
        testProtocolVersioned(AggregationRule)
        testProtocolVersioned(ClosedEnvelope)

        testVersioned(ContractMetadata)(
          generatorsProtocol.contractMetadataArb(canHaveEmptyKey = true)
        )
        testVersioned[SerializableContract](SerializableContract)(
          generatorsProtocol.serializableContractArb(canHaveEmptyKey = true)
        )

        testProtocolVersioned(ActionDescription)

        // Merkle tree leaves
        testMemoizedProtocolVersionedWithCtx(CommonMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(ParticipantMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(SubmitterMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferInCommonData, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferInView, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferOutCommonData, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferOutView, TestHash)

        testMemoizedProtocolVersionedWithCtx(
          ViewCommonData,
          (TestHash, ConfirmationPolicy.Signatory),
        )

        testMemoizedProtocolVersioned(TopologyTransactionX)
        testProtocolVersionedWithCtx(
          SignedTopologyTransactionX,
          ProtocolVersionValidation(version),
        )

        testMemoizedProtocolVersionedWithCtx(
          com.digitalasset.canton.data.ViewParticipantData,
          TestHash,
        )
        testProtocolVersioned(Batch)
        testProtocolVersioned(SetTrafficBalanceMessage)
        testMemoizedProtocolVersionedWithCtx(
          SubmissionRequest,
          MaxRequestSizeToDeserialize.NoLimit,
        )
        testVersioned(com.digitalasset.canton.sequencing.SequencerConnections)
      }

    }
  }

  "be exhaustive" in {
    val requiredTests = {
      findHasProtocolVersionedWrapperSubClasses("com.digitalasset.canton.protocol")
        ++ findHasProtocolVersionedWrapperSubClasses("com.digitalasset.canton.topology")
    }

    val missingTests = requiredTests.diff(testedClasses.toList)

    /*
        If this test fails, it means that one class inheriting from HasProtocolVersionWrapper in the
        package is not tested in the SerializationDeserializationTests
     */
    clue(s"Missing tests should be empty but found: $missingTests")(missingTests shouldBe empty)
  }
}
