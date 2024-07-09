// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  AggregationRule,
  Batch,
  ClosedEnvelope,
  GeneratorsProtocol as GeneratorsProtocolSequencing,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  MaxRequestSizeToDeserialize,
  SequencedEvent,
  SequencingSubmissionCost,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitRequest,
}
import com.digitalasset.canton.topology.transaction.{
  GeneratorsTransaction,
  SignedTopologyTransaction,
  TopologyTransaction,
}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{BaseTest, SerializationDeserializationTestHelpers}
import com.google.protobuf.ByteString
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
      generatorsTransaction,
    )
    val generatorsProtocolSeq = new GeneratorsProtocolSequencing(
      version,
      generatorsMessages,
      generatorsData,
    )
    val generatorsTrafficData = new GeneratorsTrafficData(
      version
    )

    import generatorsData.*
    import generatorsMessages.*
    import generatorsTrafficData.*
    import generatorsVerdict.*
    import generatorsLocalVerdict.*
    import generatorsProtocol.*
    import generatorsProtocolSeq.*
    import generatorsTransaction.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        testProtocolVersioned(StaticDomainParameters, version)
        testProtocolVersioned(DynamicDomainParameters, version)
        testProtocolVersioned(DynamicSequencingParameters, version)

        testProtocolVersioned(AcsCommitment, version)
        testProtocolVersioned(Verdict, version)
        testProtocolVersioned(ConfirmationResponse, version)
        testMemoizedProtocolVersionedWithCtxAndValidation(
          TypedSignedProtocolMessageContent,
          version,
        )
        testProtocolVersionedAndValidation(SignedProtocolMessage, version)

        testProtocolVersioned(LocalVerdict, version)
        testProtocolVersionedWithCtxAndValidation(EnvelopeContent, TestHash, version)
        testMemoizedProtocolVersioned(ConfirmationResultMessage, version)

        testProtocolVersioned(AcknowledgeRequest, version)
        testProtocolVersioned(AggregationRule, version)
        testProtocolVersioned(ClosedEnvelope, version)
        testProtocolVersioned(SequencingSubmissionCost, version)

        testVersioned(ContractMetadata)(
          generatorsProtocol.contractMetadataArb(canHaveEmptyKey = true)
        )
        testVersioned[SerializableContract](SerializableContract)(
          generatorsProtocol.serializableContractArb(canHaveEmptyKey = true)
        )

        testProtocolVersioned(ActionDescription, version)

        // Merkle tree leaves
        testMemoizedProtocolVersionedWithCtx(CommonMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(ParticipantMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(SubmitterMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(
          TransferInCommonData,
          (TestHash, TargetProtocolVersion(version)),
        )
        testMemoizedProtocolVersionedWithCtx(TransferInView, TestHash)
        testMemoizedProtocolVersionedWithCtx(
          TransferOutCommonData,
          (TestHash, SourceProtocolVersion(version)),
        )
        testMemoizedProtocolVersionedWithCtx(TransferOutView, TestHash)

        testMemoizedProtocolVersionedWithCtx(
          ViewCommonData,
          TestHash,
        )

        testMemoizedProtocolVersioned(TopologyTransaction, version)
        testProtocolVersionedWithCtx(
          SignedTopologyTransaction,
          ProtocolVersionValidation(version),
        )

        testMemoizedProtocolVersionedWithCtx(
          ViewParticipantData,
          TestHash,
        )
        testProtocolVersioned(Batch, version)
        testProtocolVersioned(SetTrafficPurchasedMessage, version)
        testMemoizedProtocolVersionedWithCtx(
          SubmissionRequest,
          MaxRequestSizeToDeserialize.NoLimit,
        )
        testVersioned(SequencerConnections)
        testProtocolVersioned(GetTrafficStateForMemberRequest, version)
        testProtocolVersioned(GetTrafficStateForMemberResponse, version)
        testProtocolVersioned(TopologyStateForInitRequest, version)
        testProtocolVersioned(SubscriptionRequest, version)
        testMemoizedProtocolVersioned2(
          SequencedEvent,
          version,
        )
        testMemoizedProtocolVersioned2(
          SignedContent,
          version,
        )
        testProtocolVersionedWithCtx(
          TransactionView,
          (TestHash, version),
        )
        testProtocolVersionedWithCtxAndValidation(
          FullInformeeTree,
          TestHash,
          version,
        )

        // testing MerkleSeq structure with specific VersionedMerkleTree: SubmitterMetadata.
        testProtocolVersionedWithCtxAndValidation(
          MerkleSeq,
          (
            TestHash,
            (bytes: ByteString) => SubmitterMetadata.fromTrustedByteString(TestHash)(bytes),
          ),
          version,
        )

        testProtocolVersionedWithCtxAndValidation(LightTransactionViewTree, TestHash, version)

        testProtocolVersionedWithCtxAndValidationWithTargetProtocolVersion(
          TransferInViewTree,
          TestHash,
          TargetProtocolVersion(version),
        )
        testProtocolVersionedWithCtxAndValidationWithSourceProtocolVersion(
          TransferOutViewTree,
          TestHash,
          SourceProtocolVersion(version),
        )
      }
    }
  }

  "be exhaustive" in {
    val requiredTests = findHasProtocolVersionedWrapperSubClasses("com.digitalasset.canton").toSet

    val missingTests = requiredTests.diff(testedClasses)

    /*
        If this test fails, it means that one class inheriting from HasProtocolVersionWrapper in the
        package is not tested in the SerializationDeserializationTests
     */
    clue(s"Missing tests should be empty but found: $missingTests")(missingTests shouldBe empty)
  }
}
