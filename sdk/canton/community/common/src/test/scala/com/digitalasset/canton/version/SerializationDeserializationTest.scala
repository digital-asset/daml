// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{SymmetricKey, TestHash}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.computeRandomnessLength
import com.digitalasset.canton.pruning.*
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.channel.{
  ConnectToSequencerChannelRequest,
  ConnectToSequencerChannelResponse,
}
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelConnectedToAllEndpoints,
  SequencerChannelMetadata,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
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
  SubscriptionRequestV2,
  TopologyStateForInitRequest,
}
import com.digitalasset.canton.topology.transaction.{
  GeneratorsTransaction,
  SignedTopologyTransaction,
  SignedTopologyTransactions,
  TopologyTransaction,
}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
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
    import com.digitalasset.canton.crypto.GeneratorsCrypto.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        testVersioned(SymmetricKey, version)

        test(StaticSynchronizerParameters, version)
        test(DynamicSynchronizerParameters, version)
        test(DynamicSequencingParameters, version)

        test(AcsCommitment, version)
        test(Verdict, version)
        test(ConfirmationResponses, version)
        testContext(TypedSignedProtocolMessageContent, version, version)
        testContext(SignedProtocolMessage, version, version)
        test(ProtocolSymmetricKey, version)

        test(LocalVerdict, version)
        testContext(EnvelopeContent, (TestHash, version), version)
        test(ConfirmationResultMessage, version)

        test(AcknowledgeRequest, version)
        test(AggregationRule, version)
        test(ClosedEnvelope, version)
        test(SequencingSubmissionCost, version)

        testVersioned(ContractMetadata, version)(
          generatorsProtocol.contractMetadataArb(canHaveEmptyKey = true)
        )
        testVersioned[SerializableContract](SerializableContract, version)(
          generatorsProtocol.serializableContractArb(canHaveEmptyKey = true)
        )

        test(ActionDescription, version)

        // Merkle tree leaves
        testContext(CommonMetadata, TestHash, version)
        testContext(ParticipantMetadata, TestHash, version)
        testContext(SubmitterMetadata, TestHash, version)
        testContext(AssignmentCommonData, TestHash, version)
        testContext(AssignmentView, TestHash, version)
        testContext(UnassignmentCommonData, TestHash, version)
        testContext(UnassignmentView, TestHash, version)

        testContext(ViewCommonData, TestHash, version)

        test(TopologyTransaction, version)
        testContext(SignedTopologyTransaction, ProtocolVersionValidation(version), version)
        testContext(SignedTopologyTransactions, ProtocolVersionValidation(version), version)

        testContext(ViewParticipantData, TestHash, version)
        test(Batch, version)
        test(SetTrafficPurchasedMessage, version)
        testContext(SubmissionRequest, MaxRequestSizeToDeserialize.NoLimit, version)
        testVersioned(SequencerConnections, version)
        testVersioned(CounterParticipantIntervalsBehind, version)
        test(GetTrafficStateForMemberRequest, version)
        // This fails, which is expected, because PartySignatures serialization is only defined on PV.dev
        // We do this on purpose to make clear that this is a work in progress and should **NOT** be merged to 3.1
        test(ExternalAuthorization, version)
        test(GetTrafficStateForMemberResponse, version)
        test(TopologyStateForInitRequest, version)
        test(SubscriptionRequest, version)
        test(SubscriptionRequestV2, version)
        if (version.isDev) {
          test(ConnectToSequencerChannelRequest, version)
          test(ConnectToSequencerChannelResponse, version)
          test(SequencerChannelMetadata, version)
          test(SequencerChannelConnectedToAllEndpoints, version)
          test(SequencerChannelSessionKey, version)
          test(SequencerChannelSessionKeyAck, version)
        }
        test(SequencedEvent, version)
        test(SignedContent, version)
        testContext(TransactionView, (TestHash, version), version)
        testContext(FullInformeeTree, (TestHash, version), version)

        // testing MerkleSeq structure with specific VersionedMerkleTree: SubmitterMetadata.
        testContext(
          MerkleSeq,
          (
            (
              TestHash,
              (bytes: ByteString) => SubmitterMetadata.fromTrustedByteString(TestHash)(bytes),
            ),
            version,
          ),
          version,
        )

        val randomnessLength = computeRandomnessLength(ExampleTransactionFactory.pureCrypto)
        testContext(LightTransactionViewTree, ((TestHash, randomnessLength), version), version)

        testContextTaggedProtocolVersion(AssignmentViewTree, TestHash, Target(version))
        testContext(
          UnassignmentViewTree,
          (TestHash, Source(ProtocolVersionValidation.PV(version))),
          version,
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
