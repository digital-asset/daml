// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{SymmetricKey, TestHash}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.participant.GeneratorsParticipant
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationSourceParticipantMessage,
  PartyReplicationTargetParticipantMessage,
}
import com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.computeRandomnessLength
import com.digitalasset.canton.pruning.*
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.channel.{
  ConnectToSequencerChannelRequest,
  ConnectToSequencerChannelResponse,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelConnectedToAllEndpoints,
  SequencerChannelMetadata,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.store.VersionedStatus
import com.digitalasset.canton.synchronizer.sequencer.traffic.LSUTrafficState
import com.digitalasset.canton.synchronizer.sequencer.{
  GeneratorsSequencer,
  OnboardingStateForSequencer,
  OnboardingStateForSequencerV2,
  SequencerSnapshot,
}
import com.digitalasset.canton.synchronizer.sequencing.integrations.state.DbSequencerStateManagerStore.AggregatedSignaturesOfSender
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  SignedTopologyTransactions,
  TopologyTransaction,
}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString
import org.scalacheck.Arbitrary
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** Because we do exhaustiveness check via class listing, this test needs to be in the project which
  * has as many dependencies as possible.
  */
final class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {

  forAll(Table("protocol version", ProtocolVersion.supported*)) { version =>
    val generators = new CommonGenerators(version)
    val sequencerGenerators =
      new GeneratorsSequencer(
        version,
        generators.topology,
        generators.transaction,
        generators.generatorsProtocolSeq,
        generators.protocol,
      )
    val participantGenerators =
      new GeneratorsParticipant(generators.topology, generators.lf, generators.protocol, version)

    import com.digitalasset.canton.crypto.GeneratorsCrypto.*
    import generators.data.*
    import generators.generatorsMessages.*
    import generators.generatorsProtocolSeq.*
    import generators.generatorsSequencing.*
    import generators.localVerdict.*
    import generators.protocol.*
    import generators.trafficData.*
    import generators.transaction.*
    import generators.verdict.*
    import sequencerGenerators.*
    import participantGenerators.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        testVersioned(SymmetricKey, version)

        test(StaticSynchronizerParameters, version)
        test(DynamicSynchronizerParameters, version)
        test(DynamicSequencingParameters, version)

        test(AcsCommitment, version)
        test(Verdict, version)
        test(ConfirmationResponses, version)
        testContext(
          TypedSignedProtocolMessageContent,
          ProtocolVersionValidation.PV(version),
          version,
        )
        testContext(SignedProtocolMessage, ProtocolVersionValidation.PV(version), version)
        test(ProtocolSymmetricKey, version)
        test(LocalVerdict, version)
        testContext(
          EnvelopeContent,
          (TestHash, version),
          version,
        )
        test(ConfirmationResultMessage, version)

        test(AcknowledgeRequest, version)
        test(AggregationRule, version)
        test(ClosedEnvelope, version)
        test(SequencingSubmissionCost, version)
        testVersioned(ContractMetadata, version)(
          generators.protocol.contractMetadataArb(canHaveEmptyKey = true)
        )
        testVersioned[SerializableContract](SerializableContract, version)(
          generators.protocol.serializableContractArb(canHaveEmptyKey = true)
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
        test(UnassignmentData, version)
        testContext(ViewCommonData, TestHash, version)

        testContext(RootHashMessage, SerializedRootHashMessagePayload.fromByteString, version)
        testContext(AssignmentMediatorMessage, (TestHash, Target(version)), version)(
          assignmentMediatorMessageArb
        )
        testContext(
          UnassignmentMediatorMessage,
          (TestHash, Source(ProtocolVersionValidation.PV(version))),
          version,
        )(
          unassignmentMediatorMessageArb
        )
        // InformeeMessage become large due to the embedded ExternalAuthorization (quadratic list)
        // on top of transaction view trees, so give this test more time.
        testContext(InformeeMessage, (TestHash, version), version)(informeeMessageArb)
        test(EncryptedViewMessage, version)(encryptedViewMessage)

        test(TopologyTransaction, version)
        testContext(TopologyTransactionsBroadcast, version, version)
        testContext(SignedTopologyTransaction, ProtocolVersionValidation(version), version)
        testContext(SignedTopologyTransactions, ProtocolVersionValidation(version), version)
        test(SequencerSnapshot, version)
        test(OnboardingStateForSequencer, version)
        test(OnboardingStateForSequencerV2, version)
        test(LSUTrafficState, version)

        testContext(ViewParticipantData, TestHash, version)
        // the generated recipient trees can be quite big, even they are already limited
        testContext(Batch, defaultMaxBytesToDecompress, version)
        test(SetTrafficPurchasedMessage, version)
        testContext(
          SubmissionRequest,
          defaultMaxBytesToDecompress,
          version,
        )
        testVersioned(SequencerConnections, version)
        testVersioned(CounterParticipantIntervalsBehind, version)
        test(GetTrafficStateForMemberRequest, version)
        // This fails, which is expected, because PartySignatures serialization is only defined on PV.dev
        // We do this on purpose to make clear that this is a work in progress and should **NOT** be merged to 3.1
        test(ExternalAuthorization, version)
        test(GetTrafficStateForMemberResponse, version)
        test(TopologyStateForInitRequest, version)
        test(SubscriptionRequest, version)
        if (version.isDev) {
          test(ConnectToSequencerChannelRequest, version)
          test(ConnectToSequencerChannelResponse, version)
          test(SequencerChannelMetadata, version)
          test(SequencerChannelConnectedToAllEndpoints, version)
          test(SequencerChannelSessionKey, version)
          test(SequencerChannelSessionKeyAck, version)

          test(PartyReplicationStatus, version)
          test(PartyReplicationSourceParticipantMessage, version)
          test(PartyReplicationTargetParticipantMessage, version)
        }
        test(ActiveContractOld, version)

        // Generated sequenced events get quite big because each batched envelope has recipient trees
        // of quadratic size breadth * depth, so this test takes longer than other tests.
        testContext(SequencedEvent, defaultMaxBytesToDecompress, version)
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

        test(SubmissionTrackingData, version)

        test(VersionedStatus, version)
        test(AggregatedSignaturesOfSender, version)
      }
    }
  }

  "be exhaustive" in {
    val unfilteredRequiredTests = findBaseVersioningCompanionSubClasses()

    val ignoreAnnotation = classOf[IgnoreInSerializationTestExhaustivenessCheck]
    val requiredTests =
      unfilteredRequiredTests.filterNot(ignoreAnnotation.isAssignableFrom).map(_.getName).toSet

    val exceptions = Set(
      // TODO(#26599) Remove this exception
      "com.digitalasset.canton.participant.admin.data.ActiveContract$",

      // TODO(#26601) Remove these exceptions
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistLeaderSelectionPolicyState$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability$RemoteDissemination$RemoteBatch$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability$RemoteDissemination$RemoteBatchAcknowledged$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability$RemoteOutputFetch$RemoteBatchDataFetched$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability$RemoteOutputFetch$FetchRemoteBatchData$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus$EpochStatus$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus$StateTransferMessage$BlockTransferRequest$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus$StateTransferMessage$BlockTransferResponse$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment$ConsensusMessage$Commit$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment$ConsensusMessage$NewView$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment$ConsensusMessage$PrePrepare$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment$ConsensusMessage$Prepare$",
      "com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment$ConsensusMessage$ViewChange$",

      // TODO(#26726) Remove this exception
      "com.digitalasset.canton.data.MerkleSeq$MerkleSeqElement$",
    )

    val untestedClasses = requiredTests.diff(testedClasses)
    val missingTests = untestedClasses.diff(exceptions)
    /*
        If this test fails, it means that one class inheriting from HasProtocolVersionWrapper in the
        package is not tested in the SerializationDeserializationTests
     */
    clue(s"Missing tests should be empty but found: $missingTests")(missingTests shouldBe empty)

    // Check that all exceptions are actually needed
    val unnecessaryExceptions = exceptions.diff(untestedClasses)
    clue(s"Unnecessary exceptions should be removed") {
      unnecessaryExceptions shouldBe empty
    }
  }

  forAll(Table("contract ID version", CantonContractIdVersion.all*)) { version =>
    val generatorContract = new GeneratorsContract(version)
    import generatorContract.*

    s"Serialization and deserialization methods for contract ID version $version" should {
      "compose to the identity" in {
        testContractVersion[ContractAuthenticationData](
          "ContractAuthenticationData to LfBytes",
          _.toLfBytes.toByteString,
          bytes => ContractAuthenticationData.fromLfBytes(version, Bytes.fromByteString(bytes)),
        )

        testContractVersion[ContractAuthenticationData](
          "ContractAuthenticationData to v30.SerializableContractId",
          _.toSerializableContractProtoV30,
          ContractAuthenticationData.fromSerializableContractProtoV30(version, _),
        )

        testContractVersion[ContractAuthenticationData](
          "ContractAuthenticationData to admin.SerializableContractId",
          _.toSerializableContractAdminProtoV30,
          ContractAuthenticationData.fromSerializableContractAdminProtoV30(version, _),
        )
      }
    }

  }

  private def testContractVersion[T](
      name: String,
      toByteString: T => ByteString,
      fromByteString: ByteString => ParsingResult[T],
  )(implicit arb: Arbitrary[T]): Unit =
    forAll { (instance: T) =>
      val byteString = toByteString(instance)

      val deserializedInstance = clue(s"Deserializing serialized $name")(
        fromByteString(byteString).value
      )

      withClue(s"Comparing $name with (de)serialization") {
        instance shouldBe deserializedInstance
      }
    }

}
