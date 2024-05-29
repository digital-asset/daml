// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.SerializationDeserializationTestHelpers.DefaultValueUntilExclusive
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{
  GeneratorsProtocol as GeneratorsProtocolSequencing,
  MaxRequestSizeToDeserialize,
}
import com.digitalasset.canton.topology.transaction.{
  GeneratorsTransaction,
  LegalIdentityClaim,
  SignedTopologyTransaction,
}
import com.digitalasset.canton.version.ProtocolVersion.{v3, v4}
import com.digitalasset.canton.{BaseTest, SerializationDeserializationTestHelpers}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.List

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {
  import com.digitalasset.canton.sequencing.GeneratorsSequencing.*

  // Continue to test the (de)serialization for protocol version 3 and 4 to support the upgrade to 2.x LTS:
  // A participant node will contain data that was serialized using an old a protocol version which then may
  // get deserialized during the upgrade. For example, there may be still contracts which use an old contract ID
  // scheme.
  forAll(Table("protocol version", (ProtocolVersion.supported ++ List(v3, v4)) *)) { version =>
    val generatorsDataTime = new GeneratorsDataTime()
    val generatorsProtocol = new GeneratorsProtocol(version, generatorsDataTime)
    val generatorsData = new GeneratorsData(version, generatorsDataTime, generatorsProtocol)
    val generatorsTransaction = new GeneratorsTransaction(version, generatorsProtocol)
    val generatorsLocalVerdict = GeneratorsLocalVerdict(version)
    val generatorsVerdict = GeneratorsVerdict(version)
    val generatorsMessages = new GeneratorsMessages(
      version,
      generatorsData,
      generatorsDataTime,
      generatorsProtocol,
      generatorsTransaction,
      generatorsLocalVerdict,
      generatorsVerdict,
    )
    val generatorsProtocolSeq = new GeneratorsProtocolSequencing(
      version,
      generatorsDataTime,
      generatorsMessages,
    )
    val generatorsTransferData = new GeneratorsTransferData(
      version,
      generatorsDataTime,
      generatorsProtocol,
      generatorsProtocolSeq,
    )

    import generatorsData.*
    import generatorsTransferData.*
    import generatorsMessages.*
    import generatorsVerdict.*
    import generatorsLocalVerdict.*
    import generatorsProtocolSeq.*
    import generatorsTransaction.*
    import generatorsProtocol.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        if (version >= ProtocolVersion.v5) {
          testProtocolVersioned(StaticDomainParameters)
        }
        testProtocolVersioned(com.digitalasset.canton.protocol.DynamicDomainParameters)

        testProtocolVersioned(AcsCommitment)
        testProtocolVersioned(Verdict)
        testProtocolVersioned(MediatorResponse)
        if (version >= ProtocolVersion.v5) {
          testProtocolVersionedWithCtx(SignedProtocolMessage, (TestHash, version))
        }

        testProtocolVersioned(LocalVerdict)
        testProtocolVersioned(TransferResult)
        testProtocolVersioned(MalformedMediatorRequestResult)
        if (version >= ProtocolVersion.v4) {
          testProtocolVersionedWithCtx(EnvelopeContent, (TestHash, version))
        }
        if (version >= ProtocolVersion.v5) {
          /*
          With pv < 5, the TransactionResultMessage expects a NotificationTree that we cannot generate yet.
          Generating merkle trees will be done in the future.
           */
          testMemoizedProtocolVersionedWithCtx(TransactionResultMessage, (TestHash, version))
        }

        testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.AcknowledgeRequest)
        testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.ClosedEnvelope)

        testVersioned(ContractMetadata, version)(
          generatorsProtocol.contractMetadataArb(canHaveEmptyKey = true)
        )

        testVersioned[SerializableContract](
          SerializableContract,
          version,
          List(DefaultValueUntilExclusive(_.copy(contractSalt = None), ProtocolVersion.v4)),
        )(generatorsProtocol.serializableContractArb(canHaveEmptyKey = true))

        testProtocolVersioned(com.digitalasset.canton.data.ActionDescription)

        // Merkle tree leaves
        testMemoizedProtocolVersionedWithCtx(CommonMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(ParticipantMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(SubmitterMetadata, TestHash)
        if (version >= ProtocolVersion.v5) {
          testMemoizedProtocolVersionedWithCtx(TransferInCommonData, TestHash)

          testMemoizedProtocolVersionedWithCtx(TransferInView, TestHash)

          testMemoizedProtocolVersionedWithCtx(TransferOutCommonData, TestHash)
          testMemoizedProtocolVersionedWithCtx(TransferOutView, TestHash)
        }

        if (version >= ProtocolVersion.v5) {
          Seq(ConfirmationPolicy.Vip, ConfirmationPolicy.Signatory).map { confirmationPolicy =>
            testMemoizedProtocolVersionedWithCtx(
              com.digitalasset.canton.data.ViewCommonData,
              (TestHash, confirmationPolicy),
            )
          }
        }

        testMemoizedProtocolVersionedWithCtx(
          SignedTopologyTransaction,
          ProtocolVersionValidation(version),
        )

        testMemoizedProtocolVersioned(LegalIdentityClaim)

        testMemoizedProtocolVersionedWithCtx(
          com.digitalasset.canton.data.ViewParticipantData,
          TestHash,
        )
        testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.Batch)
        testMemoizedProtocolVersionedWithCtx(
          com.digitalasset.canton.sequencing.protocol.SubmissionRequest,
          MaxRequestSizeToDeserialize.NoLimit,
        )
        testVersioned(
          com.digitalasset.canton.sequencing.SequencerConnections,
          version,
        )

        if (version >= ProtocolVersion.v5) {
          testProtocolVersionedWithCtx(
            TransactionView,
            (TestHash, ConfirmationPolicy.Signatory, version),
          )
        }

        if (version >= ProtocolVersion.v5) {
          testProtocolVersionedWithCtxAndValidation(LightTransactionViewTree, TestHash, version)
        }
      }

    }
  }

  "be exhaustive" in {
    val requiredTests =
      findHasProtocolVersionedWrapperSubClasses("com.digitalasset.canton.protocol")

    val notSerializedTests = Seq(
      // Used for ACS commitments but not serialized
      "com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent"
    )

    val missingTests = requiredTests.diff(testedClasses.toList).diff(notSerializedTests)

    /*
        If this test fails, it means that one class inheriting from HasProtocolVersionWrapper in the
        package is not tested in the SerializationDeserializationTests
     */
    clue(s"Missing tests should be empty but found: $missingTests")(missingTests shouldBe empty)
  }
}
