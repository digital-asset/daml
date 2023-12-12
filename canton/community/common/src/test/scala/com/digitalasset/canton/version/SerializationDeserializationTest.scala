// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.SerializationDeserializationTestHelpers.DefaultValueUntilExclusive
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.protocol.{
  GeneratorsProtocol as GeneratorsProtocolSequencing,
  MaxRequestSizeToDeserialize,
}
import com.digitalasset.canton.topology.transaction.{
  GeneratorsTransaction,
  LegalIdentityClaim,
  SignedTopologyTransaction,
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

  forAll(Table("protocol version", ProtocolVersion.supported *)) { version =>
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
        testProtocolVersioned(StaticDomainParameters)
        testProtocolVersioned(com.digitalasset.canton.protocol.DynamicDomainParameters)

        testProtocolVersioned(AcsCommitment)
        testProtocolVersioned(Verdict)
        testProtocolVersioned(MediatorResponse)
        if (version >= ProtocolVersion.CNTestNet) {
          testMemoizedProtocolVersionedWithCtx(
            TypedSignedProtocolMessageContent,
            (TestHash),
          )
        }
        if (version >= ProtocolVersion.v5) {
          testProtocolVersionedWithCtx(SignedProtocolMessage, (TestHash))
        }

        testProtocolVersioned(LocalVerdict)
        testProtocolVersioned(TransferResult)
        testProtocolVersioned(MalformedMediatorRequestResult)
        if (version >= ProtocolVersion.v4 && version < ProtocolVersion.CNTestNet) {
          testProtocolVersionedWithCtx(EnvelopeContent, TestHash)
        }
        if (version >= ProtocolVersion.CNTestNet) {
          testMemoizedProtocolVersionedWithCtx(TransactionResultMessage, (TestHash))
        }

        testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.AcknowledgeRequest)

        if (version >= ProtocolVersion.CNTestNet) {
          testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.AggregationRule)
        }
        testProtocolVersioned(com.digitalasset.canton.sequencing.protocol.ClosedEnvelope)

        testVersioned(ContractMetadata)(
          generatorsProtocol.contractMetadataArb(canHaveEmptyKey = true)
        )
        testVersioned[SerializableContract](
          SerializableContract,
          List(DefaultValueUntilExclusive(_.copy(contractSalt = None), ProtocolVersion.v4)),
        )(generatorsProtocol.serializableContractArb(canHaveEmptyKey = true))

        testProtocolVersioned(com.digitalasset.canton.data.ActionDescription)

        // Merkle tree leaves
        testMemoizedProtocolVersionedWithCtx(CommonMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(ParticipantMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(SubmitterMetadata, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferInCommonData, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferInView, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferOutCommonData, TestHash)
        testMemoizedProtocolVersionedWithCtx(TransferOutView, TestHash)

        if (version >= ProtocolVersion.v5) {
          Seq(ConfirmationPolicy.Vip, ConfirmationPolicy.Signatory).map { confirmationPolicy =>
            testMemoizedProtocolVersionedWithCtx(
              com.digitalasset.canton.data.ViewCommonData,
              (TestHash, confirmationPolicy),
            )
          }
        }

        if (version < ProtocolVersion.CNTestNet) {
          testMemoizedProtocolVersioned(SignedTopologyTransaction)
        }
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
          List(
            SerializationDeserializationTestHelpers
              .DefaultValueUntilExclusive[SequencerConnections](
                transformer = (sc: SequencerConnections) =>
                  SequencerConnections.single(
                    sc.default
                  ),
                untilExclusive = ProtocolVersion.CNTestNet,
              )
          ),
        )
      }

    }
  }

  "be exhaustive" in {
    val requiredTests =
      findHasProtocolVersionedWrapperSubClasses("com.digitalasset.canton.protocol")

    val missingTests = requiredTests.diff(testedClasses.toList)

    /*
        If this test fails, it means that one class inheriting from HasProtocolVersionWrapper in the
        package is not tested in the SerializationDeserializationTests
     */
    clue(s"Missing tests should be empty but found: $missingTests")(missingTests shouldBe empty)
  }
}
