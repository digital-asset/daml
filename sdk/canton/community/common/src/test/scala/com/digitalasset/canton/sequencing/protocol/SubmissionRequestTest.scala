// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Signature, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.time.Duration
import java.util.UUID

class SubmissionRequestTest extends BaseTestWordSpec {

  private lazy val defaultAggregationRule = AggregationRule(
    NonEmpty(Seq, DefaultTestIdentities.participant1, DefaultTestIdentities.participant2),
    PositiveInt.tryCreate(1),
    testedProtocolVersion,
  )

  private lazy val defaultTopologyTimestamp = Some(
    CantonTimestamp.Epoch.add(Duration.ofSeconds(1))
  )

  private lazy val defaultSubmissionRequest =
    SubmissionRequest.tryCreate(
      DefaultTestIdentities.participant1,
      MessageId.fromUuid(new UUID(1L, 1L)),
      isRequest = false,
      Batch.empty(testedProtocolVersion),
      maxSequencingTime = CantonTimestamp.MaxValue,
      topologyTimestamp = defaultTopologyTimestamp,
      Some(defaultAggregationRule),
      testedProtocolVersion,
    )

  "aggregation id" should {
    "authenticate the relevant fields" in {
      if (testedProtocolVersion >= ProtocolVersion.v31) {

        val envelope1 = ClosedEnvelope.create(
          ByteString.copyFromUtf8("Content1"),
          Recipients.cc(DefaultTestIdentities.participant1),
          Seq.empty,
          testedProtocolVersion,
        )
        val envelope2 = ClosedEnvelope.create(
          ByteString.copyFromUtf8("Content2"),
          Recipients.cc(DefaultTestIdentities.participant1),
          Seq.empty,
          testedProtocolVersion,
        )

        val differentRequests = Seq(
          defaultSubmissionRequest,
          defaultSubmissionRequest.copy(batch = Batch.fromClosed(testedProtocolVersion, envelope1)),
          defaultSubmissionRequest.copy(batch = Batch.fromClosed(testedProtocolVersion, envelope2)),
          defaultSubmissionRequest.copy(batch =
            Batch.fromClosed(
              testedProtocolVersion,
              envelope1.copy(signatures = Seq(Signature.noSignature)),
            )
          ),
          defaultSubmissionRequest.copy(batch =
            Batch.fromClosed(testedProtocolVersion, envelope1, envelope2)
          ),
          defaultSubmissionRequest.copy(batch =
            Batch.fromClosed(
              testedProtocolVersion,
              envelope1.copy(recipients = Recipients.cc(DefaultTestIdentities.participant2)),
            )
          ),
          defaultSubmissionRequest.copy(maxSequencingTime = CantonTimestamp.Epoch),
          defaultSubmissionRequest.copy(topologyTimestamp = Some(CantonTimestamp.MinValue)),
          defaultSubmissionRequest.copy(topologyTimestamp = Some(CantonTimestamp.MaxValue)),
          defaultSubmissionRequest.copy(topologyTimestamp = Some(CantonTimestamp.Epoch)),
          defaultSubmissionRequest.copy(aggregationRule =
            Some(
              defaultAggregationRule.copy(eligibleMembers =
                NonEmpty(
                  Seq,
                  DefaultTestIdentities.participant1,
                  DefaultTestIdentities.participant3,
                )
              )
            )
          ),
          defaultSubmissionRequest.copy(aggregationRule =
            Some(defaultAggregationRule.copy(threshold = PositiveInt.tryCreate(2)))
          ),
        )

        val aggregationIds = differentRequests.map(_.aggregationId(TestHash))
        aggregationIds.distinct.size shouldBe differentRequests.size
      }
    }

    "ignore sender-specific fields" in {
      if (testedProtocolVersion >= ProtocolVersion.v31) {
        val envelope1 = ClosedEnvelope.create(
          ByteString.copyFromUtf8("some-content"),
          Recipients.cc(DefaultTestIdentities.participant1, DefaultTestIdentities.participant3),
          Seq.empty,
          testedProtocolVersion,
        )

        val submissionRequestWithEnvelope1 =
          defaultSubmissionRequest.copy(batch = Batch.fromClosed(testedProtocolVersion, envelope1))

        def assertEquivalentRequests(requests: Seq[SubmissionRequest]): Unit = {
          // Sanity check that the requests themselves are actually different
          requests.distinct.size shouldBe requests.size
          requests.size shouldBe >(1)

          val aggregationIds = requests.map(_.aggregationId(TestHash))
          aggregationIds.distinct.size shouldBe 1
        }

        val requestsWithoutSignatures = Seq(
          submissionRequestWithEnvelope1,
          submissionRequestWithEnvelope1.copy(sender = DefaultTestIdentities.participant3),
          submissionRequestWithEnvelope1.copy(isRequest = true),
          submissionRequestWithEnvelope1.copy(messageId = MessageId.fromUuid(new UUID(10, 10))),
        )

        assertEquivalentRequests(requestsWithoutSignatures)

        val envelope2 = ClosedEnvelope.create(
          ByteString.copyFromUtf8("some-content"),
          Recipients.cc(DefaultTestIdentities.participant1, DefaultTestIdentities.participant2),
          Seq(Signature.noSignature),
          testedProtocolVersion,
        )

        val submissionRequestWithEnvelope2 =
          defaultSubmissionRequest.copy(batch = Batch.fromClosed(testedProtocolVersion, envelope2))

        val someSignature = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("A signature"),
          DefaultTestIdentities.participant1.fingerprint,
        )

        val requestsWithSignatures = Seq(
          submissionRequestWithEnvelope2,
          submissionRequestWithEnvelope2.copy(batch =
            Batch.fromClosed(
              testedProtocolVersion,
              envelope2.copy(signatures = Seq(someSignature)),
            )
          ),
          submissionRequestWithEnvelope2.copy(batch =
            Batch.fromClosed(
              testedProtocolVersion,
              envelope2.copy(signatures = Seq(someSignature, Signature.noSignature)),
            )
          ),
          submissionRequestWithEnvelope2.copy(isRequest = true),
          submissionRequestWithEnvelope2.copy(messageId = MessageId.fromUuid(new UUID(10, 10))),
        )

        assertEquivalentRequests(requestsWithSignatures)
      }
    }
  }
}
