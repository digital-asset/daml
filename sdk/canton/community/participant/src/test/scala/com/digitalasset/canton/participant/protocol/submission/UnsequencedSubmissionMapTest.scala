// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.UnsequencedSubmissionMap
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.topology.SynchronizerId
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class UnsequencedSubmissionMapTest extends AnyWordSpec with BaseTest {

  private val rootHash1: RootHash = RootHash(
    Hash.digest(HashPurpose.SequencedEventSignature, ByteString.EMPTY, HashAlgorithm.Sha256)
  )
  private val rootHash2: RootHash = RootHash(
    Hash.digest(
      HashPurpose.SequencedEventSignature,
      ByteString.copyFrom(Array[Byte](1, 2, 3)),
      HashAlgorithm.Sha256,
    )
  )
  assert(rootHash1 != rootHash2)
  private val messageUuid1: UUID = UUID.randomUUID()
  private val messageId1: MessageId = MessageId.fromUuid(messageUuid1)
  private val synchronizerId: SynchronizerId = SynchronizerId.tryFromString("x::domain")

  "pushIfNotExists" should {
    "add entry if not present yet" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = None,
      )
      testee.pull(messageId1).map(_.trackingData) shouldBe Some("submissionTrackingData")
    }

    "not add entry if present" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = None,
      )
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData2",
        submissionTraceContext = implicitly,
        rootHash = None,
      )
      testee.pull(messageId1).map(_.trackingData) shouldBe Some("submissionTrackingData")
    }

    "add entry to root-hash map as well if present" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = Some(rootHash1),
      )
      testee.pullByHash(rootHash1)
      testee.pull(messageId1).map(_.trackingData) shouldBe None
    }
  }

  "addRootHashIfNotSpecifiedYet" should {
    "add entry to root-hash map as well if present" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = None,
      )
      testee.addRootHashIfNotSpecifiedYet(messageId1, rootHash1)
      testee.pullByHash(rootHash1)
      testee.pull(messageId1).map(_.trackingData) shouldBe None
    }

    "not change entry if root hash already present" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = None,
      )
      testee.addRootHashIfNotSpecifiedYet(messageId1, rootHash1)
      testee.addRootHashIfNotSpecifiedYet(messageId1, rootHash2)
      testee.pullByHash(rootHash1)
      testee.pull(messageId1).map(_.trackingData) shouldBe None
    }
  }

  "changeIfExists" should {
    "change entry if present" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = None,
      )
      testee.changeIfExists(messageId1, "submissionTrackingData2")
      testee.pull(messageId1).map(_.trackingData) shouldBe Some("submissionTrackingData2")
    }
  }

  "pull" should {
    "remove entry if present" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = None,
      )
      testee.pull(messageId1).map(_.trackingData) shouldBe Some("submissionTrackingData")
      testee.pull(messageId1).map(_.trackingData) shouldBe None
    }
  }

  "pullByHash" should {
    "remove entry if present" in {
      val testee = unsequencedSubmissionMap()
      testee.pushIfNotExists(
        messageUuid = messageUuid1,
        trackingData = "submissionTrackingData",
        submissionTraceContext = implicitly,
        rootHash = Some(rootHash1),
      )
      testee.pullByHash(rootHash1)
      testee.pull(messageId1).map(_.trackingData) shouldBe None
    }
  }

  private def unsequencedSubmissionMap() = new UnsequencedSubmissionMap[String](
    synchronizerId = synchronizerId,
    sizeWarnThreshold = 1000,
    unsequencedInFlightGauge =
      ParticipantTestMetrics.domain.inFlightSubmissionDomainTracker.unsequencedInFlight,
    loggerFactory = loggerFactory,
  )
}
