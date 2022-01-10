// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimeProvider
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.lf.data.{Ref, Time}
import com.daml.metrics.Metrics
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.sandbox.bridge.LedgerBridge.toOffset
import com.daml.ledger.sandbox.domain.Submission
import com.daml.logging.LoggingContext
import com.daml.platform.server.api.validation.ErrorFactories
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import scala.util.chaining.scalaUtilChainingOps

class SequenceTest
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  behavior of classOf[SequenceImpl].getSimpleName

  // TODO SoX: Assert duplicate party
  it should "store party on internal empty state on party allocation" in withFixture { context =>
    import context._
    val partyHint = "some-party"
    val displayName = "Some display name"
    val submissionId = Ref.SubmissionId.assertFromString("some-submission-id")
    val allocatedParty = Ref.Party.assertFromString(partyHint)
    val partyAllocationSubmission = NoOpPreparedSubmission(
      Submission.AllocateParty(
        hint = Some(allocatedParty),
        displayName = Some(displayName),
        submissionId = submissionId,
      )
    )

    val result = sequenceImpl()(Right(Offset.beforeBegin -> partyAllocationSubmission))

    result should have size 1
    val (assignedOffset, update) = result.head

    assignedOffset shouldBe toOffset(1L)
    update shouldBe Update.PartyAddedToParticipant(
      party = allocatedParty,
      displayName = displayName,
      participantId = Ref.ParticipantId.assertFromString(participantName),
      recordTime = currentRecordTime,
      submissionId = Some(submissionId),
    )

    sequenceImpl.allocatedPartiesRef.get() shouldBe Set(allocatedParty)
  }

  it should "perform configuration upload validation" in withFixture { context =>
    import context._

    val maxRecordTime = Time.Timestamp.assertFromLong(1337L)
    val submissionId = Ref.SubmissionId.assertFromString("some-submission-id")

    val config1 = Configuration(
      generation = 1L,
      timeModel = LedgerTimeModel.reasonableDefault,
      maxDeduplicationTime = Duration.ofSeconds(0L),
    )
    val configUpload = Submission.Config(
      maxRecordTime = maxRecordTime,
      submissionId = submissionId,
      config = config1,
    )
    val successfulConfigUploadSubmission = NoOpPreparedSubmission(configUpload)

    val currentTime = Time.Timestamp.assertFromLong(1000L)
    when(timeProviderMock.getCurrentTimestamp).thenReturn(currentTime)

    // Assert successful upload
    sequenceImpl()(Right(Offset.beforeBegin -> successfulConfigUploadSubmission))
      .pipe { result =>
        result should have size 1
        val (assignedOffset, update) = result.head

        assignedOffset shouldBe toOffset(1L)
        update shouldBe Update.ConfigurationChanged(
          recordTime = currentTime,
          submissionId = submissionId,
          participantId = Ref.ParticipantId.assertFromString(participantName),
          newConfiguration = config1,
        )

        // Assert config state is set
        sequenceImpl.ledgerConfigurationRef.get() shouldBe Some(config1)
      }

    // Assert rejection on config generation mismatch
    val config2 = config1.copy(maxDeduplicationTime = Duration.ofSeconds(10L))
    sequenceImpl()(
      Right(Offset.beforeBegin -> NoOpPreparedSubmission(configUpload.copy(config = config2)))
    ).pipe { result =>
      result should have size 1
      val (assignedOffset, update) = result.head

      assignedOffset shouldBe toOffset(2L)
      update shouldBe Update.ConfigurationChangeRejected(
        recordTime = currentTime,
        submissionId = submissionId,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        proposedConfiguration = config2,
        rejectionReason = s"Generation mismatch: expected=Some(2), actual=1",
      )

      // Assert config state is unchanged
      sequenceImpl.ledgerConfigurationRef.get() shouldBe Some(config1)
    }

    // Assert rejection on max record time violation
    val currentTime2 = Time.Timestamp.assertFromLong(2000L)
    when(timeProviderMock.getCurrentTimestamp).thenReturn(currentTime2)
    sequenceImpl()(
      Right(Offset.beforeBegin -> NoOpPreparedSubmission(configUpload.copy(config = config2)))
    ).pipe { result =>
      result should have size 1
      val (assignedOffset, update) = result.head

      assignedOffset shouldBe toOffset(3L)
      update shouldBe Update.ConfigurationChangeRejected(
        recordTime = currentTime2,
        submissionId = submissionId,
        participantId = Ref.ParticipantId.assertFromString(participantName),
        proposedConfiguration = config2,
        rejectionReason = s"Configuration change timed out: $maxRecordTime > $currentTime2",
      )

      // Assert config state is unchanged
      sequenceImpl.ledgerConfigurationRef.get() shouldBe Some(config1)
    }
  }

  private def withFixture(f: TestContext => Assertion): Assertion =
    f(new TestContext)

  private class TestContext {
    val timeProviderMock: TimeProvider = mock[TimeProvider]
    val participantName: String = "participant"
    val sequenceImpl: SequenceImpl = SequenceImpl(
      participantId = Ref.ParticipantId.assertFromString(participantName),
      bridgeMetrics = new BridgeMetrics(new Metrics(new MetricRegistry)),
      timeProvider = timeProviderMock,
      errorFactories = ErrorFactories(useSelfServiceErrorCodes = false),
      validatePartyAllocation = true,
      initialLedgerEnd = Offset.beforeBegin,
      initialAllocatedParties = Set.empty,
      initialLedgerConfiguration = Option.empty,
    )

    val currentRecordTime: Time.Timestamp = Time.Timestamp.assertFromLong(1337L)
    when(timeProviderMock.getCurrentTimestamp).thenReturn(currentRecordTime)
  }
}
