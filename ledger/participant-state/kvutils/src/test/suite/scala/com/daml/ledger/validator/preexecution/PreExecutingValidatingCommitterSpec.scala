// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlState.DamlStateKey
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.`export`.{
  LedgerDataExporter,
  SubmissionAggregator,
  SubmissionInfo,
}
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.validator.TestHelper.{FakeStateAccess, aParticipantId}
import com.daml.ledger.validator.reading.StateReader
import com.daml.ledger.validator.{LedgerStateOperations, LedgerStateWriteOperations}
import com.daml.lf.data.Ref.ParticipantId
import com.daml.logging.LoggingContext
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class PreExecutingValidatingCommitterSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  import PreExecutingValidatingCommitterSpec._
  import PreExecutionTestHelper._

  "commit" should {

    "write the transactions when no post execution conflicts are detected" in {
      val fixture = new ValidatingCommitterFixture
      fixture.validatorReturnsSuccess()
      when(
        fixture.conflictDetector.detectConflicts(
          any[PreExecutionOutput[TestReadSet, TestWriteSet]],
          any[StateReader[DamlStateKey, TestValue]],
        )(any[ExecutionContext], any[LoggingContext])
      )
        .thenReturn(Future.unit)
      val submissionAggregator = mock[SubmissionAggregator]
      when(fixture.ledgerDataExporter.addSubmission(any[SubmissionInfo]))
        .thenReturn(submissionAggregator)
      when(
        fixture.postExecutionWriter.write(
          any[TestWriteSet],
          any[LedgerStateWriteOperations[Long]],
        )(any[ExecutionContext], any[LoggingContext])
      ).thenReturn(Future.successful(SubmissionResult.Acknowledged))
      fixture.committer
        .commit(
          aParticipantId,
          "corid",
          Raw.Envelope.empty,
          Instant.now(),
          new FakeStateAccess(mock[LedgerStateOperations[Unit]]),
        )
        .map { result =>
          verify(fixture.postExecutionWriter)
            .write(any[TestWriteSet], any[LedgerStateWriteOperations[Long]])(
              any[ExecutionContext],
              any[LoggingContext],
            )
          verify(submissionAggregator).finish()
          result shouldBe SubmissionResult.Acknowledged
        }
    }

    "drop transaction when post execution conflicts are detected" in {
      val fixture = new ValidatingCommitterFixture
      fixture.validatorReturnsSuccess()
      when(
        fixture.conflictDetector.detectConflicts(
          any[PreExecutionOutput[TestReadSet, TestWriteSet]],
          any[StateReader[DamlStateKey, TestValue]],
        )(any[ExecutionContext], any[LoggingContext])
      ).thenReturn(Future.failed(new ConflictDetectedException()))
      val submissionAggregator = mock[SubmissionAggregator]
      when(fixture.ledgerDataExporter.addSubmission(any[SubmissionInfo]))
        .thenReturn(submissionAggregator)
      fixture.committer
        .commit(
          aParticipantId,
          "corid",
          Raw.Envelope.empty,
          Instant.now(),
          new FakeStateAccess(mock[LedgerStateOperations[Unit]]),
        )
        .map { result =>
          verify(fixture.postExecutionWriter, never).write(
            any[TestWriteSet],
            any[LedgerStateWriteOperations[Long]],
          )(any[ExecutionContext], any[LoggingContext])
          verify(submissionAggregator, never).finish()
          result shouldBe SubmissionResult.Acknowledged
        }
    }
  }

}

object PreExecutingValidatingCommitterSpec {

  import ArgumentMatchersSugar._
  import MockitoSugar._
  import PreExecutionTestHelper._

  class ValidatingCommitterFixture {

    val validator = mock[PreExecutingSubmissionValidator[TestValue, TestReadSet, TestWriteSet]]
    val conflictDetector =
      mock[PostExecutionConflictDetector[DamlStateKey, TestValue, TestReadSet, TestWriteSet]]
    val postExecutionWriter = mock[PostExecutionWriter[TestWriteSet]]
    val writeSetSelector = mock[WriteSetSelector[TestReadSet, TestWriteSet]]
    val ledgerDataExporter = mock[LedgerDataExporter]

    val committer =
      new PreExecutingValidatingCommitter(
        _ => createLedgerStateReader(Map.empty),
        validator,
        conflictDetector,
        writeSetSelector,
        postExecutionWriter,
        ledgerDataExporter,
      )

    def validatorReturnsSuccess() = {
      when(
        validator.validate(
          any[Raw.Envelope],
          any[ParticipantId],
          any[StateReader[DamlStateKey, TestValue]],
        )(any[ExecutionContext], any[LoggingContext])
      ).thenReturn(
        Future.successful(
          PreExecutionOutput(
            None,
            None,
            TestWriteSet(""),
            TestWriteSet(""),
            TestReadSet(Set.empty),
            Set.empty,
          )
        )
      )
    }
  }
}
