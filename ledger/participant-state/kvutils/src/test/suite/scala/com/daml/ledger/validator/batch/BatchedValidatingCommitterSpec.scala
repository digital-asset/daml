// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import java.time.Instant

import akka.stream.Materializer
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.TestHelper.aParticipantId
import com.daml.ledger.validator.reading.DamlLedgerStateReader
import com.daml.ledger.validator.{CommitStrategy, LedgerStateOperations}
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.MockitoSugar
import org.mockito.stubbing.ScalaFirstStubbing
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class BatchedValidatingCommitterSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MockitoSugar {
  "commit" should {
    "return Acknowledged in case of success" in {
      val mockValidator = mock[BatchedSubmissionValidator[Unit]]
      whenValidateAndCommit(mockValidator)
        .thenReturn(Future.unit)
      val instance =
        BatchedValidatingCommitter[Unit](() => Instant.now(), mockValidator)

      instance
        .commit(
          correlationId = "",
          submissionEnvelope = ByteString.EMPTY,
          submittingParticipantId = aParticipantId,
          ledgerStateOperations = mock[LedgerStateOperations[Unit]])
        .map { actual =>
          actual shouldBe SubmissionResult.Acknowledged
        }
    }

    "return InternalError in case of an exception" in {
      val mockValidator = mock[BatchedSubmissionValidator[Unit]]
      whenValidateAndCommit(mockValidator)
        .thenReturn(Future.failed(new IllegalArgumentException("Validation failure")))
      val instance = BatchedValidatingCommitter[Unit](() => Instant.now(), mockValidator)

      instance
        .commit(
          correlationId = "",
          submissionEnvelope = ByteString.EMPTY,
          submittingParticipantId = aParticipantId,
          ledgerStateOperations = mock[LedgerStateOperations[Unit]])
        .map { actual =>
          actual shouldBe SubmissionResult.InternalError("Validation failure")
        }
    }
  }

  private def whenValidateAndCommit(
      mockValidator: BatchedSubmissionValidator[Unit]): ScalaFirstStubbing[Future[Unit]] =
    when(
      mockValidator.validateAndCommit(
        any[ByteString](),
        anyString(),
        any[Instant](),
        any[ParticipantId](),
        any[DamlLedgerStateReader](),
        any[CommitStrategy[Unit]]())(any[Materializer](), any[ExecutionContext]()))
}
