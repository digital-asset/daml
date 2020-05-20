// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter.Index
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.{CommitStrategy, DamlLedgerStateReader}
import com.daml.ledger.validator.batch.BatchedSubmissionValidator
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{times, verify, when}

import scala.concurrent.{ExecutionContext, Future}

class InMemoryBatchedLedgerReaderWriterSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MockitoSugar {
  "commit" should {
    "not signal new head in case of failure" in {
      val mockDispatcher = mock[Dispatcher[Index]]
      val mockValidator = mock[BatchedSubmissionValidator[Index]]
      when(
        mockValidator.validateAndCommit(
          any[ByteString](),
          anyString(),
          any[Instant](),
          any[ParticipantId](),
          any[DamlLedgerStateReader](),
          any[CommitStrategy[Index]]())(any[Materializer](), any[ExecutionContext]()))
        .thenReturn(
          Future.failed(new IllegalArgumentException("Validation failed with an exception")))
      val instance = new InMemoryBatchedLedgerReaderWriter(
        Ref.ParticipantId.assertFromString("participant ID"),
        "ledger ID",
        () => Instant.now(),
        mockDispatcher,
        InMemoryState.empty,
        mockValidator,
        new Metrics(new MetricRegistry)
      )

      instance.commit("correlation ID", ByteString.copyFromUtf8("some bytes")).map { actual =>
        verify(mockDispatcher, times(0)).signalNewHead(anyInt())
        actual should be(a[SubmissionResult.InternalError])
      }
    }
  }
}
