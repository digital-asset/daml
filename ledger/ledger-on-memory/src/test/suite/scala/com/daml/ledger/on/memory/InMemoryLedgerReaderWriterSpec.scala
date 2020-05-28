// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.{BatchedValidatingCommitter, LedgerStateOperations}
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class InMemoryLedgerReaderWriterSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MockitoSugar {
  "commit" should {
    "not signal new head in case of failure" in {
      val mockDispatcher = mock[Dispatcher[Index]]
      val mockCommitter = mock[BatchedValidatingCommitter[Index]]
      when(
        mockCommitter.commit(
          anyString(),
          any[ByteString](),
          any[ParticipantId](),
          any[LedgerStateOperations[Index]])(any[ExecutionContext]()))
        .thenReturn(
          Future.successful(SubmissionResult.InternalError("Validation failed with an exception")))
      val instance = new InMemoryLedgerReaderWriter(
        Ref.ParticipantId.assertFromString("participant ID"),
        "ledger ID",
        mockDispatcher,
        InMemoryState.empty,
        mockCommitter,
        new Metrics(new MetricRegistry)
      )

      instance.commit("correlation ID", ByteString.copyFromUtf8("some bytes")).map { actual =>
        verify(mockDispatcher, times(0)).signalNewHead(anyInt())
        actual should be(a[SubmissionResult.InternalError])
      }
    }
  }
}
