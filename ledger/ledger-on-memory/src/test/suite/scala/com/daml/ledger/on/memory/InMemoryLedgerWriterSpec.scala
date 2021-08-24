// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.api.CommitMetadata
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.validator.LedgerStateAccess
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class InMemoryLedgerWriterSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext

  "commit" should {
    "not signal new head in case of failure" in {
      val mockDispatcher = mock[Dispatcher[Index]]
      val mockCommitter = mock[InMemoryLedgerWriter.Committer]
      when(
        mockCommitter.commit(
          any[Ref.ParticipantId],
          any[String],
          any[Raw.Envelope],
          any[Instant],
          any[LedgerStateAccess[Any]],
        )(any[ExecutionContext])
      )
        .thenReturn(
          Future.successful(SubmissionResult.SynchronousError(Status()))
        )
      val instance = new InMemoryLedgerWriter(
        participantId = Ref.ParticipantId.assertFromString("participant ID"),
        dispatcher = mockDispatcher,
        now = () => Instant.EPOCH,
        state = InMemoryState.empty,
        committer = mockCommitter,
        committerExecutionContext = executionContext,
        metrics = new Metrics(new MetricRegistry),
      )

      instance
        .commit(
          "correlation ID",
          Raw.Envelope(ByteString.copyFromUtf8("some bytes")),
          CommitMetadata.Empty,
        )
        .map { actual =>
          verify(mockDispatcher, times(0)).signalNewHead(any[Int])
          actual should be(a[SubmissionResult.SynchronousError])
        }
    }
  }
}
