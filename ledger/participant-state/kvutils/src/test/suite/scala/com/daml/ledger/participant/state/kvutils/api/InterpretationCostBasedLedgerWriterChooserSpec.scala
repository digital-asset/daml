// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.api.health.{Healthy, Unhealthy}
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.v2.SubmissionResult.Acknowledged
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class InterpretationCostBasedLedgerWriterChooserSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext

  "commit" should {
    "delegate to cheap writer in case of no estimated interpretation cost" in {
      val commitMetadata = simpleCommitMetadata(estimatedInterpretationCost = None)
      val mockWriterCheap = mock[LedgerWriter]
      when(
        mockWriterCheap.commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
          any[TelemetryContext]
        )
      )
        .thenReturn(Future.successful(Acknowledged))
      val instance =
        new InterpretationCostBasedLedgerWriterChooser(1L, mockWriterCheap, mock[LedgerWriter])

      instance.commit(aCorrelationId, anEnvelope, commitMetadata).map { _ =>
        verify(mockWriterCheap, times(1))
          .commit(any[String], any[Raw.Envelope], any[CommitMetadata])(any[TelemetryContext])
        succeed
      }
    }

    "delegate to cheap writer in case estimated interpretation cost is below threshold" in {
      val commitMetadata = simpleCommitMetadata(estimatedInterpretationCost = Some(1))
      val mockWriterCheap = mock[LedgerWriter]
      when(
        mockWriterCheap.commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
          any[TelemetryContext]
        )
      )
        .thenReturn(Future.successful(Acknowledged))
      val instance =
        new InterpretationCostBasedLedgerWriterChooser(2L, mockWriterCheap, mock[LedgerWriter])

      instance.commit(aCorrelationId, anEnvelope, commitMetadata).map { _ =>
        verify(mockWriterCheap, times(1))
          .commit(any[String], any[Raw.Envelope], any[CommitMetadata])(any[TelemetryContext])
        succeed
      }
    }

    "delegate to expensive writer in case estimated interpretation cost reaches the threshold" in {
      val commitMetadata = simpleCommitMetadata(estimatedInterpretationCost = Some(1))
      val mockWriterExpensive = mock[LedgerWriter]
      when(
        mockWriterExpensive.commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
          any[TelemetryContext]
        )
      )
        .thenReturn(Future.successful(Acknowledged))
      val instance =
        new InterpretationCostBasedLedgerWriterChooser(1L, mock[LedgerWriter], mockWriterExpensive)

      instance.commit(aCorrelationId, anEnvelope, commitMetadata).map { _ =>
        verify(mockWriterExpensive, times(1))
          .commit(any[String], any[Raw.Envelope], any[CommitMetadata])(any[TelemetryContext])
        succeed
      }
    }

    "delegate to expensive writer in case threshold is 0" in {
      val commitMetadata = simpleCommitMetadata(estimatedInterpretationCost = None)
      val mockWriterExpensive = mock[LedgerWriter]
      when(
        mockWriterExpensive.commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
          any[TelemetryContext]
        )
      )
        .thenReturn(Future.successful(Acknowledged))
      val instance =
        new InterpretationCostBasedLedgerWriterChooser(0L, mock[LedgerWriter], mockWriterExpensive)

      instance.commit(aCorrelationId, anEnvelope, commitMetadata).map { _ =>
        verify(mockWriterExpensive, times(1))
          .commit(any[String], any[Raw.Envelope], any[CommitMetadata])(any[TelemetryContext])
        succeed
      }
    }
  }

  "currentHealth" should {
    "query both writer's health" in {
      val mockWriterCheap = mock[LedgerWriter]
      val mockWriterExpensive = mock[LedgerWriter]
      when(mockWriterCheap.currentHealth()).thenReturn(Unhealthy)
      when(mockWriterExpensive.currentHealth()).thenReturn(Healthy)
      val instance =
        new InterpretationCostBasedLedgerWriterChooser(1L, mockWriterCheap, mockWriterExpensive)

      instance.currentHealth() shouldBe (Unhealthy and Healthy)

      verify(mockWriterCheap, times(1)).currentHealth()
      verify(mockWriterExpensive, times(1)).currentHealth()
      succeed
    }
  }

  private def aCorrelationId: String = ""

  private def anEnvelope: Raw.Envelope = Raw.Envelope.empty

  private def simpleCommitMetadata(estimatedInterpretationCost: Option[Long]): CommitMetadata = {
    val mockCommitMetadata = mock[CommitMetadata]
    when(mockCommitMetadata.estimatedInterpretationCost).thenReturn(estimatedInterpretationCost)
    mockCommitMetadata
  }
}
